# Relation Query Extension Plan

Plan for extending `IRelation.Query<T>()` (`IQueryable<T>`) and making the bytecode interpreter behind it as fast as
possible. Hard constraint: no `Reflection.Emit`, no `Expression.Compile()` code generation; everything must work under
NativeAOT.

## Where it stands

- [RelationQuery.cs](../BTDB/ODBLayer/RelationQuery.cs) supports only `Queryable.Where` with `==`, `&&`, `||`,
  constants and captured variables. `CreateQuery` rejects anything else, `Execute` always throws, and the element type
  cannot change.
- Supported field types: `string`, `bool`, the integer types, `Half`, `float`, `double`, `DateTime`.
- Access path: only a cursor prefix built from leading primary-key equalities (`BuildCursorPrefix`). Secondary indexes,
  ranges, ordering, paging, counting and projection are not used or supported.
- Execution: one program per query. For every row it:
  - allocates scratch space with `StackBytesAlloc` / `StackAllocObject`, each a nested `Run` wrapped in try/finally;
  - skips the relation prefix in bytecode;
  - picks the value version through a linear chain of compares;
  - allocates a `string` for every referenced string field (`ReadString`).
- The interpreter loop in [Interpreter.cs](../BTDB/Interpreter/Interpreter.cs):
  - has a `try` block around every instruction;
  - keeps `pc` behind a `ref`;
  - decodes VUInt operands for every instruction;
  - needs 3–5 dispatches for a single `field == const` test (load slot, add offset, load const, compare, branch).
- The `OpCode` byte space has 151 of 256 values used, so new opcodes need a second-level page, the same way `CodeOps`
  works.

## Design decisions

1. **Keep the bytecode interpreter** and make its hot path cheap: fused superinstructions, a dedicated frame/slot
   model, fixed-width operands, and no exception handling in the inner loop. A closure tree of generic sealed nodes is
   the main alternative. Phase 0 benchmarks it once, so we have numbers and are not guessing.
2. **Split the work in three: planner, program, cursor driver.** The planner turns the expression into an access path,
   a key-residual program, a value-residual program (one per persisted value version), and terminal semantics. Seeking
   and paging stay in C#; the interpreter only evaluates per-row work.
3. **Parameterize and cache.** Captured values become parameter slots. A compiled plan is cached in `RelationInfo` under
   a structural shape key: node types, members, constant types, and whether each constant is null. A cache hit skips
   parsing and building and only fills the parameter block and computes the seek bytes.
4. **Use reflection only at plan time, never emit.**
   - Captured values are read with `FieldInfo` / `PropertyInfo` (already done this way).
   - Rare constant subexpressions are evaluated with `MethodInfo.Invoke`, falling back to
     `LambdaExpression.Compile(preferInterpretation: true)`.
   - Rows are materialized with the existing loaders and source-generated `ClassMetadata`
     (`Creator`, `PropRefSetter`).

## Phases

### Phase 0: Baseline and safety nets

- BenchmarkDotNet suite in `DBBenchmark/RelationQueryBenchmark.cs` covering:
  - a full scan with a value predicate;
  - a PK-prefix predicate;
  - a string equality predicate;
  - a selective predicate versus a non-selective one;
  - a comparison against hand-written `ScanBy*` / `ListBy*` code and against `AsEnumerable().Where(...)`.
  - It reports allocations per row.
- A differential test harness in `BTDBTest`. It generates random rows and random predicate trees, then checks that
  `Query` returns the same results as LINQ-to-Objects over the materialized relation. Every later phase adds node
  kinds to the generator.
- A NativeAOT smoke app (`AotQuerySmoke/`, `PublishAot=true`) that runs representative queries and fails on any
  trimming or AOT warning coming from the query code path. Run it in CI.

#### Phase 0 status: done

How to run each piece:

- Benchmarks: `dotnet run -c Release --project DBBenchmark -- relation-query --filter '*RelationQuery*'`.
  - Add `--inProcess` when another `DBBenchmark.csproj` exists under the repository, such as in a leftover
    `.claude/worktrees` checkout; BenchmarkDotNet refuses to build out of process in that case.
  - `RelationQueryBenchmark` measures end-to-end scans. Every variant reads `Current`, so every variant pays for row
    materialization.
  - `RelationQueryEngineBenchmark` evaluates the same predicate over value bytes copied out up front. It compares the
    current interpreter, a benchmark-only closure-tree prototype (`DBBenchmark/QueryClosureTreePrototype.cs`),
    hand-written decoding, and hand-written matching of encoded bytes.
- Differential test: `BTDBTest/ObjectDbRelationQueryDifferentialTest.cs`, 10 seeds × 150 predicates over 400 rows.
  - It also asserts that at least a quarter of the predicates select a proper subset of the rows.
  - Checked by mutation: compiling `||` as `&&` fails the first predicate of several seeds.
- AOT smoke: `./AotQuerySmoke/run.sh`; it also runs as the `aot-query-smoke` CI job.
  - It currently reports 0 warnings in the query path and 217 trim/AOT warnings elsewhere in BTDB.
  - The native binary passes every check.

Baseline results (Apple M2 Max, .NET 10.0.13, in-process toolchain, per scanned row):

| Scenario                          | Query   | Comparison                                | Comparison time |
|-----------------------------------|--------:|-------------------------------------------|----------------:|
| Value, selective (1.25 %)         | 306 ns  | LINQ over all rows, materializing each    | 135 ns          |
| Value, non-selective (50 %)       | 393 ns  | LINQ over all rows                        | 136 ns          |
| String equality (5 %)             | 288 ns  | LINQ over all rows                        | 129 ns          |
| PK prefix (whole tenant matches)  | 372 ns  | `FindById` / `ScanById`                   | 129 / 139 ns    |
| PK prefix + value                 | 489 ns  | `FindById` + LINQ filter                  | 123 ns          |

The current Query is 2–4× slower than materializing every row and filtering in LINQ, even when it rejects almost all
rows without materializing them.

Engine only (predicate evaluation per row, no cursor):

| Case                         | Interpreter | Closure tree | Hand-written | Encoded match |
|------------------------------|------------:|-------------:|-------------:|--------------:|
| `Age == 35 && Active`        | 256 ns      | 45 ns        | 38 ns        | 32 ns         |
| `Age == 35 && City == "..."` | 230 ns, 37 B | 49 ns, 37 B | 39 ns, 37 B  | 25 ns, 0 B    |

What this means for Phase 1:

- About 200 ns of per-row overhead is interpreter cost, mostly per-row setup:
  - nested `StackBytesAlloc` / `StackAllocObject` runs with try/finally;
  - the version compare chain;
  - multi-instruction slot addressing;
  - the `int` ← `long` converter call.
  The value decoding itself is cheap.
- A closure tree already reaches 1.2× hand-written. Phase 1 has to bring the interpreter into the same range, or we
  should reconsider design decision 1 using these numbers.
- Matching encoded bytes beats decoding and removes the per-row string allocation, which confirms the Phase 2
  `MatchEncodedConstBr` plan.
- `Where(p => p.TenantId == x)` re-checks the PK-prefix predicate in the interpreter for every row, although the cursor
  prefix already guarantees it. Conjuncts fully covered by the seek prefix should be dropped; add this to Phase 1.

### Phase 1: Interpreter hot-path rework (no new query features)

- **Loop split.** Move to a `RunCore` with no exception handling, a local `pc`, and rare opcodes (exceptions,
  stack-alloc, finally) moved out of line with `NoInlining`. An outer wrapper catches exceptions, runs
  `HandleException`, and re-enters `RunCore`.
- **Frame model.** Add dedicated `InterpreterCtx` fields:
  - `ref byte Frame`: row scratch bytes;
  - `ref object? Objects`: object slots;
  - `ref MemReader KeyReader` and `ref MemReader ValueReader`;
  - `ref byte Params`: per-execution parameters.
  The query enumerator owns a reusable `byte[]` / `object?[]` frame. This removes the per-row `StackBytesAlloc` /
  `StackAllocObject` nesting and the try/finally bookkeeping it brings.
- **Second-level `QueryOps` opcode page** with fixed-width little-endian `u16` operands, read with
  `Unsafe.ReadUnaligned`:
  - `ReadToSlot reader, codeOp, slot`: one instruction per needed field;
  - `SkipFields reader, codeOp, count`;
  - `CmpSlotConstBr kind, slot, const, label` and `CmpSlotSlotBr`, where `kind` packs type × comparison × negation
    into one dense byte;
  - `MatchEncodedConstBr reader, const, len, label`: compares serialized bytes in place and skips on a match.
- **Per-version value programs.** Dispatch through `programs[version]` in C#. The key reader starts after the relation
  prefix, so there is no `SkipBlock` per row.
- **Short-circuit read-as-you-go.** For conjunctions, interleave field reads with tests in serialization order. The
  program stops reading as soon as a conjunct fails.
- **Cursor driver.** Key-only residuals run inside `FastIterateNoCursor`. Value residuals run inside `FastIterate` and
  read the value in the callback. Use pinned spans instead of `MemReader.CreateFromReadOnlyMemory` per row.
- **Target:** at least 2× faster on the Phase 0 predicate benchmarks and zero allocations per rejected row.

### Phase 2: Predicate breadth

- Comparisons `!=`, `<`, `<=`, `>`, `>=` and `!` (normalized with De Morgan's laws). A bare `bool` member counts as a
  predicate. Fields can be compared with other fields, and with arithmetic expressions over fields and constants using
  the existing Add/Sub/Mul/Div opcodes.
- New types:
  - enums (through their underlying type);
  - `Nullable<T>`: `== null`, `HasValue`, `.Value`, with C# lifted semantics;
  - `Guid`, `decimal`, `TimeSpan`, `DateTimeOffset`.
- Strings, without allocating:
  - `ReadStringToCharSlot` decodes into a frame char scratch buffer and stores pointer + length;
  - span ops: `StartsWith`, `EndsWith`, `Contains`, `string.Equals(..., Ordinal | OrdinalIgnoreCase)`,
    `CompareOrdinal`, `Length`, `IsNullOrEmpty`;
  - long strings fall back to allocating.
- Equality on canonical encodings uses `MatchEncodedConstBr` and does not decode at all. It applies to Unsigned,
  Signed, String, StringOrderable, Bool, the 8-bit types, Guid and enums. It is not safe for floats (−0.0 and NaN)
  or for `DateTime`, because `Kind` is part of the encoding.
- `IN` lists, for example `array.Contains(p.X)`:
  - numbers: a sorted constant array searched with a binary-search op;
  - strings: a `FrozenSet<string>` searched through its `ReadOnlySpan<char>` alternate lookup.
- Planner cleanup: constant folding, dropping `&& true` and `|| false`, and ordering conjuncts by key-before-value,
  then field position, then estimated cost.

### Phase 3: Terminal operators (`Execute`)

- `Count`, `LongCount`, `Any`, `All`, `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last`, `LastOrDefault`,
  including their predicate overloads.
- `Min`, `Max`, `Sum`, `Average` over a field. They decode only that field into an accumulator slot and never
  materialize a row.
- When there is no residual predicate:
  - `Count` over a pure key range uses key indexes (`CountWithPrefix` / `GetKeyIndex`) in O(log n);
  - `Any` is a single `FindFirstKey`.

### Phase 4: Access paths (seek planning)

- Ranges on the primary key: an equality prefix followed by one range field gives start and end bounds. Only handlers
  proven order-preserving get range seeks. Each handler gets a test with random values asserting that comparing the
  encoded bytes gives the same order as comparing the values. All other handlers stay key residuals.
- Secondary indexes:
  - pick the index with the longest equality prefix, then a range field;
  - prefer the PK on a tie;
  - evaluate residuals on SK + PK-suffix fields directly on the SK key bytes;
  - fetch the primary row only for survivors (`CreateInstanceFromSecondaryKey`).
- Covering index: when the predicate, projection or aggregate needs only SK and PK fields, never touch the primary
  row.
- `OR` / `IN` on key fields becomes several seeks, merged in key order.
- Optional `.UsingIndex("Name")` hint extension.

### Phase 5: Ordering and paging

- `OrderBy`, `ThenBy` and their `Descending` forms stream when the order matches the chosen access path after its
  equality fields, running in reverse through `FindPreviousKey` when needed.
- Otherwise sort in memory:
  - decode only the sort keys plus the PK bytes;
  - with `Take`, use a top-K heap;
  - materialize rows at the end.
- `Take` stops early. `Skip` over a pure key range with no residual uses `FindKeyIndex(prefix, index + skip)` in
  O(log n).

### Phase 6: Projection (`Select`)

- Element-type change: `RelationQuery<TRow, TElement>`.
- Single-field and `ValueTuple` projections decode straight into the result.
- `MemberInit` into `[Generate]` DTOs uses `AllocObjectTo*` and `CallSetter` (`ClassMetadata`). Anonymous types and
  constructor projections fall back to `ConstructorInfo.Invoke`.
- `Select` composes with Phase 3 aggregates and Phase 5 ordering.

## Out of scope for now

`GroupBy`, `Join`, `Distinct` (except possibly `Distinct` over a single projected field), async enumeration, and
predicates over collection fields such as `p.Tags.Contains(x)`.

## Cross-cutting

- Every phase:
  - adds focused `Fact` / `Theory` tests and differential-generator coverage;
  - updates `CHANGELOG.md`;
  - updates the query section of [Relations.md](Relations.md).
- Keep `InterpreterBuilder.DumpAsm` working for the new opcode page, and add `InterpreterBuilderTest` cases for every
  new op.
- Re-run the Phase 0 benchmarks after Phases 1, 2 and 4, and record the results in `Doc/Benchmarks`.

## Open questions

- Should we change `OpCode` to `ushort`, or stay with second-level pages? This plan assumes pages.
- Should the plan cache be bounded per relation, for example an LRU?
- Unsupported expressions: should they throw (as today) or fall back to client-side evaluation? This plan assumes
  throwing, plus an explicit `AsEnumerable()` for the remainder.
