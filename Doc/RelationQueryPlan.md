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

#### Phase 1 status: done

What was built, and where it differs from the plan above:

- **Loop split.** `Interpreter.Run` picks a loop by program:
  - Programs without try blocks, which includes every query program, run a loop with no exception bookkeeping.
  - Programs with try blocks wrap the same generic loop (`Loop<TMode>`) in one try that re-enters it after
    `HandleException`. There is no longer a try around each instruction.
  - `pc` lives in a local; cold instructions get a temporary copy, so the local itself is never address exposed.
- **Frame model.** As planned, except `Params`, which is deferred to plan caching (design decision 3).
  - The enumerator owns a `long[]` frame (8-byte slots) and an `object?[]` for strings.
  - Query programs use no stack allocation and no try blocks.
- **Opcodes.** Direct `OpCode` values 152–162, not a second-level page: only 11 were needed and 104 values were free,
  so the extra dispatch was not worth it. They are:
  - `ReadKeyToFrame`, `ReadKeyToObject`, `ReadValueToFrame`, `ReadValueToObject`;
  - `SkipKey` and `SkipValue`, with a repeat count;
  - `JmpIf[Not]FrameConst`, typed by `QueryValueType` and compared by `QueryCompare`;
  - `JmpIf[Not]StringConst`;
  - `ConvertSlot`.
  Operands are fixed width. Jump targets are absolute `u32` values that `Materialize` patches from labels
  (`AddLabelTarget`).
  - `CmpSlotSlotBr` moves to Phase 2 together with property-to-property comparisons.
  - `MatchEncodedConstBr` moves to Phase 2 together with encoded-equality planning.
- **Narrowing reads.** An integer stored wider than its property, for example `Signed` → `int`, is read into the 8-byte
  slot and compared at the narrower width. On little endian this is exactly the default converter's unchecked cast, so
  there is no converter call per row. Other type differences use `ConvertSlot`.
- **Older value versions.** Properties missing in an older value version are substituted with their default value and
  constant-folded per version, so a version can compile to a constant result.
- **Removing covered conjuncts.** Equalities encoded in the cursor prefix are removed from the programs.
  - The key reader starts after the cursor prefix unless a remaining conjunct still needs a prefix field.
  - Conjuncts folding to `false` make the whole query return nothing without seeking.
- **Cursor driver.** `FastIterate` (or `FastIterateNoCursor` for key-only predicates) tests rows without moving the
  cursor per row.
  - After a match it continues from the matched row instead of calling `FindNextKey`.
  - When that row was removed in the meantime, it falls back to `FindNextKey`.
  - Plain cursor stepping measured 10–15 % slower for selective queries and 5 % faster for non-selective ones.
- **Also fixed:**
  - `Query<T>()` no longer builds a new `ItemLoaderInfo` on every call; loaders are cached per type in `RelationInfo`.
  - The enumerator's per-row `stackalloc` buffers (2 KB in `Current`, 512 B in `MoveNext`) are no longer zeroed
    (`[SkipLocalsInit]`). This alone was 45 ns per row.

Results (same machine and settings as the Phase 0 baseline, per scanned row):

| Scenario                         | Phase 0 | Phase 1 | Comparison                    |
|----------------------------------|--------:|--------:|-------------------------------|
| Value, selective (1.25 %)        | 306 ns  | 85 ns   | LINQ 136 ns                   |
| Value, non-selective (50 %)      | 393 ns  | 184 ns  | LINQ 136 ns                   |
| String equality (5 %)            | 288 ns  | 85 ns   | LINQ 133 ns                   |
| PK prefix (whole tenant matches) | 372 ns  | 127 ns  | `FindById` 128 ns             |
| PK prefix + value                | 489 ns  | 86 ns   | `FindById` + LINQ 131 ns      |
| Empty result, cost per query     | –       | 1.0 µs, 2.3 KB | `FindById` 83 ns, 96 B |

| Engine case                  | Phase 0 interpreter | Phase 1 interpreter | Closure tree | Hand-written |
|------------------------------|--------------------:|--------------------:|-------------:|-------------:|
| `Age == 35 && Active`        | 256 ns              | 57 ns               | 43 ns        | 37 ns        |
| `Age == 35 && City == "..."` | 230 ns              | 49 ns               | 47 ns        | 38 ns        |

What this means:

- **Design decision 1 holds.** The interpreter is within 1.0–1.3× of the closure-tree prototype, so we keep it.
- **Selective predicates** now beat materializing every row in LINQ by about 1.6×.
- **Non-selective predicates** are still slower than LINQ. Every matched row pays for evaluation and materialization,
  so cheaper evaluation (Phase 2 encoded matching) is the lever there.
- **Allocations on rejected rows:**
  - numeric predicates allocate nothing;
  - string predicates still allocate one string per decoded value (37 B), which Phase 2 `MatchEncodedConstBr`
    removes.
- **Per-query cost** is about 1 µs and 2.3 KB (expression parsing and program building). Plan caching
  (design decision 3) is the fix if short queries matter.

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

#### Phase 2 status: done

What was built, and where it differs from the plan above:

- **Expressions.** The AST builder now converts:
  - comparisons, `!`, bool members, numeric, enum and nullable conversions, and arithmetic, including `checked`
    and decimal operators;
  - `HasValue`, `.Value` and string `Length`;
  - string `StartsWith`, `EndsWith`, `Contains` and `Equals` with an optional `StringComparison`, plus
    `string.IsNullOrEmpty` and `string.CompareOrdinal(...)` compared to `0`;
  - `Contains` on constant collections.
  It accepts `Enumerable.Contains`, `MemoryExtensions.Contains` and instance `Contains`. C# 14 binds
  `array.Contains(x)` inside expression trees to the span overload with a `null` comparer argument.

  Constants independent of the row that are not plain members or conversions are evaluated once with
  `LambdaExpression.Compile(preferInterpretation: true)`. NativeAOT reports no warnings for it.
- **New types:** enums, `Nullable<T>`, `Guid`, `decimal`, `TimeSpan` and `DateTimeOffset`.
  - Frame slots are 8 or 16 bytes, plus 8 for a nullable HasValue byte.
  - Enum fields read through the enum handler's signedness, because `EnumFieldHandler.HandledType()` builds a type
    with Reflection.Emit.
  - Fields the query does not use but that cannot be skipped by a code op are skipped by their field handler
    (`Skip*ByHandler`), unless the handler needs a reader context.
- **New opcodes (163–182):**
  - `JmpIf[Not]FrameFrame`, `Arithmetic` and `ConvertNumber`, which follow C# rules: decimal conversions are always
    checked, and `checked` operations throw `OverflowException`;
  - `CaptureKey` / `CaptureValue`, `JmpIf[Not]EncodedEqual`, `JmpIf[Not]EncodedStartsWith`;
  - `DecodeString`, `JmpIf[Not]StringOp`, `StringLength`, `JmpIf[Not]FrameInSet`;
  - `SetFrameByte`, `ThrowQueryException`, `Skip*ByHandler`.
- **Strings never allocate.**
  - Every string field is captured as a pointer and length into the pinned key or value bytes.
  - Equality, static ordinal `Equals` and ordinal `StartsWith` compare encoded bytes. Both string formats keep the
    prefix property, except when a prefix ends with a high surrogate; then the string is decoded.
  - Other string operations decode into a reused `char[]` per field and use span APIs. Culture comparisons use the
    current culture at evaluation time, like LINQ.
  - The ordered key format is decoded by a dedicated loop. `MemReader.ReadStringOrderedAsSpan` returns copies, which
    made the first version grow its buffer on every row.
- **Encoded equality covers strings only.**
  - For numbers it saves only about 5 ns, decoding being cheap.
  - It also differs from C# for integer fields stored wider than their property: an old `long` field read as `int`
    is truncated in C#, but would not be in a byte comparison.
- **`IN` lists use `FrozenSet<T>`.**
  - This matches the default equality `Enumerable.Contains` uses, for example NaN is found.
  - Strings use the `ReadOnlySpan<char>` alternate lookup.
  - A `HashSet` with a custom comparer is not supported.
- **Exception semantics.** `Simplify` folds constants without dropping conjuncts that can throw. Planning also keeps
  C# short-circuit exceptions:
  - A conjunct that can throw waits for every conjunct written before it, and later conjuncts wait for it.
  - Only conjuncts before the first throwing one can become the cursor prefix or move into the key program.
  - Between such barriers, conjuncts are ordered by the field they need and then by estimated cost.
- **Tests.**
  - The differential test covers every shape above, with exceptions: the query must throw the same exception type
    after returning the same rows.
  - Non-ASCII ordered keys are included.
  - Mutation checks catch broken comparison mirroring, broken lifted null comparisons, disabled exception barriers
    and a wrong `StartsWith` prefix.

Results (same machine and settings as before, per scanned row):

| Scenario                  | Phase 1      | Phase 2            |
|---------------------------|-------------:|-------------------:|
| String equality (5 %)     | 85 ns, 45 B  | 76 ns, 8 B         |
| Engine, string equality   | 49 ns, 37 B  | 44 ns, 0 B         |
| Engine, numeric           | 57 ns        | 59 ns              |
| Empty result, per query   | 1.0 µs, 2.3 KB | 1.3 µs, 3.8 KB   |

The string engine case is now faster than the closure-tree prototype (50 ns) and close to hand-written decoding
(39 ns). The remaining 8 B per scanned row in the string scenario is the materialization of matching rows.

The AOT smoke app now also covers ranges, captured arithmetic, enums, enum `Contains`, nullables, `decimal`, `!`,
string methods and string sets.
- It reports 0 warnings in the query path and 218 elsewhere. The extra warning comes from `DBObjectFieldHandler` for
  the new field types.
- `run.sh` now cleans before publishing: an up-to-date publish skipped the NativeAOT compiler, reported no warnings,
  and made the warning count fail.

NativeAOT caveat found by the smoke app: the C# compiler builds `decimal` comparisons in expression trees without
operator metadata, so System.Linq.Expressions looks the operators up by reflection, and trimming removes them. Apps
need `[DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(decimal))]`. This is documented in
[Relations.md](Relations.md#query).

### Phase 3: Terminal operators (`Execute`)

- `Count`, `LongCount`, `Any`, `All`, `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last`, `LastOrDefault`,
  including their predicate overloads.
- `Min`, `Max`, `Sum`, `Average` over a field. They decode only that field into an accumulator slot and never
  materialize a row.
- When there is no residual predicate:
  - `Count` over a pure key range uses key indexes (`CountWithPrefix` / `GetKeyIndex`) in O(log n);
  - `Any` is a single `FindFirstKey`.

#### Phase 3 status: done

What was built, and where it differs from the plan above:

- **`IQueryProvider.Execute`** handles:
  - `Count`, `LongCount`, `Any` and `All`, the last as `!Any(!predicate)`, which tests the same rows;
  - `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last` and `LastOrDefault`, with predicate and
    default-value overloads;
  - `Min`, `Max`, `Sum` and `Average` with a selector.
- **Scanner.** `RelationQueryScanner` replaces the enumerator internals. It offers:
  - `MoveNext` (FastIterate to the next match);
  - `MoveLast` (backwards with `FindLastKey` / `FindPreviousKey`);
  - `Count`;
  - `Aggregate`, which passes every matching row's frame to an aggregator inside one FastIterate, without stopping.
  The enumerator materializes rows on top of it.
- **Selectors** compile into the last program a row runs: the value programs when there are value conjuncts or the
  selector needs value fields, otherwise the key program. They are specialized per value version like conjuncts and
  copied into a result slot by the new `CopyToFrame` opcode (183).
- **`Count` without programs** (no predicate, or only prefix equalities) uses `FindFirstKey` / `FindLastKey` key
  indexes in O(log n). `Any` without programs is a single `FindFirstKey`.
- **Aggregates follow `Enumerable` exactly:**
  - checked `int` and `long` sums;
  - `float` summed as `double`;
  - `Average` of integers as checked `long` divided as `double`;
  - `Min` / `Max` through `Comparer<T>.Default`, so NaN is the smallest double;
  - nulls skipped;
  - an empty non-nullable `Min`, `Max` or `Average` throws, while nullable ones return null;
  - enum results are boxed as the enum.
- **`Last` follows LINQ-to-Objects over a list.** It searches backwards and stops at the first match from the end.
  The exception is `Last(predicate)` after `Where`: LINQ's `Where` iterator is not an `IList`, so it tests every row
  forwards. The difference decides which rows can throw, and the differential test found both cases.
- **Tests.**
  - A second differential test applies 42 terminal operators to random filtered sources, comparing values and
    exception types and messages with LINQ-to-Objects. Selectors use real C# lambdas.
  - Mutation checks catch an off-by-one key-index count, swapped `Min` / `Max` and a missing second-match check in
    `Single`. A `float` sum accumulated as `float` is caught by a focused test with values only `double` keeps.

Results (same machine and settings as before):

| Scenario                               | Query          | Comparison                                   |
|----------------------------------------|---------------:|----------------------------------------------|
| `Count()` of the relation, per call    | 0.85 µs        | relation `Count` 0.21 µs                     |
| `Count(p => p.TenantId == x)`, per call | 1.8 µs        | `FindById(x).Count()` 67 µs (10 000 rows)    |
| `Count(p => p.Active)`, per row        | 88 ns, 0 B     | LINQ 127 ns, 149 B                           |
| `Sum(p => p.Score)`, per row           | 84 ns, 0 B     | LINQ 123 ns, 149 B                           |
| Empty result, per query                | 1.4 µs, 3.9 KB | `FindById` 92 ns                             |

What this means: per-row work is fast. The fixed per-query cost of about 1 µs (parsing, planning and building
programs) now dominates `Count` and `Any` on key ranges. Plan caching (design decision 3) would remove most of it.

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
