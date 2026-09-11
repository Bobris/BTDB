# Fused stored-string to ordered-string conversion

Measured 2026-09-11 on the same Apple M2 Max / .NET 10.0.12 setup as README.md.
This change builds on the batch buffer reuse described in Followup.md.

## Encoding and scope

`MemReader.CopyStringToOrdered(ref MemWriter writer)` produces the bytes of
`writer.WriteStringOrdered(reader.ReadString())` without materializing a managed string.
The formats are different: the source has a UTF-16 length plus one followed by variable-length
encoded Unicode values; the destination increments Unicode values and terminates with zero.
Null maps to the ordered null marker, not to an empty string. This is transcoding, not copying.

ASCII 0..126 is converted in 16-byte SIMD and 4-byte scalar blocks, with a direct single-byte
path for tails and mixed text. 127 and higher values use the variable-length codec. Scalar
Unicode values count as two UTF-16 units above U+FFFF. Unpaired surrogates are preserved;
separately encoded adjacent high/low surrogates are normalized to the same ordered bytes as
the original two-step operation. Invalid lengths/code points are rejected, including a scalar
that would exceed the declared UTF-16 length. Input/output controller refills remain supported.
The converter writes incrementally; malformed input need not leave output unchanged.

`RelationInfo.CreateBytesToSKSaver` selects the fused operation only by reference identity of
the standard String and StringOrderable handlers. Custom/encrypted handlers and schema type
conversions retain their existing paths. It applies to both emitted and no-emit in-order value
fields. The no-emit merger also transcodes out-of-order fields into its temporary byte writer.
The emitted out-of-order-local path still uses its existing typed locals and may allocate strings.
Computed secondary fields still load the object and invoke the computed property as before.
No persisted format changes, relation API changes or shared mutable buffers are introduced.

## Isolated conversion benchmark

```sh
DOTNET_TieredCompilation=0 dotnet run -c Release --project DBBenchmark -- string-conversion --filter '*' --artifacts /tmp/btdb-string-conversion
```

Both variants reuse pinned input and output arrays. One operation converts one string. Population
and buffer allocation are outside measurement. MixedUnicode repeats Czech text, an emoji, NUL
and U+007F, truncated to the specified UTF-16 length. BenchmarkDotNet ShortRun uses one launch,
three warmup and three measurement iterations. These are observed means; see the raw report for
error intervals, especially the short mixed-text case.

| UTF-16 units | Contents | Two-step ns | Fused ns | Two-step allocated B | Fused allocated B |
|---:|---|---:|---:|---:|---:|
| 24 | ASCII | 24.62 | 10.85 | 72 | 0 |
| 24 | Mixed Unicode | 198.21 | 157.32 | 72 | 0 |
| 8192 | ASCII | 1519.13 | 1154.54 | 16408 | 0 |
| 8192 | Mixed Unicode | 64180.26 | 51775.08 | 16408 | 0 |

The initial prototype had avoidable scalar-tail overhead; its exploratory results are excluded.
The final fast paths passed the equivalence tests with hardware intrinsics both enabled and disabled.

## End-to-end batch benchmark

```sh
DOTNET_TieredCompilation=0 dotnet run -c Release --project DBBenchmark -- secondary-key --filter '*' --artifacts /tmp/btdb-secondary-fused
```

The unchanged harness rolls back each operation and recreates the populated database before
each iteration. Batch sizes, ASCII lengths, BTree configuration and sixteen invocations per
iteration are the same as earlier measurements. Both variants retain batch writer reuse. For
the baseline only RelationInfo uses the previous merger; the new primitive remains present but
is not called by that merger. The final pair ran fused first, baseline second, sequentially.
One operation means a complete batch. Short iteration warnings and desktop timing noise remain.

| Operation | Rows | Field chars | Before us | Fused us | Before KiB | Fused KiB | Gen0 / 1000 ops before | After |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| RemoveBatch | 100 | 24 | 65.03 | 61.25 | 13.43 | 6.40 | 0.0000 | 0.0000 |
| UpdateBatch | 100 | 24 | 157.71 | 156.98 | 21.02 | 13.99 | 0.0000 | 0.0000 |
| RemoveBatch | 100 | 8192 | 511.77 | 453.34 | 2444.23 | 841.89 | 187.5000 | 0.0000 |
| UpdateBatch | 100 | 8192 | 1875.89 | 1762.47 | 10461.62 | 8859.28 | 250.0000 | 62.5000 |
| RemoveBatch | 1000 | 24 | 756.11 | 715.92 | 166.13 | 95.81 | 0.0000 | 0.0000 |
| UpdateBatch | 1000 | 24 | 1818.85 | 1821.34 | 269.67 | 199.35 | 0.0000 | 0.0000 |
| RemoveBatch | 1000 | 8192 | 8575.01 | 7777.51 | 24437.27 | 8413.84 | 2000.0000 | 0.0000 |
| UpdateBatch | 1000 | 8192 | 22419.26 | 20094.50 | 106768.42 | 90745.00 | 3187.5000 | 1250.0000 |

No Gen1/Gen2 collections were reported. Removing a 1,000-row long-key batch saves about 16 MiB
of managed allocation, including 16,408 bytes per eliminated intermediate string. Gen0 was zero
in the measured deletion iterations, not a guarantee that production deletion never causes GC.
Timing changes in the whole BTree operation are smaller than the allocation changes and do not
establish production latency improvements. Disk/log behavior and other relation fields remain.

## Validation

- SourceGenerator suite: 190 passed before the final integration checks.
- Focused suite: 24 passed (14 secondary-key relation cases and 10 transcoder cases).
- `DOTNET_EnableHWIntrinsic=0 dotnet test BTDBTest/BTDBTest.csproj --no-build --filter FullyQualifiedName~OrderedStringTranscodeTest`: 10 passed.
- Equivalence covers all 65,536 individual UTF-16 units, seeded random sequences, null/empty,
  ASCII/vector boundaries, paired and unpaired surrogates, noncanonical separately encoded pairs,
  supplementary boundaries, oversized strings and input/output controllers with tiny input buffers.
- Both emitted and no-emit relation mergers are tested through Update, UpdateById and batch deletion,
  including reversed field order and long strings. Existing schema/custom-handler tests remain in the full suite.
- `dotnet test BTDB.sln`: 1,757 passed, 9 existing skips, no failures
  (BTDBTest 1,545; SourceGenerator 190; AzureStorage 22).
- CHANGELOG.md records the new primitive and relation integration. No package was published.
