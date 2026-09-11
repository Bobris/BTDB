# Direct VInt / VUInt copying in secondary-key mergers

Follow-up measured on 2026-09-11 after the fused string conversion. Built-in Signed and Unsigned
handlers use the same varint representation for stored values and ordered index fields. Their
encoded values can be copied without numeric decoding and re-encoding.

## Implementation

`MemReader.CopyVInt64ToWriter` and `CopyVUInt64ToWriter` derive the encoded length from the first
byte. A one-byte encoding writes directly; contiguous multibyte encodings use WriteBlock. Values
split across reader buffers use a separate slow path with a nine-byte stack buffer. Keeping that
stack allocation out of the fast helper allows inlining. These are raw-copy operations: they
preserve the input representation, including noncanonical encodings, rather than normalizing it.
Database writers produce canonical encodings. Truncated input is rejected.

`RelationInfo.GetSerializedFieldCopy` selects the string transcoder or numeric copier once when
building the merger. Integer copying requires reference identity with the corresponding built-in
Signed or Unsigned handler on both sides. Matching names alone, custom handlers, enum handlers,
signed-to-unsigned changes and other conversions do not qualify. The existing conversion path
remains the fallback. Emitted in-order fields and no-emit in-order/out-of-order fields use the
copier; emitted out-of-order typed locals keep the existing path. No wire-format changes.

The current no-emit primary-key path already skipped and copied primary-key fields directly.
This follow-up extends the optimization to eligible stored value fields (and eligible emitted
in-order primary-key fields), rather than assuming all same-typed serializers are interchangeable.

## Isolated operation

```sh
DOTNET_TieredCompilation=0 dotnet run -c Release --project DBBenchmark -- integer-copy --filter '*' --artifacts /tmp/btdb-integer-copy
```

Both variants reuse pinned input/output arrays; one operation processes one value. Small values
are 42; wide values are long.MinValue or ulong.MaxValue. BenchmarkDotNet ShortRun on Apple M2 Max,
.NET 10.0.12, same host as the preceding reports. These are measured means, not production estimates.

| Encoding | Encoded bytes | Decode + encode ns | Copy ns | Allocated, both |
|---|---:|---:|---:|---:|
| VUInt | 1 | 7.470 | 2.247 | 0 B |
| VUInt | 9 | 7.444 | 4.220 | 0 B |
| VInt | 1 | 8.179 | 2.804 | 0 B |
| VInt | 9 | 7.964 | 5.013 | 0 B |

This saves CPU, not a per-value object allocation. The contiguous paths avoid reconstructing the
integer and recalculating/serializing its varint representation.

## Batch deletion

```sh
DOTNET_TieredCompilation=0 dotnet run -c Release --project DBBenchmark -- integer-secondary --filter '*' --artifacts /tmp/btdb-integer-secondary
```

Each operation deletes 1,000 records with one Signed and one Unsigned secondary index, then rolls
back. Each iteration starts with a fresh populated database; sixteen invocations, ten iterations.
Short values repeat within the one-byte ranges; wide values are unique near the 64-bit limits.
Their BTree layouts differ, so compare before/after within a row, not short versus wide.
The baseline retains fused string conversion and batch buffer reuse but has the previous numeric
merger. The final comparison ran optimized first, baseline second. BTree/log work dominates and
short-iteration warnings apply. Reported allocation also includes transaction and cold metadata
work; differences there are not per-integer allocation savings.

| Values | Before ms | After ms | Before KiB | After KiB |
|---|---:|---:|---:|---:|
| Short | 1.595 | 1.601 | 72.66 | 71.85 |
| Wide | 1.277 | 1.226 | 88.67 | 87.87 |

The whole-operation differences are small and should not be treated as a robust production
speedup. No collections were reported in these numeric batch measurements. A rerun of the existing
string batch benchmark retained its allocation savings; the raw string-control report is included.

## Validation

- SourceGenerator suite: 190 passed.
- Focused suite: 36 passed, covering varint copies, string transcoding and secondary-key maintenance.
- Varint tests cover all bit-width boundaries and extrema, one-/two-/seven-byte input buffers,
  output controller boundaries and truncated encodings.
- Relation tests cover emitted/no-emit mergers, signed/unsigned extremes, reversed secondary-field
  order, UpdateById, stale-index removal and batch deletion.
- `dotnet test BTDB.sln`: 1,769 passed, 9 existing skips, no failures
  (BTDBTest 1,557; SourceGenerator 190; AzureStorage 22).
- CHANGELOG.md updated. No package publication or production changes.
