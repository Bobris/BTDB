# Reusing secondary-key buffers across deleted rows

The string-conversion candidate below was subsequently implemented; see [fused conversion](FusedString.md).

Follow-up measured on 2026-09-11. Baseline is the initial stack-buffer change from README.md,
remeasured during this session; it is not the original unoptimized HEAD. The benchmark harness,
machine and settings are unchanged. All figures below are per complete batch.

```sh
DOTNET_TieredCompilation=0 dotnet run -c Release --project DBBenchmark -- secondary-key --filter '*' --artifacts /tmp/btdb-secondary-stage2
```

## Implementation

- Prefix, partial and range deletion now create one writer outside the row loop, passed by reference
  into secondary-index removal. A heap buffer grown for one row is reset and reused by subsequent rows.
- Partial deletion keeps one secondary-index cursor for the call instead of obtaining/disposing one per row.
  This cursor lifecycle change is regression-tested but is not separately timed by this prefix benchmark.
- Single-row deletion retains the local stack-buffer wrapper. Update is unchanged in this follow-up.

No buffer is cached in relation metadata or shared between calls. The temporary memory lives only
for the synchronous deletion call, including when callbacks reenter another call. It is reset between
indexes and rows after cursor operations finish. Computed values, veto callbacks, free-content handling,
ordering, partial limits and persisted key format retain their existing paths. Oversized buffers grow
as before but their allocation is amortized across the batch.

## First comparison

| Operation | Rows | Field chars | Stage 1 us | Stage 2 us | Stage 1 KiB | Stage 2 KiB |
|---|---:|---:|---:|---:|---:|---:|
| RemoveBatch | 100 | 24 | 58.27 | 57.27 | 13.43 | 13.43 |
| UpdateBatch | 100 | 24 | 145.97 | 144.03 | 21.02 | 21.02 |
| RemoveBatch | 100 | 8192 | 723.16 | 487.13 | 4827.19 | 2444.23 |
| UpdateBatch | 100 | 8192 | 1747.31 | 1631.80 | 10461.62 | 10461.62 |
| RemoveBatch | 1000 | 24 | 701.86 | 678.46 | 166.13 | 166.13 |
| UpdateBatch | 1000 | 24 | 1753.59 | 1698.49 | 269.67 | 269.67 |
| RemoveBatch | 1000 | 8192 | 10213.49 | 8884.91 | 48483.51 | 24437.27 |
| UpdateBatch | 1000 | 8192 | 21108.25 | 21134.28 | 106768.44 | 106768.44 |

For 8,192-character fields, deletion allocations fall by approximately 49.4% (100 rows)
and 49.6% (1,000 rows), beyond the initial stack-buffer optimization. Short-key allocations remain
unchanged. Update allocations are unchanged; its timing variation is a control, not evidence of
an additional update optimization. Gen0 counts match stage 1 (187.5 and 2,000 per 1,000 deletion
operations for 100 and 1,000 long-key rows); no Gen1/Gen2 collections were reported.

## Further opportunities examined

`RelationInfo.CreateBytesToSKSaver` loads a stored string and saves it using the ordered string
handler. `MemReader.ReadString` allocates a new UTF-16 string; for 8,192 characters this is about
16 KiB per row, even though no user object needs it. A direct serialized-string-to-ordered-string
conversion is the next concrete candidate, particularly for long fields. It is not implemented here:
it needs equivalence coverage for null, empty, ASCII boundaries, NUL, surrogate pairs and unpaired
surrogates, reader controller boundaries, schema conversions and both emitted/no-emit mergers.
The remaining allocations also include transaction-log storage; this benchmark does not isolate
every remaining allocation stack.

Caching merger delegates or removing the relation enumerator would target relatively small fixed
costs. Existing prefix erasure cannot remove the whole Messenger secondary-index batch as one
contiguous range: CustomField precedes BatchId in the secondary key, so a batch is not generally
a secondary-key prefix. Changing this requires a different index layout and is outside this fix.

The same production limitations as README.md apply: synthetic sizes, in-memory files, no production
latency or GC-pause proof, and no package publication. Short iteration timing warnings still apply.

## Reverse-order confirmation

A second run measured stage 2 first, then stage 1, with `--filter '*RemoveBatch*'`.

| Rows | Field chars | Stage 1 us | Stage 2 us | Time reduction |
|---:|---:|---:|---:|---:|
| 100 | 24 | 62.32 | 62.73 | -0.7% |
| 100 | 8192 | 811.35 | 506.94 | 37.5% |
| 1000 | 24 | 729.27 | 701.59 | 3.8% |
| 1000 | 8192 | 10896.27 | 8341.45 | 23.4% |

Allocation and collection counts matched the first comparison.

## Validation

- SourceGenerator suite: 190 passed.
- All 12 secondary-key regression cases passed on both stage 1 and stage 2.
- Added growth/shrink across rows and multiple indexes, prefix/partial/range deletion, ascending
  and descending range traversal, computed keys and OnBeforeRemove veto coverage.
- `dotnet test BTDB.sln`: 1,745 passed, 9 existing skips, no failures
  (BTDBTest 1,533; SourceGenerator 190; AzureStorage 22).
- CHANGELOG.md updated; benchmark reports preserve the initial and follow-up stages separately.
