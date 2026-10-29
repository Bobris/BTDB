# BTDB.LSM proofs

Lean 4 models of the `LsmKeyValueDB` write-ahead log, manifest fencing and counted entries, with machine-checked
proofs. They use only the Lean core library (no Mathlib).

```bash
lake build                 # checks every proof
lake env lean Check.lean   # prints the axioms each main theorem uses
```

`lean-toolchain` pins Lean 4.35.0; install it with [elan](https://github.com/leanprover/elan). The theorems depend
only on the standard axioms `propext`, `Quot.sound` and `Classical.choice`; there is no `sorry`.

## Models

`LsmProofs/Counts.lean` models counted entries at one key (`ImplementationPlan.md`, sections 6.1 to 6.3). A component
holds at most one point entry and may cover the key with a range-tombstone fragment. The model includes how
`LsmMerger.Entries` and its merged fragments combine a run of components. Splitting fragments at table boundaries
(`LsmCompaction.Clip`) recomputes `coveredBelow` by a rank query; `LsmProofs/ClipCoverage.lean` shows the pieces keep
the fragment's coverage.

`LsmProofs/Rank.lean` models `LsmView.RankFrom` and `CountFrom` over a stack of components with range-tombstone
fragments, sorted and disjoint as `TombstoneSet` keeps them. Point deltas and `CoveredBelow` are taken relative to the
state below each component, as consistent components store them. `TombstoneSet.Find` is modelled by its result, not
its binary search.

`LsmProofs/BlockSeek.lean` models `RawBlock.LowerBound`: finding the first restart past the key, then scanning the
entries of the restart before it. The binary search over restarts is modelled by its result.

`LsmProofs/MergeMask.lean` models how `MergingIterator` resolves a key over components newest first, by a point entry
or else a range-tombstone fragment.

`LsmProofs/TombstoneFind.lean` models `TombstoneSet.Find` over sorted, disjoint fragments, with its binary search
modelled by its result.

`LsmProofs/BlockDelta.lean` models `RawBlock.DeltaPrefix`: the delta stored at the restart of the last counted entry
plus the deltas decoded after it, and the prefix bounds `DataBlockBuilder` tracks.

`LsmProofs/TablePointDelta.lean` models `Table.PointDeltaBefore`: the delta stored before the block that may hold
the key plus the block's delta prefix up to the key.

`LsmProofs/MemtableDelta.lean` models `Memtable.PointDeltaBefore`: one walk down a search tree whose nodes keep their
subtree delta sums.

`LsmProofs/MemtableTree.lean` models `Memtable.Split` and `Merge`, which every memtable insert and removal goes
through, and `UpdatePath`; the priority that picks the root of a merge is left arbitrary.

`LsmProofs/ClipCoverage.lean` models `LsmCompaction.Clip`, which cuts range-tombstone fragments at the boundaries of
the tables a compaction writes.

`LsmProofs/PrefixSuccessor.lean` models `LsmKeys.PrefixSuccessor` over byte strings in lexicographic order.

`LsmProofs/Varint.lean` models the LEB128 integers of `LsmEncoder.WriteVUInt64` and `LsmDecoder.ReadVUInt64`.

`LsmProofs/FuzzyCursor.lean` models `LsmKeyValueDBCursor.NotifyRenamed`: how cursors that are current, fuzzy at an
erased key, or fuzzy right before a live key follow a key suffix update by another cursor.

`LsmProofs/WalFlush.lean` extends the WAL model with flushes. The newest writer publishes a manifest whose tables
hold its base and its first own chunks' records, landed or not, and whose WAL start is its next own chunk with its
epoch. Readers, recovery and takeovers then replay from the newest manifest.

`LsmProofs/Fence.lean` models how a takeover gets its epoch: one more than the highest it read, fenced by creating
the next manifest name, where equal content counts as its own lost response. Reads may be arbitrarily old. Deleting a
replaced manifest frees its name, so the results assume that no manifest create lands later than `ManifestRetention`
(1 minute) after its dispatch; `late_create_after_deletion` shows why.

`LsmProofs/Reload.lean` models one writer instance: commits publish locally, uploads make the local state durable,
and ending the role clears the writer flag and then reloads from storage (`CommitWritingTransaction`,
`LoseWriterAsync`, and `StepDownAsync` after a failed upload).

`LsmProofs/Refresh.lean` models reader refresh rounds (`CatchUpAsync`): a round advances its replay past the
generations of the manifest it found, then reads WAL chunks and may fail, and a later round may find no newer manifest
because the probed name was deleted and listing is throttled.

`LsmProofs/Wal.lean` models WAL chunks as immutable create-if-absent objects `wal/{n}` (`CreateChunkAsync`). Each
writer session has a fresh epoch. A takeover lists the WAL, replays its contiguous prefix and fences it with epoch
markers (`BecomeWriterAsync`). Readers, recovery and takeovers replay names in order up to the first missing one
(`WalReplay.Apply`, `ReplayListedWalAsync`, `CatchUpAsync`).

Writers are never stopped. A fenced, crashed or expired writer keeps every chunk it planned, and each planned chunk
or fence may land at any later time while its name is free. That covers parallel uploads completing out of order,
delayed requests and lost responses. Safety does not rely on the lease; writers may even overlap. A writer
acknowledges the longest prefix of its planned chunks that landed with its content, which is at least what
`UploadWalAsync` acknowledges.

Not modelled in `Wal.lean`:

- multi-chunk records and generations. Flushes are in `WalFlush.lean`, for starts in the flushing writer's own
  range; a start in the replayed region before them preserves the replay (`restart_agrees`);
- manifest fencing, which the model replaces by fresh, increasing epochs. `Fence.lean` proves that manifest fencing
  provides them;
- record encoding. A record is an opaque `Nat`.

## Results

| Theorem | Statement |
| --- | --- |
| `Wal.current_loses_acknowledged` | Under the rule before the fix, writer 1 crashes with chunks 1, 3 and 5 landed. Writer 2 fences only name 2 and acknowledges chunk 6. Replay stops at the hole at name 4 and loses the commit. |
| `Wal.current_replays_a_gap` | Under the rule before the fix, writer 1's delayed chunk takes writer 2's first own name. Replay then applies writer 2's next chunk, and the result is neither writer's history. |
| `Wal.fixed_keeps_acknowledged` | With the fixed rule, each writer's replayed base plus its acknowledged chunks is a prefix of every replay that stops at its first missing name. |
| `Wal.fixed_replays_one_history` | With the fixed rule, every replay is empty or a prefix of one writer's history. |
| `Wal.restart_agrees`, `Wal.fixed_restart_replays` | Restarting replay at a name the full replay reaches unbroken and in its writer's own range, with only that writer's epoch, applies exactly what the full replay applies there. That is the context a manifest records (`WalStartEpoch`), so both results above hold for readers starting at a manifest. |
| `WalFlush.flush_keeps_acknowledged` | With flushes, every acknowledged history is a prefix of every replay from the newest manifest that stops at its first missing name, tables included. |
| `WalFlush.flush_replays_one_history` | With flushes, every replay from the newest manifest, tables included, is empty or a prefix of one writer's history. |
| `WalFlush.flush_of_unlanded_chunk` | A flush whose own chunk never landed is reachable, and readers see its record from the tables: the case `restart_agrees` cannot cover. |
| `Wal.unacknowledged_replayed` | A chunk its writer never acknowledged can land and be replayed by the next writer, so a commit whose durability wait failed may still be history. Before the 2026-10-28 fix, a spilled commit whose manifest might still land was reported as lost before the commit. |
| `Reload.current_lost_commit_visible` | Before the 2026-10-28 fix, a commit that passed the writer check published after losing the role reloaded the state, so the instance showed a commit storage did not have (`LsmWriterHandoffTest.CommitRacingLeaseLossDoesNotOutliveTheReload`). |
| `Reload.fixed_reload_matches` | With the check and the publish under the state lock that ending the role also takes, the state after the reload is the durable state. Before the 2026-10-28 fix, a release whose WAL upload failed did not reload at all (`ReleaseWhoseUploadFailsReloadsTheDurableState`). |
| `Refresh.current_mixed_state` | Before the 2026-10-28 fix, a round that advanced to a newer manifest and then failed left its replay ahead, and the next round published older tables with a replay that dropped newer generations (`LsmReaderTest.RefreshFailingAfterANewerManifestNeverPublishesAMixedState`). |
| `Refresh.fixed_consistent` | When such a failure marks the replay stale and the next round reloads, every published state is consistent. |
| `Fence.current_shares_epoch` | Before the 2026-10-28 fix, two takeovers that read the same state wrote equal fencing manifests. The second took the first one's manifest for its own, and both wrote with one epoch. |
| `Fence.fixed_fresh_epochs` | With the writer session in every manifest, sessions that fenced in have different epochs, and the one fenced at a later name has the higher epoch: the assumption of `Wal.lean` and `WalFlush.lean`. |
| `Fence.late_create_after_deletion` | Once a replaced manifest is deleted, a fencing create that lands later takes its name again with an epoch already in use. Before the 2026-10-28 fix, a grace period below a minute deleted replaced manifests at once, so a create delayed by milliseconds could do that. |
| `Wal.fixed_commits` | The fixed rule still lets a writer take over, write and acknowledge, so the results above are not vacuous. |
| `Counts.total_telescopes` | Over a consistent stack of components, the count contributions at a key sum to `[exists on top] − [exists below]`, so counts and ranks add across components (L5). |
| `Counts.merge_final`, `merge_consistent`, `merge_total` | The component `LsmMerger` makes from a run reaches the same state at the key, is consistent with the state below the run, and contributes the same count. |
| `Rank.rank_component` | One component's step of `RankFrom` is exact: the live keys below `x` are its point deltas below `x`, less the `CoveredBelow` of its fragments ending at or before `x`, plus the rank below the component at the start of the fragment containing `x`, else at `x`. |
| `Rank.rank_correct`, `Rank.count_correct` | `RankFrom` over a stack is the number of live keys below `x`, and `CountFrom`, the sum of `DeltaSum − CoveredSum`, is the number of live keys. |
| `FuzzyCursor.notify_position`, `FuzzyCursor.notify_kind` | After `UpdateKeySuffix` replaces a key by the only key with its prefix, every cursor of the transaction has the position it had, and current cursors stay current and fuzzy ones fuzzy. Before the 2026-10-28 fix a fuzzy cursor kept its erased key, which the renamed key could pass (`LsmRegressionTest.KeySuffixUpdateKeepsFuzzyCursorOnItsSide`), and a cursor whose erased key was created again did not follow it (`CursorOnRecreatedKeyFollowsKeySuffixUpdate`). |
| `BlockSeek.lower_bound_correct` | Seeking in a data block of sorted entries with a restart every `R` entries, by the first restart past the key and a scan of the restart before it, returns the first entry at or after the key (after it, when the key is excluded), or the entry count. |
| `MergeMask.decided_ignores_older`, `MergeMask.newest_point_wins` | Once a component decides a key, by a point or a fragment, older components cannot change whether it is live, so `MergingIterator` seeking them past a newer fragment skips no live key; the newest point at a key decides it. |
| `TombstoneFind.find_some`, `TombstoneFind.find_covered` | `TombstoneSet.Find` returns a fragment exactly when it contains the key, and the fragments its `coveredBefore` sums are exactly those ending at or before the key, the result `Rank.lean` assumes. |
| `BlockDelta.delta_prefix_correct` | `DeltaPrefix(count)`, the delta stored at the restart of entry `count - 1`, capped at the last restart, plus the deltas of the entries from it to `count`, is the sum of the deltas of the first `count` entries. |
| `BlockDelta.bounds_cover` | The `MinPrefix` and `MaxPrefix` a block builder tracks, starting at 0 and updated after each entry, bound every prefix sum of the block, the empty one included, as `Table.PointDeltaBounds` needs. |
| `TablePointDelta.point_delta_before_correct` | `Table.PointDeltaBefore`, the index's delta before the last block starting at or before the key plus the deltas of that block's entries below the key, is the sum of the deltas of all the table's entries below the key. |
| `TablePointDelta.level_point_delta_before_correct` | `LevelComponent.PointDeltaBefore` composes its tables the same way, by the first table ending after the key, and a key after every table gets the level's whole delta sum. |
| `MemtableDelta.delta_before_correct` | `Memtable.PointDeltaBefore`, adding the left subtree sum and the node's delta at each node below the key on one path from the root, is the sum of the deltas of all keys below the key, for a search tree whose nodes keep their subtree sums. |
| `MemtableTree.split_ok`, `MemtableTree.merge_ok` | `Split` of a search tree with exact subtree sums gives two such trees holding the keys below and the rest, and `Merge` of two such trees, left keys below right keys, gives one, whatever the priorities, so `PointDeltaBefore` stays exact after every change. |
| `MemtableTree.update_ok` | `UpdatePath` on a key the tree holds keeps it a search tree with exact subtree sums, and changes its total by the new delta less the old one. |
| `ClipCoverage.pieces_cover`, `ClipCoverage.dropped_zero` | A fragment cut at any table boundaries, each piece covering `Rank(end) − Rank(start)`, keeps the fragment's coverage in sum, and dropping pieces covering nothing does not change it, so `CoveredSum` and `CountFrom` stay exact after a compaction splits its output. |
| `PrefixSuccessor.prefix_successor_correct`, `PrefixSuccessor.no_successor` | A key is below `PrefixSuccessor(p)` exactly when it is below `p` or starts with `p`, so a scan from `p` to it visits exactly the keys with the prefix; for a prefix of `0xff` bytes every key is below it or starts with it, so null is right. |
| `Varint.round_trip`, `Varint.decoded_fits` | Every value below 2^64 decodes back from its encoding, leaving the bytes after it, and every value the decoder accepts is below 2^64; before the 2026-10-29 fix a tenth byte above 1 was accepted and lost bits (`LsmEncodingTest.OverlongValueIsDamage`). |
| `Varint.zigzag_round_trip`, `Varint.zigzag_range` | The zigzag mapping of `WriteVInt64` is undone by `ReadVInt64`, and a value is a `long` exactly when its image is below 2^64, so every `long` reads back as written. |

The fixed rule, as implemented:

- a takeover places markers at the first missing name and at every other missing name below the next chunk;
- the markers name the writer's first own chunk;
- replay ignores a lower epoch as before, and in one of the current writer's own names it stops that writer's
  history.

`LsmWriterHandoffTest.TakeoverFencesEveryHoleSoAcknowledgedCommitsSurviveTheNextOpen` and
`PredecessorChunkInANewWritersNameEndsThatWritersHistory` reproduce both counterexamples on the code before the fix.
`TakeoversFromTheSameStateGetDifferentEpochs` reproduces `Fence.current_shares_epoch`.
