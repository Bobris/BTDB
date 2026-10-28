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
(`LsmCompaction.Clip`) recomputes `coveredBelow` by a rank query and is not modelled.

`LsmProofs/Rank.lean` models `LsmView.RankFrom` and `CountFrom` over a stack of components with range-tombstone
fragments, sorted and disjoint as `TombstoneSet` keeps them. Point deltas and `CoveredBelow` are taken relative to the
state below each component, as consistent components store them. `TombstoneSet.Find` is modelled by its result, not
its binary search.

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

The fixed rule, as implemented:

- a takeover places markers at the first missing name and at every other missing name below the next chunk;
- the markers name the writer's first own chunk;
- replay ignores a lower epoch as before, and in one of the current writer's own names it stops that writer's
  history.

`LsmWriterHandoffTest.TakeoverFencesEveryHoleSoAcknowledgedCommitsSurviveTheNextOpen` and
`PredecessorChunkInANewWritersNameEndsThatWritersHistory` reproduce both counterexamples on the code before the fix.
`TakeoversFromTheSameStateGetDifferentEpochs` reproduces `Fence.current_shares_epoch`.
