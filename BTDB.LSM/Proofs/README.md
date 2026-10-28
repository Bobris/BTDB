# BTDB.LSM proofs

Lean 4 models of the `LsmKeyValueDB` write-ahead log and counted entries, with machine-checked proofs. They use only
the Lean core library (no Mathlib).

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

- multi-chunk records, generations, flushes and the manifest's WAL start (the epoch it records is argued in
  `../ImplementationPlan.md`, section 14);
- manifest fencing, which the model replaces by fresh, increasing epochs;
- record encoding. A record is an opaque `Nat`.

## Results

| Theorem | Statement |
| --- | --- |
| `Wal.current_loses_acknowledged` | Under the rule before the fix, writer 1 crashes with chunks 1, 3 and 5 landed. Writer 2 fences only name 2 and acknowledges chunk 6. Replay stops at the hole at name 4 and loses the commit. |
| `Wal.current_replays_a_gap` | Under the rule before the fix, writer 1's delayed chunk takes writer 2's first own name. Replay then applies writer 2's next chunk, and the result is neither writer's history. |
| `Wal.fixed_keeps_acknowledged` | With the fixed rule, each writer's replayed base plus its acknowledged chunks is a prefix of every replay that stops at its first missing name. |
| `Wal.fixed_replays_one_history` | With the fixed rule, every replay is empty or a prefix of one writer's history. |
| `Wal.fixed_commits` | The fixed rule still lets a writer take over, write and acknowledge, so the results above are not vacuous. |
| `Counts.total_telescopes` | Over a consistent stack of components, the count contributions at a key sum to `[exists on top] − [exists below]`, so counts and ranks add across components (L5). |
| `Counts.merge_final`, `merge_consistent`, `merge_total` | The component `LsmMerger` makes from a run reaches the same state at the key, is consistent with the state below the run, and contributes the same count. |

The fixed rule, as implemented:

- a takeover places markers at the first missing name and at every other missing name below the next chunk;
- the markers name the writer's first own chunk;
- replay ignores a lower epoch as before, and in one of the current writer's own names it stops that writer's
  history.

`LsmWriterHandoffTest.TakeoverFencesEveryHoleSoAcknowledgedCommitsSurviveTheNextOpen` and
`PredecessorChunkInANewWritersNameEndsThatWritersHistory` reproduce both counterexamples on the code before the fix.
