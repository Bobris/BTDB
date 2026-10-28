# Event log and replication proofs

Lean 4 models of the event-log commit protocol in `../EventLog/` (library `EventLogProofs`) and of replication lease
authority (library `ReplicationProofs`), with machine-checked proofs. They use only the Lean core library (no
Mathlib).

```bash
lake build                 # checks every proof
lake env lean Check.lean   # prints the axioms each main theorem uses
```

`lean-toolchain` pins Lean 4.35.0; install it with [elan](https://github.com/leanprover/elan). A clean build takes a
few seconds. Every theorem depends only on the standard axioms `propext`, `Quot.sound` and `Classical.choice`; there
is no `sorry`.

## Models

| File | Models | Code |
| --- | --- | --- |
| `Chain.lean` | Level-0 split chain: conditional appends to the writable tail (combined takeover, seal-only writes, owner change), successor creation after the lane's own seal, adopting a helper's header-only successor, header-only successors created by anybody after a seal, candidates reading the tail, cleanup marks on sealed splits | `EventLogOwnerLane.AppendAsync`, `CreateSuccessorAsync`, `SetOwnerAsync`, `CreateCandidateAsync`, `ValidateAsync`, `EventLogStorageReader.FindTailAsync`, `EventLogMerger` |
| `Bounds.lean` | One versioned topic history; lanes whose conditional writes are dispatched, applied and answered separately; `ValidateAsync` calls answering from a write or a tail check | `EventLogOwnerLane.ValidateAsync`, `WriteAsync`, `SetOwnerAsync` |
| `Merge.lean` | Splits and any number of merged levels (`MergeLevels`) with fan-out `F`; merges once a later split exists; deletions justified by a covering object that existed at some point, as concurrent and deposed mergers with stale listings and verification caches decide | `EventLogMerger.RunPassAsync`, `IsGarbageAsync` |
| `ReplicationProofs/Lease.lean` | Monotone node clocks within the drift bound; the lease deadline from request dispatch, confirmation grants and the handoff drain bound, with the code's integer rounding | `LeaseAuthority.UsableDuration`, `BoundPeerWindow`, `CanGrant`; `ConfirmationGrants`; the poll evidence window in `ReplicationNodeCoordinator` |
| `ReplicationProofs/Lineage.lean` | The walk back over native TRL headers (`PreviousFileId`) from an advertised end to the current file | `TrlLineage.SuccessorsAsync` |
| `LateCreate.lean` | Split names that merging deletes only while a later split exists, and create-if-absent writes that land at any time | `EventLogOwnerLane.CreateSuccessorAsync`, `CreateCandidateAsync`, `EventLogMerger` deletions |
| `StopSettles.lean` | An owner lane's queue, the batch a write takes from it, failed or succeeded writes and stops from outside | `EventLogOwnerLane.RunAsync`, `Stop` |
| `DisposeAttach.lean` | Service disposal (cancel, then detach under the topic lock) racing a takeover that attaches a candidate lane | `EventLogService.DisposeAsync`, `EventLogTopic.Attach`, `ResolveOwnerAsync` |
| `Linger.lean` | Arrivals binned by delay after the moment a write would start, and the gain test of each window, with decay keeping empty bins empty | `CommitLingerEstimator` (event log owner and LSM WAL uploader) |
| `ReplicationProofs/Attempts.lean` | This node's attempt journal: the last started offset and its count; runs restarting below an event and consuming ascending offsets | `EventAttemptJournal.BeforeAttemptAsync` (`../../BTDB.Replication.EventSourcing/EventPoison.cs`) |
| `ExactlyOnce.lean` | One versioned topic history of transfer identities `(session, sequence)`; owner lanes with per-session state and queues; publishers with resolved sequences, `ResumeFrom` and `_knownNext`; a network that keeps every submission, so each can arrive late, repeatedly and at any lane | `EventLogOwnerLane.SubmitAsync`, `AcceptLocked`, `ResolveSessionAsync`, `CommitAsync`; `EventLogTopic.PrepareRound`, `Resolve`, `PeerAnswered` |

Every step is atomic and may happen at any time, so the proofs cover all interleavings of the modelled operations,
including delayed requests and lost responses. An ambiguous write either landed or not; the lane then adopts exactly
its own content and session, or stops. That is a successful write followed by `stop`, or `stop` alone, so the models
need no separate ambiguous outcome. A rejected write changes nothing and is not a step.

`Chain.lean` justifies the single versioned history of `ExactlyOnce.lean`: a write that applies appends its batch
exactly after the history its writer holds (`append_exact`, `createSucc_exact`).

## Results

| Theorem | Statement |
| --- | --- |
| `Chain.chain` | Every split except the last is sealed: the chain never forks. |
| `Chain.step_hist_prefix` | Canonical history only grows; a committed prefix is never replaced or truncated (contract 3). |
| `Chain.append_exact`, `Chain.createSucc_exact` | A successful write appends its batch at the writer's next offset, so receipts carry exact offsets. |
| `Chain.step_sealed_stable` | A sealed split never changes, except for the version of a cleanup mark. |
| `Chain.sealed_tail_validation` | A lane whose split is sealed holds the whole history while the successor is absent or header-only. `ValidateAsync` therefore checks only the successor of a sealed tail; before the 2026-10-28 fix it also compared the sealed split's version, which cleanup changes. |
| `ExactlyOnce.previous_duplicates` | The rule before the fix could commit one Publish call twice (finding below). |
| `ExactlyOnce.fixed_exactly_once` | With the implemented rule, no transfer is committed twice (contract 7). |
| `ExactlyOnce.fixed_call_order` | With the implemented rule, each session's committed sequences are exactly `1, 2, …, k` in history order (contract 8). |
| `ExactlyOnce.fixed_receipts` | With the implemented rule, every receipt names the record at its offset. |
| `ExactlyOnce.fixed_commits` | The implemented rule still publishes, so the results above are not vacuous. |
| `Merge.coverage` | Every split stays readable from an existing split, level-1 or level-2 object, whatever deletions concurrent mergers make. |
| `Merge.latest_split_exists` | The latest split, where tail discovery starts, is never deleted. |
| `Merge.old_listing_ends_early` | A split after the last one of an old listing can be deleted while history continues. Before the 2026-10-28 fix, a reader walking past an old listing took such a missing split for the end of history. |
| `Merge.fresh_listing_end` | In a listing taken now, the last split is the latest one, so a split missing after it is past the end of history. |
| `Merge.torn_listing_ends_early` | A listing returns splits before merged objects, and its later page may return a merged object made after it returned the splits. That object covers every listed split while history continues. Before the 2026-10-28 fix, the reader then reported the end of history. |
| `Merge.merged_has_successor` | After any merged object that ever existed, history continues with a split readable from an existing object, now and later. The reader therefore walks on from the split after a merged object. |
| `Bounds.current_bounds_stale` | Under the rule before the 2026-10-28 fix, a write dispatched before a bounds call lands, another owner commits, and the late response makes the call answer an end below a completed receipt. |
| `Lease.lease_safe` | While the local clock is below request dispatch plus `UsableDuration`, the provider-guaranteed lease still holds. |
| `Lease.grant_within_lease` | A grant issued while `CanGrant` held covers only real times at which the lease holds, however the follower's and leader's clocks drift within the bound. |
| `Lease.drained_grants_expired` | Once the leader's clock reaches the drain bound of every grant it issued, no follower holds any of them, so the lease can move. |
| `Lineage.walk_links`, `Lineage.successors_correct` | When the walk succeeds it returns the files after the current one through the end in ascending order, each following its predecessor, and it always terminates. |
| `LateCreate.deleted_has_later`, `LateCreate.fixed_not_reused` | Every deleted split name has a later split present, so a create after which no later split exists reused no deleted name: the check `EventLogOwnerLane` makes after creating a split. |
| `LateCreate.previous_reuses` | Before the 2026-10-28 fix, a late create of a split that merging had deleted landed on the free name, and its owner acknowledged a batch no reader sees (`EventLogLateCreateTest`). |
| `StopSettles.fixed_settles` | Once an owner lane is stopped and no write is in flight, every accepted record has its outcome, including a batch whose write failed after a stop from outside. |
| `StopSettles.previous_hangs` | Before the 2026-10-28 fix, a stop during a write that then failed left the batch's publications pending forever (`EventLogOwnerLaneTest.StopDuringAFailingWriteStillFailsItsBatch`). |
| `DisposeAttach.fixed_detached` | Once disposal detached a topic's lane, the topic holds no lane, whatever takeovers attach later. |
| `DisposeAttach.previous_keeps_lane` | Before the 2026-10-28 fix, a takeover attaching after disposal left the disposed service owning the topic (`EventLogServiceTest.OwnerCandidateCreatedDuringDisposalIsStopped`). |
| `Linger.single_writer_never_lingers` | A single writer awaiting each commit never has an arrival binned, so no window passes the gain test and the commit window stays zero: its latency does not grow. |
| `Linger.answering_publishers_fill_bins` | A second record after a commit, as publishers answering commits send, is binned, which is what lets a window pay off for them. |
| `Attempts.crash_skipped` | An event that crashes every run reaching it is skipped by the `Attempts + 1`-th such run at the latest, whatever offsets below it the runs restart from, and no earlier offset is skipped on the way. |
| `Attempts.skip_needs_attempts` | A skip at an offset needs at least `Attempts` earlier starts of it. |
| `Bounds.fixed_bounds_fresh` | With the implemented rule, which trusts only a write dispatched after the call started or a tail check, every answer covers every receipt completed before the call. |

## Finding: a delayed first dispatch was committed again (fixed 2026-10-27)

Before the fix, `EventLogOwnerLane.SubmitAsync` created the state of an unknown session as
`Committed = oldestUnresolved - 1` whenever `previouslyDispatched` was false, without looking at history. The flag
describes the round when the publisher prepared it. By the time the submission arrives, its retry may already be
committed.

1. Node C sends sequence 1 of a new session to node B (`previouslyDispatched = false`). B is slow to process the
   request, for example paused or blocked while it resolves ownership.
2. C's request times out. C retries sequence 1 (`previouslyDispatched = true`) at the current owner A, which scans
   history, commits it at offset 0 and returns the receipt.
3. B takes over the topic, because it suspects A or the tail still names B's endpoint. Its candidate reads the
   tail, which already contains offset 0.
4. B now processes the delayed request from step 1. Its new lane does not know the session, trusts
   `previouslyDispatched = false`, accepts sequence 1 and commits it at offset 1. B's write holds the current version,
   so storage accepts it.

The one Publish call was then durable twice. `previous_duplicates` reaches this in the model, and
`EventLogOwnerLaneTest.DelayedFirstDispatchAtANewOwnerReturnsTheOffsetOfItsCommittedRetry` reproduced it on the old
code. No fencing prevented it, because B's state was wrong about B's own snapshot, not stale with respect to a newer
writer, so safety depended on delivery timing.

**Implemented fix (`Rule.fixed`).** A lane that does not know a session derives its state from history
(`ResolveSessionAsync`): the highest sequence of the session from `oldestUnresolved` on, in frames from `resumeFrom` up
to the lane's next offset, and at least `oldestUnresolved - 1`. The proof needs neither `previouslyDispatched` nor
the preceding `InstallAsync`, because a foreign write after the lane's snapshot fences the lane before it can commit.
The code therefore installs first only for a previously dispatched round, as before. When `resumeFrom` is at or
beyond the lane's next offset, the range is empty and the state is `oldestUnresolved - 1` without I/O. That is the
usual first submission, so it keeps the combined takeover write
(`NewSessionAtTheNextOffsetTakesOverWithItsFirstCommitWithoutReading`). Only a new session that arrives after records
were committed since the publisher's `_knownNext` pays for a read.

The proof also covers rounds whose `PrepareRound` calls run out of sequence order. Pipelined rounds can do that,
because `PrepareRound` runs after the asynchronous owner resolution.

## Not covered

- Byte-level formats, checksums, frame and seal encoding, and the merged-object index (`EventLogFormatTest` covers
  them).
- Deletion deadlines and readers in the middle of a pass over an object that is deleted; readers locating offsets
  through merged objects.
- `ValidateAsync`, heartbeats, bounds freshness, subscriptions and the HTTP transport.
- Liveness: bounded retries, gap timeouts, backoff, and whether a publisher eventually gets its receipt.
- A refinement proof from the C# code to the models. The mapping above is by inspection.
