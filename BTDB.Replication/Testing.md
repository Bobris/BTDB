# BTDB.Replication testing

This file describes how replication is tested, what the important test groups prove and what is not covered yet.
Protocol rules and open proof items live in [Architecture.md](Architecture.md); core BTDB contracts are in
[ReplicationCore.md](../Doc/ReplicationCore.md), and the clock/lease model behind the authority tests is in
[M1Evidence.md](M1Evidence.md). Tests are evidence for the named scenarios; they do not by themselves close any proof
blocker tracked in Architecture.md.

## Running the suites

```sh
dotnet test BTDB.Replication.Test/BTDB.Replication.Test.csproj
dotnet test BTDB.Replication.Http.Test/BTDB.Replication.Http.Test.csproj
dotnet test BTDB.Replication.Azure.Test/BTDB.Replication.Azure.Test.csproj    # needs Azurite
dotnet test BTDB.Replication.Process.Test/BTDB.Replication.Process.Test.csproj  # needs Azurite
dotnet test BTDBTest/BTDBTest.csproj --filter 'FullyQualifiedName~ReplicationPreparationTest|FullyQualifiedName~TransactionLogCaptureTest|FullyQualifiedName~TransactionBatchingTest|FullyQualifiedName~ReplicationCompactorTest|FullyQualifiedName~KeyIndexSnapshotTest'
```

Install Azurite 3.35.0 (`npm install --global azurite@3.35.0`, the version CI installs) so `azurite-blob` is on
`PATH`, or set `BTDB_AZURITE_EXECUTABLE`. The fixtures start their own loopback instance with temporary storage and
development credentials; they never read cloud credentials or create Azure resources.

## Deterministic simulation harness

`BTDB.Replication.Test/Simulation` provides the deterministic primitives used by the in-process tests:

- `DeterministicScheduler` queues every callback explicitly. `RunNext` runs one callback, `AdvanceBy` advances virtual
  monotonic time and runs due callbacks, and `RunUntilIdle` drains runnable work within a step budget. Equal deadlines
  run in insertion order; paused scopes stay pending; stopped scopes cancel their callbacks. `SeededRandom` is a fixed
  SplitMix64, so the same seed gives the same IDs, backoff and schedule (`SchedulerTest`).
- A callback or invariant failure reports the seed and execution trace and, by default, writes it under
  `simulation-failures` beside the test assembly (inside the ignored `artifacts` tree). Re-run the named test with the
  recorded seed and inputs; the trace is diagnostic, not a replayable program.
- `ClusterFixture(seed)` gives each node separate native BTDB databases, in-memory file collections, allocators,
  application input and scheduler scopes, with background compaction disabled. A seeded native TRL header avoids random
  database identities, so same-seed runs also match native file hashes. After every step it compares each node's
  published root with an independent dictionary replay of application inputs, without opening a reader or retaining a
  root (`ClusterIsolationTest`).
- `SimulatedBlobStore` separates dispatch, remote effect and response, so delay, lost responses and timeouts before
  the effect are independent faults; CAS versions are never reused and buffers are copied at the boundary
  (`StorageFaultTest`). `SimulatedLeases` models finite per-object leases (`LeaseServiceTest`), and `SimulatedPeerLink`
  models opaque-byte queues, delay, partitions and connection replacement.
- `HistoryOracle` reconstructs selected authority, append-only history, cursors and reachable ranges from the observed
  storage journal. `HistoryOracleTest` shows it rejects rewritten history, deletion of reachable files and unselected
  publishers, and that selection alone does not fence an already dispatched predecessor write. Its `ModelAuthority` /
  `ModelHistory` inputs are synthetic fixtures, not native TRL or production leader metadata.

The component and coordinator tests below reuse `DeterministicScheduler` with native BTDB databases, in-memory
conditional storage fakes and `InProcessReplicationPeerTransport`. They do not run through `HistoryOracle`.

## Core BTDB support (`BTDBTest`)

- `TransactionLogCaptureTest`: completed positions advance only at complete commit/rollback, capture preserves native
  bytes across rotations, async open initializes capture from existing TRLs, and compaction keeps unacknowledged TRLs.
- `WriterCancellationTest`: cancelling a queued writer does not leak its reservation.
- `ReplicationPreparationTest`: the event cursor follows writer commit/rollback, a legacy even tail rotates to odd TRLs,
  file-ID parity allocation, startup metadata before application writes and rollback actions.
- `TransactionBatchingTest`: virtual batches keep ordinary per-event commits, and readers do not publish a pending batch
  (`ReadersDoNotPublishPendingBatch`).
- `ReplicationCompactorTest` and `KeyIndexSnapshotTest`: replicated compaction creates no local KVI, keeps files needed
  by readers and export snapshots, and cancelling a remote export leaves local compaction running.
- `NativeFileRestoreTest`: restored native file IDs stay physical IDs through KVI and TRL.
- `ObjectDbInitializeRelationsTest`: relation schemas are checked read-only and persisted in at most one startup writer,
  including empty relations, index upgrades and rollback on failure.
- `ObjectDbCompactorLeakCleanupTest`: leak candidates apply identically and idempotently in two databases, including
  rollback, and replicated compaction never erases leaks on its own
  (`ReplicatedCompactorDoesNotIndependentlyEraseDetectedLeaks`).

## Canonical TRL lane necessity and limits

`CanonicalTrlPublisherTest` publishes real native commits and rollbacks through the in-memory conditional store and
restores them with ordinary `OpenAsync`. It covers same-file and cross-file transactions, multi-file genesis selecting
its root only after successors, interruption before every cross-file effect, coalescing several transactions, schema
commits with an unchanged event cursor, adoption racing an old-term append in both orders, authority loss with prepared
successors, and stopped publication leaving local writes, rollback and compaction running.

The lane keeps one unresolved conditional intent because a delayed-effect test shows an old read cannot prove failure.
Exact retries reuse the same token and source cut, so a late original request cannot append twice
(`ExactRetryCannotDuplicateBytesWhenOriginalRequestLandsLate`). Ambiguous replies are reconciled by comparing only the
appended bytes in bounded chunks with two 64 KiB buffers (`AmbiguousAppendReconcilesOnlyTheAppendedBytes`,
`ReconciliationReadsLargeNativePrefixesInBoundedChunks`); any different byte is a conflict. The target position is
acknowledged only after selection. `NativePublicationTest`, `TrlMetadataTest` and `ReplicationEndMarkerTest` add
native multi-file reopen, TRL metadata validation, and that replication writes no temporary or rotation end markers.

## Discovery, restore and local cache

- `CanonicalTrlPublisherTest` restore cases: discovered history resumes publication in a new term; broken selected
  links, a missing published genesis and changed object versions fail instead of restoring wrong or empty state; an
  interrupted restore retries without trusting partial cache.
- `ReplicationFileSetTest`: exact remote IDs, cache reuse only after extension, length and SHA-256 checks, separate
  local/remote inventories, shared and bounded prefetch, collision safety, removal of partial downloads,
  `FailedInitializationPublishesNoPartialInventory`, and
  `RetiredPublicationGetsAFreshIdentityInsteadOfRecreatingItsKey`. Downloads keep four 4 MiB range reads in flight and
  write blocks in order (`DownloadUsesBoundedParallelBlocksAndPreservesOrder`); sealed files with a checksum are
  verified while written (`DownloadVerifiesSealedChecksumBeforeEstablishingPlacement`), and one failed block cancels
  the others (`FailedParallelBlockCancelsOtherReadsAndRemovesPartialFile`).
- `AsyncOpenTest`: discovery reads only needed KVI/TRL headers and prefetches KVI references in parallel. A restored
  complete canonical tail continues the leader's native file at its exact committed EOF, while partial, corrupt or
  sealed tails rotate (`RestoredCompleteCanonicalTailContinuesTheLiveLeadersNativeFile`,
  `ReplicatedOpenDoesNotAppendBeyondAnUncleanOrSealedTail`); this defect was found by the process tests.
- `RestartRecoveryTest`: with plain or compressed KVI and empty or corrupt cache, a fresh process state restores a
  checkpoint after obsolete TRLs (including genesis) are deleted, adopts a new term, publishes, and restores again.
  Replacement checkpoints or tail appends landing after discovery make the stale attempt fail; a repeated
  initialization/open selects the new state without mixing versions.

## Checkpoints and remote maintenance

`CheckpointPublisherTest` streams native KVI with whole-PVL remote ID substitutions and unchanged TRL. KVI upload
starts only after the canonical TRL cut and required PVLs are published; pending, cancelled or conflicting canonical
state and authority loss stop it before the first KVI chunk. Lost KVI/PVL replies retain their identity, conflicting or
missing SHA fences the session, restored PVLs are reused without upload, and a promoted follower reuploads a missing
restored PVL at a fresh ID.

`RemoteMaintenanceTest` covers delayed deletion that preserves the recovery closure and never reuses the highest ID,
version protection defeating a delayed delete, protection only of new or marked PVLs, failed KVI never authorizing
deletes, restore of an actual compacted checkpoint after cleanup, and the maintenance watchdog deadline.
`CheckpointRequestTest` (Azurite) bounds a steady-state checkpoint to three listings and no request per retained
file. Deletion delay must be positive; these tests use small
positive delays in virtual time.

## Follower comparison

- `TrlPrefixComparerTest` compares independent native databases byte-for-byte over the leader's local files: matching
  history with different batching, legacy even-to-odd rotation, lag, fixed cuts, unchanged event IDs, sticky
  divergence, bounded reads and cancellation. Only a full match advances the matched cut; the comparer never
  acknowledges capture itself.
- `LeaderTrlReaderTest` and `RetainingLeaderTrlReaderTest`: leader reads stop at the complete cut, inline bytes follow
  lineage across files, retention is bounded, and an inline file switch answers the end-of-file check without a peer
  read (`InlineFileSwitchAnswersEndOfFileAndContinuesIntoTheSuccessor`).
- `FollowerComparisonSessionTest`: explicit per-poll cuts and lag retries, fixed cuts during reads, cancellation on
  close, restart requested once on divergence,
  and a new leader compared from the verified restore cut
  (`NewLeaderCannotConfirmBytesOnlyAcknowledgedByItsPredecessor`).
- `ReplicationPeerPollTest` validates poll answers and inline byte budgets. `TransactionLogCaptureTest` (replication
  project) shows `TransactionLogCapture.NonApplicationCommitted` tracks schema commits live and after replay, which the
  leader announces instead of followers scanning its TRL.

## Authority, selection and activation

- `AuthorityTest` and `LeaseServiceTest`: delayed or ambiguous lease responses and process pauses cannot extend
  authority, and one drain deadline waits out every grant.
- `LeaseSessionControllerTest` and `LeaseMaintenanceTest`: renewal before the deadline, late responses cannot revive a
  session, permanent disqualification, and a failed renewal retries before expiry even with a long retry interval
  (`FailedRenewalRetriesBeforeTheLeaseExpiresEvenWithALongRetryInterval`).
- `LeaderSelectionTest`: exact JSON reconciliation after a lost reply, preserved application data, the generation
  floor, malformed records, and `SameGenerationComparesDatabaseNamesAsASet`. `ReplicationApplicationDataTest` covers
  conditional `applicationData` writes, reconciliation and follower/fenced read-only access.
- `LeadershipActivationTest`: Blob validation despite peer acknowledgement, divergence never adopting, all databases
  adopting before any publisher is returned, and append racing adoption. `ReplicationProgressWatchdogTest` checks that
  stale timeouts cannot override progress.

## Coordinator simulation

`ReplicationNodeCoordinatorTest` runs up to three real BTDB nodes with the actual lease, selection, activation,
publisher, comparison and maintenance components under virtual time. Applications execute transactions directly; the
coordinator never runs handlers.

- Failover: `ThreeNodesRestoreFollowAndFailOverAutomaticallyWithDelayedOldPublication` publishes the optimistic tail
  once and a delayed old-term CAS cannot overwrite it; lagging followers catch up and can take over without restart.
- Isolation of work: renewal is independent of blocked publication and late peer replies cannot acknowledge; slow
  canonical writes and checkpoints do not stall publication; local compaction continues on leaders and followers.
- Comparison traffic: one poll per step, each byte received once under load, and
  `CaughtUpFollowerComparesInlinePollBytesWithoutRangeReads` (with and without small logs) needs no range reads.
- Faults: startup outage prevents election, divergence requests restart while local writes continue, malformed leader
  records request restart, and wrong API keys or stale sessions fail authentication.
- Activation conflict: `DelayedPredecessorGenesisConflictsWithActivationAndRestartsWithoutAWatchdog` lands the old
  genesis between empty-root discovery and the new leader's CAS. The new leader fences and restarts without renewing
  the lease or requiring a watchdog; a fresh node restores the winning initialization cursor and can lead.
- Upgrades and database sets: prepared highest-generation handoff before lease expiry, older generations following but
  never contending, removed databases continuing locally with their TRL released to compaction, and leader-only
  initialization that keeps its captured cursor after a lost reply or is recreated at a newer input end after leader
  loss; `LeaderThatInitializedDatabaseFollowsAfterLosingLeaseWithoutRestart` follows the next leader.
- Schema: `CoalescedSchemaDetachesLaggingFollowerAndNeverPromotesItsLocalWork` detaches a lagging follower before
  comparison, never reattaches it, releases its TRL to compaction and requests restart after 15 minutes without
  leader evidence;
  `FollowerRestoredAfterPublishedSchemaTreatsItAsDuplicate` keeps a follower restored past the schema commit following.
- Watchdogs: publication and checkpoint deadlines fence authority despite healthy renewals and delay fatal restart
  while a healthy follower takes over; idle leaders, activation retries and shutdown do not trigger false recovery.

## HTTP adapter and hosting

`BTDB.Replication.Http.Test` uses real loopback Kestrel listeners. `HttpReplicationPeerTransportTest` covers
per-request authentication of the exact leader identity (including key rotation and replaced sessions), cancellation,
rejection of missing, wrong and rotated-out Bearer keys without reading the body, key rotation during body reads,
missing retained bytes, bounded and oversized requests, immediate overload rejection, redirect rejection, invalid range
responses and native comparison across rotated TRLs over the socket. `ReplicationPeerWireTest` round-trips the wire
codec. `ReplicationHostingTest` runs the coordinator inside an ASP.NET host: start only after Kestrel, restore retry
without contention, cancellation during restore, shutdown fencing an uncooperative renewal, restart-required and fatal
failures stopping the host, readiness metrics and configuration validation. See the
[adapter instructions](../BTDB.Replication.Http/README.md).

## Azure adapter (Azurite)

`BTDB.Replication.Azure.Test` uses Azure.Storage.Blobs against Azurite. It covers lease acquisition with lost-response
reconciliation, finite lease expiry, prepared lease transfer confirmed by the target's renewal, selection requiring
lease plus ETag, atomic canonical append/adoption and version-bound reads, end-to-end acquire/select/adopt/publish/KVI
restore, immutable PVL SHA fencing, and restore after obsolete genesis TRLs are deleted via the checkpoint root.
Block Blob and cleanup specifics: `RepeatedAppendsReuseTheirOwnCommittedBlockList` (no block-list reads between
own appends), `SmallTrlAppendsStageOnlyTheirSuffixAndMergeTrailingBlocksOccasionally`,
`CheckpointClearsDeletionMarksWithoutChangingUnmarkedTrlVersions`,
`DeletionDeadlineSurvivesNewAdapterAndDoesNotMoveOnRetry`, throttling surfacing as retryable `IOException`, and
`EmptyPrefixIsRejectedBecauseCleanupListsTheWholeNamespace`.

## Process failover

[`BTDB.Replication.Process.Test`](../BTDB.Replication.Process.Test/README.md) launches independent .NET/Kestrel node
processes that use only the public hosting/provider API with real Azure adapters against Azurite:

- `UnavailableLeaderIsReplacedAndItsUnpublishedTailSurvivesWithoutReexecution` kills or (except on Windows) suspends
  the leader without releasing its lease; after real lease expiry the follower publishes the compared unpublished tail
  without rerunning the handler; a resumed suspended leader follows without a restore, and a cold third process
  restores it and continues comparison.
- `StalledPublicationTerminatesTheLeaderAndFollowerRecoversItsOptimisticTail`,
  `DivergentFollowerTerminatesItsProcessWithoutPublishingItsLocalOutcome` and
  `CrashedFollowerRestartsFromItsOwnDiskStorage` cover the publication watchdog, divergence exit and restart from
  existing local files after a kill.
- `PublicHostingApiTest` guards the external-consumer boundary: no friend-assembly access, no public way to create or
  renew authority, and no secrets in public record strings. See [hosting contracts](../Doc/ReplicationHosting.md).

## Recorded measurements

- Parallel download against Azurite: four 128 MB files took 1.40 s with 256 KiB ranges and 1.00 s with 4 MiB ranges,
  which is why downloads use 4 MiB ranges (peak 16 MiB of buffers per file). Real Blob latency was not measured.
- Cache validation at startup: 1 GB of cached files on local disk took 2.7 s hashed serially and 1.1 s with four
  concurrent validations.
- Azure PVL upload: a 128 MB PVL took 595 ms with serial block staging and about 330 ms with four concurrent stages
  (Azurite).
- The removed follower-side schema scanner needed about 5.8 s for 23 MB of 20-byte transactions; leader-announced
  schema positions replaced it.
- The opt-in [live Azure probe](../BTDB.Replication.Test/Integration/azure_probe.py) is not run automatically; its
  recorded run is described in [ObjectStorages.md](ObjectStorages.md).

## Not covered yet

- Live Azure behavior: latency, throttling, failover timing and throughput are only exercised against Azurite (plus
  the one-off probe). The 15-minute startup target for about 100 GB has not been measured.
- Physical disk faults: no torn-write or power-loss model of a disk file collection; process tests only kill or suspend
  processes.
- Production clock qualification: in-process tests use virtual time and process tests a Stopwatch-based scheduler.
- Exhaustive interleavings: schedules are seeded and hand-chosen, not model-checked, and `HistoryOracle` is not
  attached to coordinator or adapter runs.
- Concurrent remote publication/deletion races beyond the listed restart cases, and remote orphan selection.
- Production TLS/proxy deployment of the HTTP adapter.
- Multi-process partitions, rolling-upgrade handoff across processes, and workload/restore performance.
