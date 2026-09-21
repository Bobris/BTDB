# Replication test foundation

M0 and the bounded M1 authority/TRL model are implemented in `BTDB.Replication.Test`.
See [M1Evidence.md](M1Evidence.md) for mechanism necessity, clock assumptions and the integration preconditions.
M2 now adds core capture/cancellation tests in `BTDBTest`: [capture](../BTDBTest/TransactionLogCaptureTest.cs),
[writer cancellation](../BTDBTest/WriterCancellationTest.cs), and ObjectDB metadata
retry coverage in ReplicationPreparationTest. The M3 checkpoint slice adds
[CheckpointPublisherTest](../BTDB.Replication.Test/CheckpointPublisherTest.cs) and
[KeyIndexSnapshotTest](../BTDBTest/KeyIndexSnapshotTest.cs): actual native KVI restore with whole-PVL ID substitutions,
unchanged TRL, plain/Brotli chunked output with no local staging file, prerequisite failure, uncertain PVL retry,
receipt reuse across compactions, independent cancellation, and source pins. [CanonicalTrlPublisherTest](../BTDB.Replication.Test/CanonicalTrlPublisherTest.cs) adds capture-backed native publication:
same-file/cross-file commit and rollback, genesis selection, interruption before every cross-file effect, lost/cancelled
replies, exact retry, unchanged-event schema commits, large-prefix bounded reads, and both tail-adoption race orders.
These hooks are not a distributed replication runtime.
No B1/B3/B5/B6 proof blocker is closed by these tests. The normative scenario requirements remain in
[Architecture.md](Architecture.md#testing-strategy-implied-by-the-design).

## Running and reproducing a scenario

```sh
dotnet test BTDB.SourceGenerator.Test/BTDB.SourceGenerator.Tests.csproj
dotnet test BTDB.Replication.Test/BTDB.Replication.Test.csproj
dotnet test BTDBTest/BTDBTest.csproj --filter 'FullyQualifiedName~ReplicationPreparationTest|FullyQualifiedName~TransactionLogSizeStrategyTest|FullyQualifiedName~TransactionBatchingTest'
```

`ClusterFixture(seed)` constructs separate native BTDB databases, memory file collections, allocators, application
input histories, identities and scheduler scopes for each node. A seeded existing empty native TRL header supplies
the database GUID, avoiding native random identity generation; same-seed tests compare native file hashes as well as
schedules. Background compaction is disabled. The application
fixture supplies input and drives transactions; the replication assembly does not run handlers. Node disposal cancels
its callbacks and releases its native resources, but cannot undo a request already dispatched to the storage scope.
This simulates a lost process/session; it is not yet a torn-write or power-loss model of a disk file collection.

All callbacks are explicitly queued. `RunNext` executes one callback; `AdvanceBy` advances monotonic time and executes
due callbacks; `RunUntilIdle` drains runnable work with a step budget. Paused scopes remain pending and do not make the
drain wait forever. Equal-deadline callbacks use insertion order. A fixed SplitMix64 implementation supplies seeded
IDs and backoff. Tests vary delays to explore interleavings; this is not yet an exhaustive model checker.

On a callback or invariant failure, the exception includes the seed and execution trace. By default the same trace is
written under `simulation-failures` beside the test assembly, inside the repository's ignored `artifacts` tree.
Filenames use a trace hash. Expected-failure tests inject a sink instead. Re-run the named test with its recorded seed
and scenario inputs; the trace records scheduling, execution, virtual timestamps, pauses, cancellation and remote effects.
The trace is diagnostic evidence, not a serialized program that can replay arbitrary callbacks by itself.

## Observation without changing the tested state

After every scheduled callback and explicit time advance, the cluster checks the observable storage journal and each
node's published native root. Local expected values come from independent dictionary replay of application inputs.
The observer briefly references the root and resolves actual values, including file-backed values, then releases it.
It retains no root between steps. Ordinary readers also leave pending virtual batches unpublished, as covered by
`TransactionBatchingTest.ReadersDoNotPublishPendingBatch`. The application fixture models the existing rollback behavior that publishes the committed batch prefix.

`SimulatedBlobStore` separates dispatch, remote effect and response. CAS uses opaque model versions which are never
reused. Delay, response loss and a timeout before the effect are independent controls. An old read after timeout does
not cancel the request. Request/read buffers and journal observations are copied at the boundary. Conditional create,
replace, append and delete include atomic metadata. `SimulatedLeases` separately models finite per-object leases;
its supported subset and real-provider limits are documented in M1Evidence.md.

`HistoryOracle` independently reconstructs selected authority, append-only transaction history, sequence/cursors and
reachable hashed ranges from the observed dispatch/effect journal. Negative tests demonstrate that it detects invalid
observations. It accepts the possibility of an old-authority request landing after a new leader is selected; selection
alone does not fence data blobs. Separate `PublicationTest`, `AuthorityTest` and `LeaseServiceTest` now exercise
per-TRL adoption and finite lease/grant bounds. They are not yet integrated into the cluster oracle.

`ModelAuthority`, `ModelHistory` and their JSON bytes are synthetic oracle fixtures only. They are not native TRL,
production leader metadata, a checkpoint manifest, a persistent transaction-kind sidecar or a proposed second state
commit. M1/M2 must connect the oracle to independently decoded real protocol/native storage evidence. Today a model
history test does not prove native recovery, structural comparison or distributed publication safety.

`SimulatedPeerLink` exercises opaque-byte queue pressure, delay, partitions and connection replacement. It deliberately
is not a production `PeerTransport` adapter. M1/M4 must add the shared wire codec, authentication, resume and validation
before either this path or HTTP can claim transport conformance.

## Invariant coverage

Status describes partial evidence for the listed aspect, never completion of the entire invariant.

| Invariant | Current evidence | Remaining implementation |
| --- | --- | --- |
| I1 Authority | Model oracle, lease/grant deadline inequalities, pause/delayed-response tests and bounded grant drain. | Production clock qualification and serialized role/commit fencing: M1/M4. |
| I2 Ordered history | Model sequence/cursor checks; real BTDB values, cursors and rollback prefix checked per step. | Completed/acknowledged native positions implemented; distributed byte comparison remains M4. |
| I3 Confirmation | Challenge expiry, stale response and closed-session rejection helpers. | Structural comparator, confirmation and mismatch restart: M4. |
| I4 Local execution | Real node transactions run independently with no storage work; non-perturbing batch observation. | Completed positions, compactor retention and writer cancellation tested; canonical publisher tested; coordinator/comparator integration remains M4. |
| I5 Durable closure | Model rejects missing/corrupt ranges, partial transactions and prefix changes. | Native publication, KVI restart and TRL-only restore tested; concurrent recovery races and adapters remain M3/M7. |
| I6 Publication fence | Append/adoption races, lost success reconciliation and competing genesis. | Serialized canonical lane tested; coordinator and production adapter integration remain M3/M4/M7. |
| I7 Database set | Independent local databases only. | Generation, activation and upgrades: M5. |
| I8 Recovery | Real native multifile commit/rollback replay through selected links; incomplete local KVI-copy fixture. | Native restore/cache validation tested; concurrent recovery races and optimistic adoption remain M3/M4. |
| I9 Reader lifetime | Real old reader survives batch commit/rollback; observer releases roots per step. | Capture boundary protects TRLs during compaction; NativeFileRestoreTest covers unchanged physical IDs; compaction integration remains M6. |
| I10 Maintenance | ReplicationCompactorTest, KeyIndexSnapshotTest and CheckpointPublisherTest cover no-local-KVI compaction, pins, remapped export, receipts and cancellation. | Maintenance scheduling, remote GC and leak-event integration: M6. |
| I11 Volatile execution | Publisher cancellation with continued local execution. | Distributed detachment and drain remain M5. |
| I12 Isolation | Three native nodes, separate scopes/files/allocators, reproducible schedules and bounded queues. | Complete runtime/codec isolation and adapter conformance: M4/M7. |

## Scenario-family register

Every architecture family is mapped below. Existing test class names are linked to source; other group names are
planned and do not imply files exist. No scenario currently has production-adapter qualification.

| Architecture family | Test group | Status / next milestone |
| --- | --- | --- |
| Authority | [AuthorityTest](../BTDB.Replication.Test/AuthorityTest.cs), [LeaseServiceTest](../BTDB.Replication.Test/LeaseServiceTest.cs) | Bounded M1 model; production clock/role integration M4. |
| Publisher | [PublicationTest](../BTDB.Replication.Test/PublicationTest.cs), [NativePublicationTest](../BTDB.Replication.Test/NativePublicationTest.cs) | TRL CAS races and native chain replay; CanonicalTrlPublisherTest covers the actual lane. Coordinator/adapters remain M3/M4/M7. |
| Durable boundaries | HistoryOracleTest, [TransactionLogCaptureTest](../BTDBTest/TransactionLogCaptureTest.cs) | Completed/acknowledged positions, retention and native publication tested; coordinator integration remains. |
| Legacy TRL parity | [ReplicationPreparationTest](../BTDBTest/ReplicationPreparationTest.cs), LegacyRestoreTest | Existing native parity tests; distributed interruption cases M3. |
| Checkpoints | [CheckpointPublisherTest](../BTDB.Replication.Test/CheckpointPublisherTest.cs), CanonicalTrlPublisherTest | Real canonical fixed-cut barrier before PVL/KVI publication; pending/cancelled/rejected CAS, authority loss and empty-cache native restore covered. Production KVI ambiguity reconciliation and restore retry remain M3/M7. |
| Transition recovery | TransitionRecoveryTest | Unimplemented, M4. |
| Upgrade | GenerationUpgradeTest | Unimplemented, M5. |
| Retirement | RetirementTest | Unimplemented, M5. |
| Graceful drain | GracefulDrainTest | Unimplemented, M5. |
| Stopped publication | CanonicalTrlPublisherTest | Ordinary local writes, rollback and compaction continue without further Blob changes. |
| Application batches | [ClusterIsolationTest](../BTDB.Replication.Test/ClusterIsolationTest.cs), [TransactionBatchingTest](../BTDBTest/TransactionBatchingTest.cs) | Native local batching/rollback evidence; distributed comparison/coordinator integration M4. |
| Structural comparison/restart | [TrlPrefixComparerTest](../BTDB.Replication.Test/TrlPrefixComparerTest.cs) | Native bounded byte comparison, lag, sticky divergence and retry tested; peer/grant/role and restart orchestration remain M4. |
| Optimistic tail adoption | OptimisticAdoptionTest | Unimplemented, M4. |
| Invalid comparison cache | ComparisonCacheTest | Unimplemented, M4. |
| Failed consumption | ClusterIsolationTest, ApplicationFailureTest | Native rollback prefix only; distributed outcomes M4/M5. |
| Skip replay | SkipReplayTest | Unimplemented, M5. |
| Follower acceptance | FollowerAcceptanceTest | Unimplemented, M4: coalesced three-field progress, native TRL pull, unchanged-eventId schema notification and stale-session rejection. |
| Restart recovery | [RestartRecoveryTest](../BTDB.Replication.Test/RestartRecoveryTest.cs) | Native checkpoint restart after obsolete history removal, empty/corrupt cache, new-term publication and second restart pass; production adapters and concurrent recovery races remain. |
| Speculation resources | SpeculationResourceTest | Unimplemented, M4/M7. |
| Leak events | LeakEventTest | Core bounded detector and idempotent erase exist; candidate-access seam and ordered application-event integration remain M6. |
| Physical compaction | CheckpointPublisherTest, KeyIndexSnapshotTest | Native remapped KVI restore and independent local/remote tokens covered; ReplicationCompactorTest covers no-local-KVI mode; scheduling and production/GC integration remain M6. |
| KVI and local cleanup | [ReplicationCompactorTest](../BTDBTest/ReplicationCompactorTest.cs), [KeyIndexSnapshotTest](../BTDBTest/KeyIndexSnapshotTest.cs), CheckpointPublisherTest | No local KVI, source pins, whole-PVL mapping and KVI-last publication tested; remote GC remains M6. |
| Reader/file lifetime | ClusterIsolationTest, ReaderLifetimeTest | Native old-reader, capture and snapshot retention tested; comparison integration and remote GC remain M4/M6. |
| Startup pipeline | [AsyncOpenTest](../BTDB.Replication.Test/AsyncOpenTest.cs), [ReplicationFileSetTest](../BTDB.Replication.Test/ReplicationFileSetTest.cs) | Lazy discovery, prefetch and bounded shared downloads tested; coordinator retry and throughput qualification remain M3/M7. |
| Cache loss | ReplicationFileSetTest, RestartRecoveryTest | Missing/corrupt cache and interrupted downloads tested; disk/process and concurrent remote-change qualification remain M3/M7. |
| Remote GC | HistoryOracleTest, RemoteGcTest | Model rejects deletion of reachable ranges; actual GC unimplemented, M6. |
| Transport | StorageFaultTest, TransportConformanceTest | Opaque-byte fault fixture only; progress/control codec, authority-bound TRL pull and auth/resume M4/M7; piggyback deferred. |
| Input/read contract | ClusterIsolationTest, InputReadContractTest | Native input cursor and snapshot evidence; replay/retention M3/M5. |
| Startup secondary-index reconciliation | ReplicationPreparationTest, [ObjectDbInitializeRelationsTest](../BTDBTest/ObjectDbInitializeRelationsTest.cs) | Read-only full schema check, empty schema/index upgrades, at most one writer and rollback tested. Coordinator authority/publication timing remains pending. |
| Detached liveness | DetachedLivenessTest | Unimplemented, M5. |
| Schema upgrade | SchemaUpgradeTest | Unimplemented, M5. |
| Same-term control revision | ControlRevisionTest | Unimplemented, M4/M5. |
| Liveness | [SchedulerTest](../BTDB.Replication.Test/SchedulerTest.cs), LivenessTest | Virtual-time/step-budget fixture only; watchdogs M4. |

The next step is M3 production restore/storage integration, recovery-race qualification and coordinator wiring. Ordinary KVI-based restart is covered by RestartRecoveryTest. Checkpoint prerequisite checks already use the canonical TRL lane. Keep interfaces internal until their semantics are
exercised; publication, remote allocation, restore retry and role integration remain M3–M4.
The [live Azure capability probe](../BTDB.Replication.Test/Integration/azure_probe.py) is an explicit opt-in script,
not an automatically run cloud test or a production adapter. Its recorded run and cleanup are linked from ObjectStorages.md.


### Canonical TRL lane necessity and limits

`CanonicalTrlPublisher` retains one unresolved conditional intent because the delayed-effect test demonstrates that an
old read cannot prove failure. Exact retries keep the same token and native source cut, so a late original request
cannot append twice. Successor writes run in reverse dependency order because native restore must never observe a
reachable partial transaction. Each boundary is interrupted in tests; previously selected transactions remain readable.
Metadata-only adoption and append compete on the predecessor token; tests cover either winner without losing history.

The lane consumes the completed-position API. Append payloads reference retained local files;
only exceptional reconciliation allocates two 64KB buffers and compares the exact versioned native prefix. No full TRL
copy, wire hash, transaction envelope or second state CAS is added. Remote cancellation never cancels local execution.
The fixed target position is acknowledged only after selection is confirmed; prepared successors do not advance it.

The storage seam still needs a production conditional Azure adapter. Tests use an independent in-memory conditional
store with delayed effects and native BTDB reopen, not a live Azure publisher. ID/key allocation and verified restored
input are coordinator preconditions. The lane reports conflict instead of inventing a new continuation after another
term wins. Production discovery, abandoned staging cleanup and cross-component coordinator integration remain pending.


### Genesis/TRL discovery and restore

`CanonicalTrlPublisherTest` now restores its publication/race fixtures through `CanonicalTrlInventory`, file-set initialization and
ordinary native `OpenAsync`. `DiscoveredRestoreResumesPublicationInANewTerm` covers native cross-file commit/rollback
recovery followed by adoption and a fresh application commit. Unselected prepared objects never enter the inventory.
`InventoryRejectsBrokenSelectedLinks` covers missing successors, key cycles and decreasing IDs/terms. A missing published genesis fails instead of returning an empty database.

The inventory adapter does not open the database or validate native headers separately. Version-bound reads do not
add a separate guarantee that malformed transaction bytes will be rejected rather than recovered by the existing decoder.
Version-change and interruption tests fail the attempt, then rediscover/restore without any remote mutation. Caller
cancellation may leave a completed shared download; disposal drains transfer work and the next initialization validates
cache again. Downloads remain bounded to 256 KiB reads. These tests cover the in-memory storage seam, not Azure or
power-loss recovery of a disk cache. Ordinary KVI restart is tested separately below; cleanup-race retry remains pending.


### Checkpoint restart without previous process state

`RestartRecoveryTest.RestartFromCheckpointAfterHistoryCleanupResumesPublication` tests the existing collection
initialization and native `OpenAsync` path. Four cases combine plain/Brotli KVI with an empty or corrupt local cache.
The fixture publishes a compacted checkpoint, deletes unneeded TRLs including genesis, publishes a subsequent commit
and rollback, and leaves an unpublished local suffix. It disposes the old database, publisher, captures and local
storage. Only remote object bodies and metadata are copied to a new storage fixture; no authority, receipts, tail
objects, pending requests or callbacks are carried forward.

Restart checks all values and cursors, removes an unselected local file and redownloads same-size corrupted cache
files. The test reads the restored tail's metadata through the existing storage interface, adopts a fresh term,
publishes a new transaction and KVI, then verifies them through a second fresh restart. No production change was
required. This is ordinary restart evidence at the in-memory storage boundary, not qualification of concurrent remote
publication/deletion races, remote orphan selection, Azure or physical process/disk failure.


### Remote changes during native open

`RestartRecoveryTest.CleanupAfterDiscoveryRetriesAgainstNewCheckpoint` schedules replacement checkpoint publication
and deletion after the old inventory has been initialized. Plain/Brotli cases delete either the selected KVI or one
of its required PVLs. The stale attempt fails; disposing it and repeating ordinary initialization/open selects the new
checkpoint, preserves a verified sealed TRL without downloading it again, removes obsolete cache files and restores
all values/cursors without remote writes.

`TailPublicationAfterDiscoveryRetriesWithoutMixingVersions` appends to the selected active TRL after discovery.
Version-bound reads reject the stale attempt; rediscovery/open recovers the new committed event without mixing object
versions. Both tests exercise existing production file-set/native-open code through the in-memory storage seam.
They do not implement automatic coordinator retry or qualify Azure, physical disk/process failure, GC scheduling or
all publication/deletion interleavings. No additional recovery algorithm or core option was needed.


### Inventory allocation and uncertain publication

`RemoteAllocationRefreshesInventoryWithoutReservationObjects` verifies fresh remote discovery, even IDs, session-local
choices and restart without reservation objects. Existing lost-response tests retry the same PVL/KVI identity.
`ConflictingOrMissingShaFencesCheckpointSession` covers mismatching and absent SHA metadata for both file types;
matching SHA confirms the intended content. KVI retries retain the snapshot and mapping and block later snapshots.
These tests exercise native checkpoint publication against the in-memory storage adapter, not a production provider.


### Parallel block download

`DownloadUsesBoundedParallelBlocksAndPreservesOrderWithoutChecksumValidation` holds the first block while three later
blocks complete, exercises short reads and a partial final block, and accepts deliberately invalid checksum metadata.
`FailedParallelBlockCancelsOtherReadsAndRemovesPartialFile` verifies cancellation/draining before cleanup. Downloads
use four 256 KiB buffers per file; existing file-level concurrency still bounds simultaneous files. This verifies
concurrency and byte order, not a measured Azure throughput improvement. Existing cache checksum tests remain.


### Native prefix comparison

`TrlPrefixComparerTest` executes independent native databases with the same bootstrap identity and reads directly from
the leader's local files through `ILeaderTrlReader`; no Blob fixture or upload is involved. It covers cross-file
commits/rollbacks with different batching, legacy even-to-odd rotation, lag, fixed cuts before later local work,
unchanged event IDs, divergence, bounded/short reads, cancellation, missing leader files and stale-session errors.
Only a full match advances capture acknowledgement; reader-visible state is unchanged. No listing or native-header
validation is performed by the comparer. Authenticated transport, grants and host restart remain coordinator work.
Blob history validation is required when becoming leader before adoption/publication, not for routine follower checks;
that takeover coordinator remains pending. Ordinary bootstrap/recovery still uses the existing Blob path.
