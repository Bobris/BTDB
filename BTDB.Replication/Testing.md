# Replication test foundation

M0 and the bounded M1 authority/TRL model are implemented in `BTDB.Replication.Test` (46 test cases).
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
It does not open a normal read transaction: that API can publish a pending virtual batch. It retains no root between
steps. The application fixture models the existing rollback behavior that publishes the committed batch prefix.

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
| I2 Ordered history | Model sequence/cursor checks; real BTDB values, cursors and rollback prefix checked per step. | Core range capture/shared decoding implemented; distributed comparison remains M4. |
| I3 Confirmation | Challenge expiry, stale response and closed-session rejection helpers. | Structural comparator, confirmation and mismatch restart: M4. |
| I4 Local execution | Real node transactions run independently with no storage work; non-perturbing batch observation. | Completed positions, compactor retention and writer cancellation tested; publisher/comparator integration remains M3/M4. |
| I5 Durable closure | Model rejects missing/corrupt ranges, partial transactions and prefix changes. | Native TRL/KVI discovery and publication: M1/M3. |
| I6 Publication fence | Append/adoption races, lost success reconciliation and competing genesis. | Serialized production publisher and adoption integration: M3/M4. |
| I7 Database set | Independent local databases only. | Generation, activation and upgrades: M5. |
| I8 Recovery | Real native multifile commit/rollback replay through selected links; incomplete local KVI-copy fixture. | Native restore/cache validation and optimistic adoption: M3/M4. |
| I9 Reader lifetime | Real old reader survives batch commit/rollback; observer releases roots per step. | Capture boundary protects TRLs during compaction; NativeFileRestoreTest covers unchanged physical IDs; compaction integration remains M6. |
| I10 Maintenance | Unimplemented. | Independent local compaction without KVI, leader-only remapped remote export, no peer results, leak events and cleanup: M6. |
| I11 Volatile execution | Publisher cancellation with continued local execution. | Distributed detachment and drain remain M5. |
| I12 Isolation | Three native nodes, separate scopes/files/allocators, reproducible schedules and bounded queues. | Complete runtime/codec isolation and adapter conformance: M4/M7. |

## Scenario-family register

Every architecture family is mapped below. Existing test class names are linked to source; other group names are
planned and do not imply files exist. No scenario currently has production-adapter qualification.

| Architecture family | Test group | Status / next milestone |
| --- | --- | --- |
| Authority | [AuthorityTest](../BTDB.Replication.Test/AuthorityTest.cs), [LeaseServiceTest](../BTDB.Replication.Test/LeaseServiceTest.cs) | Bounded M1 model; production clock/role integration M4. |
| Publisher | [PublicationTest](../BTDB.Replication.Test/PublicationTest.cs), [NativePublicationTest](../BTDB.Replication.Test/NativePublicationTest.cs) | TRL CAS races and native chain replay; publisher runtime M3. |
| Durable boundaries | HistoryOracleTest, TransactionCaptureTest | Core captured commit/rollback ranges and native decoder tested; publisher integration M3. |
| Legacy TRL parity | [ReplicationPreparationTest](../BTDBTest/ReplicationPreparationTest.cs), LegacyRestoreTest | Existing native parity tests; distributed interruption cases M3. |
| Checkpoints | NativePublicationTest, CheckpointPublicationTest | Incomplete local KVI-copy fixture only; dependency-first publication and restore retry in M3. |
| Transition recovery | TransitionRecoveryTest | Unimplemented, M4. |
| Upgrade | GenerationUpgradeTest | Unimplemented, M5. |
| Retirement | RetirementTest | Unimplemented, M5. |
| Graceful drain | GracefulDrainTest | Unimplemented, M5. |
| Stopped publication | CanonicalTrlPublisherTest | Ordinary local writes, rollback and compaction continue without further Blob changes. |
| Application batches | [ClusterIsolationTest](../BTDB.Replication.Test/ClusterIsolationTest.cs), [TransactionBatchingTest](../BTDBTest/TransactionBatchingTest.cs) | Native local batching/rollback evidence; distributed capture M2. |
| Structural comparison/restart | StructuralComparisonTest | Unimplemented, M4. |
| Optimistic tail adoption | OptimisticAdoptionTest | Unimplemented, M4. |
| Invalid comparison cache | ComparisonCacheTest | Unimplemented, M4. |
| Failed consumption | ClusterIsolationTest, ApplicationFailureTest | Native rollback prefix only; distributed outcomes M4/M5. |
| Skip replay | SkipReplayTest | Unimplemented, M5. |
| Follower acceptance | FollowerAcceptanceTest | Unimplemented, M4: coalesced three-field progress, native TRL pull, unchanged-eventId schema notification and stale-session rejection. |
| Restart recovery | RestartRecoveryTest | Unimplemented, M3/M4. |
| Speculation resources | SpeculationResourceTest | Unimplemented, M4/M7. |
| Leak events | LeakEventTest | Unimplemented, M6. |
| Physical compaction | CheckpointPublisherTest, KeyIndexSnapshotTest | Native remapped KVI restore and independent local/remote tokens covered; no-local-KVI mode and production/GC integration remain M6. |
| KVI and local cleanup | LocalCleanupTest, RemoteCompactionExportTest | Unimplemented, M6: no local KVI; pinned local sources and target-file mapping; KVI-last remote publication. |
| Reader/file lifetime | ClusterIsolationTest, ReaderLifetimeTest | Native old-reader evidence; distributed pins/cleanup M2/M6. |
| Startup pipeline | StartupPipelineTest | Unimplemented, M3. |
| Cache loss | CacheLossTest | Unimplemented, M3. |
| Remote GC | HistoryOracleTest, RemoteGcTest | Model rejects deletion of reachable ranges; actual GC unimplemented, M6. |
| Transport | StorageFaultTest, TransportConformanceTest | Opaque-byte fault fixture only; progress/control codec, authority-bound TRL pull and auth/resume M4/M7; piggyback deferred. |
| Input/read contract | ClusterIsolationTest, InputReadContractTest | Native input cursor and snapshot evidence; replay/retention M3/M5. |
| Startup secondary-index reconciliation | ReplicationPreparationTest | One initial schema transaction with unchanged cursor, followed by ordinary application writes. Coordinator authority integration remains pending. |
| Detached liveness | DetachedLivenessTest | Unimplemented, M5. |
| Schema upgrade | SchemaUpgradeTest | Unimplemented, M5. |
| Same-term control revision | ControlRevisionTest | Unimplemented, M4/M5. |
| Liveness | [SchedulerTest](../BTDB.Replication.Test/SchedulerTest.cs), LivenessTest | Virtual-time/step-budget fixture only; watchdogs M4. |

The next step is M3 production restore/storage integration and connecting the canonical TRL lane to checkpoint prerequisite checks and the coordinator. Keep interfaces internal until their semantics are
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
