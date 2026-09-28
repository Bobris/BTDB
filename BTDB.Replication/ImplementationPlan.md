# BTDB.Replication implementation status and plan

Status 2026-09-28. Milestones M0–M6 are implemented and exercised by deterministic in-process, real-BTDB, Azurite
and subprocess tests. What remains is proof closure, live-provider and production qualification, measurement and
release work (M7). No B1/B3/B5/B6 blocker in the [Architecture.md](Architecture.md) blocker register is closed by
this status: link the closing tests there first.

## Sources of truth

- [Architecture.md](Architecture.md) owns every protocol rule, the blocker register and the backlog; this file only
  records implementation status and ordering. [AGENTS.md](AGENTS.md) is the working agreement.
- [ObjectStorages.md](ObjectStorages.md) owns storage semantics and provider evidence; [M1Evidence.md](M1Evidence.md)
  records the authority/clock model and CAS evidence.
- [Testing.md](Testing.md) describes the test harness and coverage; [ReplicationCore.md](../Doc/ReplicationCore.md)
  and [ReplicationHosting.md](../Doc/ReplicationHosting.md) document the core seams and the public hosting contract.

The application owns ordered input, handler execution, retries, event timeouts/skips, failure classification and
external effects. S3, local database import, a backup-import API, stronger read APIs and a business
acknowledgement/outbox protocol are out of scope.

## Admission rule

Add a wire field, persisted record, state, timer, round trip, hash, abstraction or optimization only when a concrete
failure case (preferably a deterministic test) or a measured substantial cost shows the simpler design insufficient,
and keep that evidence beside the change. Otherwise defer it. Without such evidence, do not introduce:

- durable ID reservations, operation IDs, persisted upload receipts, or persisted session/grant/placement state just
  to survive restart (fresh initialization and remote metadata reconstruct state);
- content-addressed keys, attempt directories, a checkpoint manifest/pointer, an uploaded GC ledger or a persistent
  cleanup state machine;
- a second grant-deadline calculator, per-follower revocation/acknowledgement, or follower restore leases;
- per-transaction peer envelopes, hashes, peer durability classification, or a peer compaction protocol;
- a mandatory provider capability probe on every startup (qualify providers with conformance tests instead).

## Implemented

- **M0 test oracle.** Isolated node scopes, deterministic scheduler, seeded randomness, fault-injecting
  blob/lease/peer simulators and an independent history oracle (`BTDB.Replication.Test/Simulation`). Evidence:
  `HistoryOracleTest`, `ClusterIsolationTest`, `SchedulerTest`, `StorageFaultTest`.
- **M1 authority and representation.** Conservative lease deadlines and one grant drain bound (`LeaseAuthority`,
  `ConfirmationGrants`), shared TRL names (`TrlFileName`) and the live Azure capability probe recorded in
  ObjectStorages.md. Evidence: [M1Evidence.md](M1Evidence.md).
- **M2 capture and local lifetime.** `TransactionLogCapture` completed, acknowledged and non-application positions,
  compactor retention, deterministic +2 TRL successors and even non-TRL IDs, automatic shared legacy-header publication during startup,
  TRL sizing, virtual batching, `ObjectDB.InitializeRelations` with at most
  one startup writer, and local execution after publication stops. Evidence: `TransactionLogCaptureTest`,
  `ReplicationCompactorTest`, `ObjectDbInitializeRelationsTest` in `BTDBTest`.
- **M3 publication and restore.** Serialized canonical TRL CAS lane with ordered native-file publication, adoption and
  ambiguity reconciliation (`CanonicalTrlPublisher`); fixed `{fileId}.trl` names across terms, no TRL protocol metadata;
  TRL discovery (`CanonicalTrlInventory`); remote-backed cache
  with SHA-verified reuse and bounded parallel downloads (`ReplicationFileSet`); dependency-first KVI export with
  whole-PVL remapping (`CheckpointPublisher`). Evidence: `CanonicalTrlPublisherTest`, `CheckpointPublisherTest`,
  `RestartRecoveryTest`, `ReplicationFileSetTest`, `AsyncOpenTest`.
- **M4 following, election and takeover.** Follower-first restore of every required database before lease
  contention (`ReplicationNodeCoordinator`, `LeaseSessionController`); term selection preserving application data
  (`LeaderSelection`); Blob validation and adoption before any publisher is exposed (`LeadershipActivation`,
  `LeadershipSession`); a serving leader answering stateless polls with one challenge/grant, three-field progress,
  published and schema positions and inline TRL bytes (`ServingLeader`); byte comparison (`FollowerComparisonSession`,
  `TrlPrefixComparer`, `RetainingLeaderTrlReader`); optimistic-tail adoption without rerunning handlers; activation,
  publication and maintenance watchdogs with explicit fatal recovery (`ReplicationProgressWatchdog`). Evidence:
  `ReplicationNodeCoordinatorTest`, `LeadershipActivationTest`, `FollowerComparisonSessionTest`,
  `ReplicationPeerPollTest`, `ReplicationProgressWatchdogTest`.
- **M5 lifecycle.** Generation floor and inline database set; prepared higher-generation handoff (`PreparedHandoff`);
  lower generations follow but never contend; removed databases continue locally; leader-only genesis and startup
  schema publication; leader-announced schema position with permanent follower detachment and the detached-leader
  restart timeout; opaque `applicationData` access (`ReplicationApplicationData`). Evidence:
  `ReplicationNodeCoordinatorTest`, `ReplicationApplicationDataTest`.
- **M6 compaction and cleanup.** Local compaction on every node; leader-only checkpoint export, skipped when
  unchanged; deletion only after a confirmed KVI, with a configurable positive delay persisted on each object, PVL
  protection/reupload and recovery from native KVI references (`ReplicationMaintenance`, `RemoteGarbageCollector`);
  leak candidates submitted as an application event (`CollectLeakRemovalCandidates`). Evidence:
  `RemoteMaintenanceTest`, `ObjectDbCompactorLeakCleanupTest`, Azurite tests.
- **M7 adapters (partial).** Azure leases, leader record and storage
  ([BTDB.Replication.Azure](../BTDB.Replication.Azure/README.md)); binary HTTP peer protocol (Connect, Poll, Read,
  Handoff) with ASP.NET Core hosting, health checks and metrics
  ([BTDB.Replication.Http](../BTDB.Replication.Http/README.md)); public `ReplicationStatus`; subprocess failover
  against Azurite, including leader kill and `SIGSTOP` suspension
  ([BTDB.Replication.Process.Test](../BTDB.Replication.Process.Test/README.md)). Evidence:
  `BTDB.Replication.Azure.Test`, `BTDB.Replication.Http.Test`, `ProcessFailoverTest`, `PublicHostingApiTest`.

Planned lease transfer exists only toward a prepared higher-generation follower. A same-generation graceful shutdown
fences immediately and lets the lease expire.

## Remaining work

1. **Proof closure (B1/B3/B5/B6).** Map existing tests to the blocker register and close only the blockers they
   actually prove. Extend schedule/property exploration beyond the named deterministic cases; keep each smallest
   failing schedule as a regression test.
2. **Clock qualification.** Qualify the production monotonic clock, rate bound, safety margin and OS-suspend
   behavior assumed in [M1Evidence.md](M1Evidence.md), and document them for hosts.
3. **Live Azure qualification.** Run the storage/lease suites against real Azure, including response loss, stale
   in-flight operations, finite-lease expiry, concurrent publication/cleanup and restore retry. Azurite results are
   not provider evidence.
4. **Process scenarios.** Extend subprocess tests to network partitions, rolling-upgrade handoff schedules,
   multi-database partial activation and disk/process failures during restore.
5. **Measurement.** Compare BTDB with replication disabled and enabled for allocations, throughput and commit
   latency; measure comparison memory/disk growth, warm/cold recovery and handoff. Target complete startup within
   15 minutes for about 100 GB, including cold download, validation, KVI open and TRL replay, on real Azure; tune
   bounded download concurrency and set operational defaults from these measurements, reporting hardware and network
   conditions. Sealed canonical TRLs currently carry no SHA-256 metadata, so restore always downloads them again;
   add a post-seal checksum only if measurement shows the cost matters.
6. **Operations.** Detailed recovery metrics, configuration validation and examples for application-owned event
   execution; document input retention and host restart requirements. Reader-visible progress and input lag stay
   application-owned.
7. **Release.** After qualification, update the README from the architecture status and prepare packaging.

## Validation discipline

- Update `CHANGELOG.md` for implementation, behavior or API changes; plan-only edits need no entry.
- Run SourceGenerator tests first, then the affected replication suites; run `dotnet test BTDB.sln` before a
  core-behavior PR and at release qualification.
- Fault tests interrupt before dispatch, after the storage effect and before response delivery, and check invariants
  at intermediate steps as well as at convergence.
- Treat protocol safety and measured performance as separate exit criteria; do not infer production latency from
  allocation measurements alone.
