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
- [Testing.md](Testing.md) describes the test harness and coverage; [Measurements.md](Measurements.md) records local
  performance measurements; [ReplicationCore.md](../Doc/ReplicationCore.md)
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
2. **Clock qualification.** Done: `SystemReplicationScheduler` uses the unadjusted hardware counter, measured on Azure
   against NTP-adjusted clocks and the service lease; 1000 ppm and a 250 ms margin are the documented, validated
   settings ([M1Evidence.md](M1Evidence.md), [ReplicationHosting.md](../Doc/ReplicationHosting.md)).
3. **Live Azure qualification.** Done 2026-09-28: `BTDB.Replication.Azure.Test` (including `AzureQualificationTest`:
   finite-lease expiry against the local deadline, delayed acquire replies, stale in-flight appends, renewals,
   leader-record writes, PVL uploads, deletes and marks, restore concurrent with publication, checkpoints and cleanup,
   and restore retry after deletion) and the subprocess failover suite pass against a live account from an Azure VM
   ([Testing.md](Testing.md), [ObjectStorages.md](ObjectStorages.md)). It found that a restore overlapping publication
   or cleanup failed on every changed version, and that a checkpoint published between TRL discovery and the PVL/KVI
   listing could open with missing values; both are fixed. Remaining: throttling and credential renewal under load.
4. **Process scenarios.** Done: subprocess tests cover planned upgrade handoff, storage and peer partitions of a
   leader or follower, a kill during restore and partial multi-database activation, in addition to leader kill,
   `SIGSTOP`, stalled publication, divergence and follower crash; all 13 pass on Azurite and live Azure
   ([BTDB.Replication.Process.Test](../BTDB.Replication.Process.Test/README.md)). Disk-full and device errors during
   restore are not simulated.
5. **Measurement.** Done on Azure ([Measurements.md](Measurements.md), `DBBenchmark replication*`): a cold restore
   of 100 GiB from Blob takes 4.4 minutes on an E8ads_v7, limited by the local disk; the findings led to lock-free
   node-local storage, sealed-TRL checksums, a 4 MiB poll budget and concurrent TRL staging. With production file
   sizes (2 GiB PVL, 1 GiB/4 GiB TRL) 30 GiB restore cold in 83–156 s on v4–v7. Remaining: warm-restore validation
   reads the whole cache, and HTTP peer cost, handoff timing and multi-database publication are unmeasured; set
   operational defaults from them.
6. **Operations.** Done: recovery counters in `ReplicationStatus` and the `BTDB.Replication` meter (restore attempts,
   failures and duration, leader sessions started and ended, failed steps), validation of lease-dependent settings
   when the first lease is acquired, and host guidance for application-owned event execution, input retention,
   restarts, recommended settings and alerts ([ReplicationHosting.md](../Doc/ReplicationHosting.md)).
   Reader-visible progress and input lag stay application-owned.
7. **Release.** After qualification, update the README from the architecture status and prepare packaging.

## Validation discipline

- Update `CHANGELOG.md` for implementation, behavior or API changes; plan-only edits need no entry.
- Run SourceGenerator tests first, then the affected replication suites; run `dotnet test BTDB.sln` before a
  core-behavior PR and at release qualification.
- Fault tests interrupt before dispatch, after the storage effect and before response delivery, and check invariants
  at intermediate steps as well as at convergence.
- Treat protocol safety and measured performance as separate exit criteria; do not infer production latency from
  allocation measurements alone.
