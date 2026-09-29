# BTDB.Replication implementation status

Status recorded on 2026-09-28: M0–M6 are implemented. M7 has recorded adapter, process, clock and performance
qualification; production-application qualification remains open (B3). Packages are prepared as `-preview`.
This status summarizes the linked evidence, not a new live qualification run.

## Implemented

| Milestone | Implementation | Evidence |
| --- | --- | --- |
| M0: simulation | Isolated nodes, deterministic scheduler, fault injection and history oracle. | [Testing: simulation](Testing.md#deterministic-simulation-harness) |
| M1: authority | Conservative lease deadlines, term selection and confirmation-grant drain. | [M1Evidence.md](M1Evidence.md) |
| M2: core support | Native capture, deterministic TRL IDs, legacy startup conversion, virtual batching and startup schema writes. | [Core APIs](../Doc/ReplicationCore.md) |
| M3: storage | Canonical TRL CAS, ambiguity reconciliation, verified cache and dependency-first checkpoint export. | [ObjectStorages.md](ObjectStorages.md), [restore tests](Testing.md#discovery-restore-and-local-cache) |
| M4: coordination | Follower-first restore, activation, comparison, takeover and progress watchdogs. | [Authority and activation tests](Testing.md#authority-selection-and-activation) |
| M5: lifecycle | Generation floor, prepared upgrade handoff, database changes, schema detachment and opaque application data. | [Coordinator tests](Testing.md#coordinator-simulation) |
| M6: maintenance | Independent local compaction, leader checkpoints and delayed conditional cleanup. | [Maintenance tests](Testing.md#checkpoints-and-remote-maintenance) |
| M7: integration | Azure adapter, HTTP hosting, health/metrics, subprocess scenarios and preview package metadata. | [Hosting](../Doc/ReplicationHosting.md), [provider evidence](ObjectStorages.md), [measurements](Measurements.md) |

## Remaining work

- **B3: production application.** Qualify the actual application's ordered execution, replay, schema upgrades and
  external-effect policy. The sample ObjectDB process tests are supporting evidence, not completion of B3.
- **Release.** Add the replication packages to `Releaser` and publish them after qualification; removing the preview
  suffix is a separate release decision.
- **Deployment coverage.** Qualify the intended TLS/proxy path and real network partitions. Live Azure tests are
  manual; physical disk faults are not modeled. See [test limits](Testing.md#not-covered-yet).
- **Event log.** The optional `BTDB.Replication.EventLog` is implemented as a preview (E0 harness, E1–E5). Its
  application integration, migration and multi-VM qualification remain; see
  [EventLogImplementationPlan.md](EventLogImplementationPlan.md#11-implementation-milestones).
- **Optional work.** Same-generation shutdown handoff, S3 and stronger reads remain in the
  [architecture backlog](Architecture.md#open-work); they are not prerequisites for the selected protocol.

B1, B5 and B6 have recorded closure evidence in the [blocker register](Architecture.md#open-work). Keep their stable
IDs and evidence there instead of duplicating the qualification narrative here.

## Document ownership and change discipline

- [Architecture.md](Architecture.md) owns protocol rules, invariants, blockers and the backlog.
- [ObjectStorages.md](ObjectStorages.md) owns provider contracts; [M1Evidence.md](M1Evidence.md) owns clock evidence.
- [Testing.md](Testing.md) owns test coverage; [Measurements.md](Measurements.md) owns performance results.
- [Core APIs](../Doc/ReplicationCore.md) and [Hosting](../Doc/ReplicationHosting.md) own integration guidance.
- [AGENTS.md](AGENTS.md) defines the admission rule: add a mechanism only for a demonstrated failure or measured cost.

Run SourceGenerator tests first, then affected suites. Behavior/API changes require a changelog entry and relevant
regression tests; core-behavior PRs require `dotnet test BTDB.sln`. Documentation-only changes do not need a changelog
entry. Test safety and measure performance separately; neither substitutes for production-application qualification.
