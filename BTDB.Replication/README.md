# BTDB.Replication

`BTDB.Replication` runs one or more logical BTDB databases on several disposable compute nodes. One elected leader
authors a single canonical history in Azure Blob Storage; every node keeps a local, disposable BTDB cache and executes
the application's ordered events locally. The application's event log remains the source for replaying a recent tail
that was not yet published.

Status: preview. Implemented and tested with deterministic simulation and seeded schedule exploration, loopback HTTP,
Azurite, and subprocess failover and ObjectDB application tests; the adapter and subprocess suites also pass against
live Azure, and restore throughput is measured on Azure VMs. Packages are prepared as `-preview` versions. Open before
production use: qualification with the production application (see [Architecture](Architecture.md#open-work)).

## Properties

- **Split-brain safety.** A node leads only while it holds a finite Blob lease and the CAS-selected `leader.json` names
  its term and session. Losing contact with the leader never grants authority; when authority cannot be proven,
  canonical progress stops. Every canonical write is a conditional operation checked against that authority.
- **One shared recovery base.** Blob Storage holds one canonical history per database (native TRL chain plus KVI
  checkpoints), independent of the replica count. Local files are only a validated cache.
- **Disposable compute.** A new node restores from the latest checkpoint and canonical TRL, reusing local files only
  when their identity, length and SHA-256 match. No persistent volume is required. A restore keeps working while the
  leader publishes and cleans up.
- **Local-speed commits.** Application commits and default reads are local. Followers compare their native TRL bytes
  with the leader's through a lightweight poll; a mismatch restarts and rebuilds the follower.
- **Independent compaction.** Every node compacts locally without creating a KVI. Only the leader publishes checkpoints
  (native KVI written after all its files) and deletes superseded remote files after a delay.
- **Rolling upgrades.** A monotonic application generation and database-name set are selected with the term. A prepared
  newer node receives a planned lease handoff; older nodes never lead again. Added databases are initialized by the
  leader; removed ones continue locally on old nodes. Schema transactions detach running followers.

## Projects

| Project | Contents |
| --- | --- |
| `BTDB.Replication` | Coordinator, authority, publication, comparison, file set, checkpoints and cleanup; the optional `EventLog` of opaque records ([plan](EventLogImplementationPlan.md)). |
| [`BTDB.Replication.Http`](../BTDB.Replication.Http/README.md) | Peer transport over ASP.NET Core/Kestrel and hosting registration. |
| [`BTDB.Replication.Azure`](../BTDB.Replication.Azure/README.md) | Azure leader record/lease and data storage adapters. |
| [`BTDB.Replication.Process.Test`](../BTDB.Replication.Process.Test/README.md) | Subprocess failover, partition, upgrade and ObjectDB application tests using only the public API. |

## Documents

- [Architecture](Architecture.md): behavior, invariants, mechanisms, open work and design history.
- [Hosting](../Doc/ReplicationHosting.md): how an application integrates and configures a node.
- [Core APIs](../Doc/ReplicationCore.md): the opt-in BTDB core features replication builds on.
- [Object storage](ObjectStorages.md): the provider-neutral storage contract and Azure/S3 research.
- [Testing](Testing.md): test harness, coverage and measurements. [M1 evidence](M1Evidence.md): authority clock model.
- [Implementation plan](ImplementationPlan.md): implementation status and remaining steps.
- [Event log plan](EventLogImplementationPlan.md): design, measurements and status of the optional event log.
- [AGENTS.md](AGENTS.md): working agreement for changes to this library.
