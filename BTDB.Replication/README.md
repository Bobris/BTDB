# BTDB.Replication

Status: Architecture proposal with deterministic tests and an executable authority/TRL publication model
([testing and coverage](Testing.md), [M1 evidence and remaining work](M1Evidence.md)), plus opt-in
[core transaction capture, decoding and writer admission](../Doc/ReplicationCore.md#local-transaction-capture).
The internal canonical TRL lane now publishes real capture ranges with conditional append/adoption and successor-first
ordering; see [tested behavior and remaining integration](Testing.md#canonical-trl-lane-necessity-and-limits).
Azure lease, leader-record selection, canonical TRL and immutable PVL/KVI adapters now live in
[`BTDB.Replication.Azure`](../BTDB.Replication.Azure/README.md), with pre-publication history validation and adoption.
An internal node coordinator now connects restore, authenticated in-process following, automatic election/takeover
and publication, with lease maintenance independent of application work. Its host supplies native restore, fresh
session identities and atomic completed event/cut snapshots, and owns event execution and restart.
Older generations permanently stop contending after discovering an upgrade, while shared databases keep following
and removed databases continue locally. Native local compaction, leader checkpoint maintenance, conditional
PVL/KVI cleanup and exact leak-event integration are implemented. Network hosting, Azure retained-root discovery
for TRL pruning, production API packaging and live-Azure qualification remain.

`BTDB.Replication` is a planned high-availability layer for running one or more logical BTDB databases on multiple
compute nodes. It combines a single canonical database history in object storage with fast disposable local caches on
the nodes. An ordered upstream event log remains the source for replaying a recent tail that has not yet been published
to object storage.

## Why this design

### Intended split-brain safety

Only one node may author the canonical database history. A node becomes leader only after acquiring a finite,
storage-enforced lease and publishing a new monotonic leadership term through compare-and-swap. Losing contact with the
leader does not grant authority to a follower, and nodes reject work from expired or older leadership terms.

The required behavior is fail-closed: when authority cannot be proven, canonical progress stops. Preventing two accepted
histories during a partition still depends on specifying and testing authority freshness and in-flight fencing (B1 in
the architecture's blocker register); the proposal is not yet a verified protocol.

### One shared recovery base, independent of node count

The application event log supplies durability and replay. Blob KVI/TRL accelerates recovery; an unpublished BTDB
tail can be regenerated without waiting for Blob on the commit path.

The cluster stores one shared canonical dataset in object storage rather than one complete durable database copy per
node. Checkpoints, transaction-log generations, and retained recovery artifacts may coexist, but they all belong to the
same database history. A node's local BTDB files are only a validated, disposable cache and never become a separate
source of truth.

Adding a replica therefore does not multiply the durable object-storage capacity required by the database. Storage
cost and remote garbage collection are primarily functions of database size and retention policy, not replica count.

### Disposable compute without mandatory persistent volumes

A new or replaced node can reconstruct its local cache from the shared checkpoint, the canonical transaction-log
boundary, and retained ordered events. Restart reuses sealed local files whose identity, length and freshly computed whole-file checksum match the selected Blob
version. Surviving files accelerate recovery but never establish ownership. This makes ordinary deployment replicas, autoscaling, and node replacement natural
operational choices.

### Fast local application processing

The application produces identical ordered transactions, including rollback attempts, on nodes hosting the same database.
Kafka is only one possible application integration. Nodes commit synchronously to their local speculative cache without
waiting for the leader, object storage, or acknowledgements from other replicas. The leader announces only the latest committed event ID and TRL file/position. Followers pull the needed TRL bytes
and compare the same event transaction history; notifications may be coalesced. Sending bytes with notifications to
avoid an extra request is a later optimization.
Virtual memory batching preserves ordinary TRL transaction bytes; real divergence restarts the follower.

Each node may independently use virtual transaction batching to reuse a memory BTree while preserving per-event commits
in the log. The application chooses rollback, retry, explicit skip or fail-fast; rollback preserves earlier commits. Replication needs no historical
roots, different-batch normalization or live suffix repair. Only the leader publishes canonical Blob history.

A local application-transaction commit is sufficient for application success, and ordinary reads use the local readable snapshot. Other nodes'
progress is not on this path. Virtual memory batching retains its normal deferred reader visibility. External effects
and failure policy belong entirely to the application; replication applies and checks transaction history.

Every node starts as a follower and fully verifies and restores all required published databases against Blob Storage, reusing matching local files
before attempting leadership. An unpublished new database has no existing files to restore.

On restart the previous process/readers terminate; remove scratch and obsolete/invalid files, then reuse verified
matching cache files in place. Download missing files in parallel with bounded lookahead to conserve local disk. Always download the last growing
TRL again; compute its whole-file checksum only after sealing, for reuse on later starts.
Without KVI, obtain every canonical TRL in ascending file-ID order and replay in that order while later files download.
With KVI, fetch only its required files and replay tail. Remote space is less constrained, so retained obsolete blobs
need not be downloaded. Disk exhaustion remains fatal; recovery needs no second full local cache copy.

Replication latency therefore does not normally become application-transaction latency, while the accepted state still
converges to one leader-authored sequence.

### Read and failover capacity without duplicated ownership

Additional nodes can provide read capacity and warm failover candidates while authority remains centralized and
explicitly fenced. Replicas may be temporarily at different confirmed positions, but two ready replicas at the same
canonical position must expose the same logical state.

### Independent local compaction and shared recovery

Every node, including followers, may compact its own local files independently without creating KVI. Local compaction
changes only physical storage, protects that node's readers and active file dependencies, and sends no results to peers.

Only the leader compacts remote Blob Storage files. It plans against the remote inventory, reuses or relocates local
PVL content into remote destinations, and serializes a native KVI with the corresponding remote file IDs and offsets.
It publishes the required files before KVI and only then deletes superseded remote objects. This export does not change
the running leader's local file identities or BTree pointers.

Neither compaction mode distributes physical rewrites, PVLs, completion results or deletion instructions to running
followers. A node obtains the remote KVI and its files through normal open/rebuild. Logical leak removal remains an
ordinary ordered application event requested through the parent system.

Transactions may span several TRL files. Only a complete transaction with a verified reachable recovery closure can
be published as durable through canonical TRL CAS; prepare successor files first, then publish their complete chain by CAS on the current canonical TRL.
The initial existing database is already in Blob Storage; no import workflow is required.
New replication TRLs use odd file IDs. Existing databases with even-numbered TRLs remain readable; before appending
new transactions to an even tail, the writer closes it and starts a fresh odd TRL without rewriting existing history.
Object-store publication advances only at complete BTDB transaction boundaries. Missing, stale, truncated, or mixed
local files cause validation and rebuild or fail-closed unavailability; they are never used to infer canonical state.

### Rolling application upgrades can change the database set

A newer application generation may add or remove logical databases without requiring every replica to change at once.
While an older leader is active, new database creation waits without provisional writes. The older leader gives priority
to a prepared newer node. After gaining authority, it initializes additions with a non-application transaction setting
`CommitUlong` to the event ID preceding the first input to apply and immediately publishes the result to Blob Storage.
Followers open the published canonical state. If initialization was never published,
a successor initializes that database from scratch.

When the newer database set removes a database, old nodes continue independently from their own local views in
disposable `.temptrl` files until shutdown. No exact final shared position is required, and database names are never reused. The leader record retains
a monotonic application-generation floor, so an older binary can keep following compatible databases but cannot later
be elected and resurrect a retired database.

Non-application writes wait inside BTDB until the node becomes leader. Their local commit immediately requests asynchronous
TRL publication, bypassing lazy batching without waiting for Blob or blocking later local work. Application writer
admission automatically sets CommitUlong from the supplied eventId. Followers may fetch recent tail bytes from the
leader with a short timeout, or regenerate unavailable unpublished input.
Startup restores existing bases before election without executing new migrations, allowing a newer follower to become
leader and then perform its pending schema writes.

A newer application may also add or remove secondary keys through a non-application schema transaction in canonical
TRL. An already running follower receiving that transaction stops following the affected database and continues from
its own view in disposable `.temptrl` until replacement. It does not apply the schema change or restart for divergence.
A compatible replacement restores the schema from TRL or a checkpoint containing it. The database remains active in
the cluster; its detached local suffix is never promoted.

The native KVI upload starts only after all required PVLs and canonical TRL through its saved cursor are published; no separate checkpoint pointer or manifest is needed.
Old unused files may be deleted only after that KVI write succeeds. Remote cleanup does not wait for followers restoring older checkpoints. After a complete replacement checkpoint is
published, obsolete remote files become eligible for deletion. Actual Blob deletion is planned with a configurable
operational delay, provisionally about one day, to reduce interference with restores. A follower whose startup loses a file restarts and loads
the newest published KVI; running replicas retain local files according to their own reader lifetimes.

## Design documents

- [Architecture](Architecture.md#how-to-use-this-specification) maps the normative invariants, shared identities,
  TRL publisher, transition engine, follower acceptance, and recovery rules.
- [Review decisions](Architecture.md#review-decisions-2026-09-14) record the selected rules, including KVI-last publication without a checkpoint pointer.
- [Open decisions](Architecture.md#open-decisions) tracks implementation/proof tasks and configuration work. No application-owner policy choice is currently pending.
  The proposal's safety and availability goals still require these mechanisms and their fault tests.
- [Object storage research](ObjectStorages.md) describes the provider-neutral storage contract and the Azure-first
  implementation direction.

A schema-detached follower never contends for leadership or accepts handoff during that session. If no valid leader is
observable for 15 continuous minutes, it requests a graceful restart. A fresh compatible session must restore normally
before becoming eligible; ordinary network disconnection alone does not disable failover.


## Internal lifecycle integration

`IReplicationNodeHost.RestoreAsync` returns every configured database. Published databases use ordinary file-set
initialization and native open. An explicitly unpublished addition has an empty local database and default
`RestoredBase`; a missing dependency of published history must still fail restore. The host keeps application input
for additions stopped until initialization completes. It owns database identities, canonical genesis keys and disposal.

After selecting authority and adopting every existing database, the coordinator calls
`CaptureInitializationCursorAsync` for a new database, commits that predecessor input cursor through the ordinary
native writer (including zero, using the existing temporary-close operation), then calls `PrepareSchemaAsync`. The latter reuses the application's ObjectDB `InitializeRelations`
and must be idempotent across interrupted attempts and revalidate authority before writing/commit. It also updates
`GetProgress` with the atomic completed event/cut, including genesis or schema-only work. Preparation is published
asynchronously at its fixed complete cut before leader service opens; ordinary application commits stay local.

Set `PreparedUpgrade` only when the host has validated compatibility, bounded lag/retained replay input, and all
added/removed database requirements. Its transfer UUID is fresh for this prepared target and retained across retries.
The selected leader drains grants and transfers through `IReplicationLeaseTransferStorage`; Azure implements native
lease Change. The receiver proves ownership by renewal, then performs normal term selection and Blob validation.
Equal-generation offers do not cause upgrade handoff. Equal higher-generation offers retain the first observed target.

Live native schema commits call `SchemaDetached` before comparison/confirmation. The host reports this database as
local-only and continues its ordinary reads/writes; replication permanently disables takeover for the node session.
Other databases may keep following. Missing current leader evidence for fifteen minutes requests graceful restart.
Schema transactions are identified from native commit semantics, with no wire kind, per-event capture queue or new
core transaction commands. The scanner uses one reusable bounded buffer; byte equality still uses the existing comparer.

## Maintenance integration

The coordinator runs ordinary native `Compact` on every node (five-minute default, configurable through
`CompactionInterval`). Its cancellation belongs to host lifetime, independent of remote authority and upload failure.
Implement `IReplicationNodeHost.CreateMaintenance` to return a session-scoped `ReplicationMaintenance` for each
published database, using its original file set, selected canonical publisher, authority-bound maintenance storage,
and configured checkpoint interval/deletion delay. The coordinator disposes the job on leadership replacement.
The default host hook disables remote maintenance; application database wrappers and storage clients remain host-owned.

Checkpoint publication retains one native snapshot across retries. Only confirmed publication permits cleanup,
which retains native dependencies, replay suffix and the highest even allocation identity. Conditional metadata
protection of reused PVLs prevents delayed old deletes from destroying a newer checkpoint. Candidate age is
session-local: restart safely restarts the deletion delay. Azure retains canonical links needed for root discovery.

Supply the ObjectDB and `publishLeakEvent` callback to submit bounded `LeakRemovalCandidates` as an application
event. Transport its `EncodedKeys` and `KeyCount`; apply it on every replica using `ApplyTo(transaction)` inside
that event's ordinary transaction, including its cursor and commit. The application owns ordering and retries.
Candidates must not be used after restoring a different database history. Replicated compaction disables automatic
leak erasure; standalone ObjectDB behavior is unchanged.
