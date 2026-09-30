# BTDB.Replication Architecture

This is the living design document for `BTDB.Replication`: the agreed behavior, its safety invariants, the selected
mechanisms and the work that remains. Provider research and Azure/S3 mechanics live in
[ObjectStorages.md](ObjectStorages.md); test coverage lives in [Testing.md](Testing.md); the opt-in BTDB core APIs are
described in [ReplicationCore.md](../Doc/ReplicationCore.md) and hosting in [ReplicationHosting.md](../Doc/ReplicationHosting.md).

## Status

The mechanisms below are implemented: the node coordinator, lease authority and leader selection, activation and
canonical TRL publication, follower comparison and the poll protocol, schema detachment, upgrade handoff, checkpoint
publication with delayed remote cleanup, restore through `ReplicationFileSet`, progress watchdogs, the HTTP transport
and ASP.NET Core hosting, and the Azure adapters. They are exercised by deterministic in-process simulation and seeded
schedule exploration, loopback HTTP, Azurite, and subprocess failover and ObjectDB application tests, and qualified
against live Azure (adapter and subprocess suites, clock and lease margins, restore of 100 GiB on Azure VMs). Open:
qualification with the production application; see [Open work](#open-work).

The optional event log (`BTDB.Replication.EventLog`) is a sibling component, not part of this protocol: it has its own
per-topic ETag ownership and never changes database authority, publication or restore. Its design, invariants and
status live in [EventLogImplementationPlan.md](EventLogImplementationPlan.md).

### Admission rule

Implement only mechanisms whose necessity has been established. A proposed field, message, state, persisted record or
optimization needs a concrete failure of the simpler design (preferably a deterministic test) or a measured
substantial cost on a relevant workload; record that evidence with the change. Agreed behavior and safety invariants
are requirements; mechanism descriptions are the current solution, not a checklist of future additions.

### Section ownership

| Concern | Owner |
| --- | --- |
| Safety invariants and shared identities | [Invariants](#invariants) |
| Leader record, lease, grants | [Authority](#authority) |
| Bootstrap, takeover, activation, handoff | [Node lifecycle and transitions](#node-lifecycle-and-transitions) |
| Every canonical TRL write | [Canonical TRL publication](#canonical-trl-publication) |
| Live follower acceptance | [Following the leader](#following-the-leader) |
| Database sets, genesis, schema transactions | [Upgrades and database sets](#upgrades-and-database-sets) |
| KVI publication and remote cleanup | [Checkpoints and remote maintenance](#checkpoints-and-remote-maintenance) |
| Opening or rebuilding a database | [Restore](#restore) |
| Provider behavior | [ObjectStorages.md](ObjectStorages.md) |

## Model

`BTDB.Replication` runs one or more logical `BTreeKeyValueDB` databases on disposable compute nodes.

- A **cluster** shares one Azure container, one leader record (`leader.json`) with a native Blob lease, and one
  ordered application event stream. `Deployment` and `StatefulSet` topologies use the same protocol; node count,
  ordinals and surviving local files never establish authority.
- One process hosts one node, which may host several databases. Each database has a short name unique in the
  cluster; names are never reused after removal.
- Exactly one cluster-wide **leader** authors canonical history for every database in the selected database set. It
  publishes canonical TRL, checkpoints and remote cleanup to Blob Storage. Followers execute the same application
  events locally and compare their native TRL bytes with the leader's.
- The **application** owns the ordered input, identical execution on every node (including rollbacks), recovery of
  unpublished work by replaying retained input, event timeouts/skips, external effects and command acknowledgement.
  Kafka is only an example. Replication never invents an event outcome.
- Blob KVI/TRL is the shared recovery base that avoids replaying all input. Losing an unpublished BTDB tail is
  expected; the application input regenerates it. A published (Blob-durable) boundary is internal restore
  bookkeeping, not an application acknowledgement.
- Ordinary application commits are local: they never wait for another node, the leader or Blob Storage. Default reads
  use local snapshots. Every local file is a disposable, untrusted cache.

Non-goals: multi-primary histories or merging, consensus or event retention (the input log owns those), a
peer-to-peer membership mesh, distributed transactions across databases, treating loss of a peer session as loss of
leader authority, Byzantine tolerance, and a local-database import workflow (the initial database already lives in
Blob Storage).

## Invariants

| ID | Invariant |
| --- | --- |
| I1 Authority | Only the session that holds the lease and is named by the selected leader record authors canonical work. Losing a peer session never grants authority. A fenced authority object never becomes valid again. |
| I2 Ordered history | Each application event is one ordinary BTDB transaction whose commit changes `CommitUlong` to its event ID. A committed transaction that leaves `CommitUlong` unchanged is non-application (genesis or schema). Rollbacks stay in TRL and are compared like commits. Virtual batching changes only memory-root publication, never TRL payloads. No new `KVCommandType` exists. |
| I3 Confirmation | Followers compare native TRL bytes over the same history. A match advances metadata only; a mismatch restarts the follower for canonical rebuild. Replication keeps no historical roots and repairs no live suffix. |
| I4 Local execution | Application commits never wait for leader progress, external I/O or Blob Storage. Non-application writes run only under leader authority and trigger immediate asynchronous publication. Disk exhaustion is a fatal node error. |
| I5 Durable closure | A canonical TRL CAS directly publishes complete transactions; a transaction may span TRL files and is durable only when its whole chain is reachable. A KVI is published only after every prerequisite and never describes history ahead of canonical TRL. |
| I6 Publication fence | One serialized publisher per database dispatches canonical writes. Adoption fences the predecessor on the TRL itself. Ambiguous results are reconciled; cancellation or an old read never proves that a write did not land. |
| I7 Database set | The selected application generation never decreases; equal generations select equal name sets. Published genesis fixes a database's initial history; unpublished additions may be initialized again. Removed names are never reused. |
| I8 Recovery | Restart reuses only sealed local files matching the selected remote identity, length and whole-file SHA-256; everything else, including the growing TRL tail, is downloaded again. Unverified local data never becomes canonical input. |
| I9 File lifetime | Every live reader and root keeps its bytes. Local deletion depends only on local pins; remote deletion protects the current recovery closure and is delayed; a restore that loses a superseded file restarts onto the newest checkpoint. |
| I10 Maintenance | Every node compacts locally without creating a KVI. Only the leader publishes checkpoints and deletes remote files. No compaction operation, result or deletion command is sent to peers. |
| I11 Stopped publication | Shutdown, removal and schema detachment irreversibly stop remote publication for that node session; ordinary local writes, reads and compaction continue. Such local work is never promoted. |
| I12 Isolation | Every nondeterministic or external boundary is injected and node-scoped. Standalone BTDB format, synchronous semantics and hot-path cost are unchanged when replication is not used. |

Shared identities reuse what already exists: database/stream identity, term and session, native `(fileId, offset)`
positions, `CommitUlong`, native TRL continuation headers (`PreviousFileId`) and opaque storage version tokens. Positions
are ordered only within one database and proven native ancestry; equal event IDs alone never compare branches.
Operation IDs, sequence counters, event hashes, manifests and kind markers are not used.

## Ports

Correctness-relevant code runs without a real network, web server, cloud SDK, wall clock or second process. The core
depends only on injected ports:

| Port | Responsibility |
| --- | --- |
| `IReplicationScheduler` | Monotonic time and serialized timer callbacks. |
| `IReplicationLeaderStorage` | `leader.json` reads and lease/ETag-conditional writes; lease acquire, renew and change. |
| `IReplicationStorage` (`IRemoteFileCollection`) | Canonical TRL CAS and version-bound reads, immutable PVL/KVI publication, remote inventory, delayed deletion. |
| `IReplicationPeerTransport` | Leader-centered peer sessions: poll, range read, handoff offer. |
| `IReplicationNodeHost` | Application integration: restore, candidate identity, progress, restart and fatal restart, schema preparation, maintenance, genesis cursor. |
| `IReplicationFileStorage` | Node-local file storage (`InMemoryReplicationFileStorage`, `OnDiskReplicationFileStorage`). |

The in-process peer transport and in-memory storage are test adapters with the same semantics as the HTTP and Azure
adapters. A deterministic cluster harness runs several complete nodes in one process under virtual time.

## Authority

### Leader record

`leader.json` is one small block blob. Its ETag is the CAS token and a finite native Blob lease on the same blob gives
server-enforced liveness:

```json
{
  "format": 1,
  "clusterId": "cluster",
  "term": 7,
  "revision": 42,
  "nodeId": "node-a",
  "sessionId": "random-session-id",
  "applicationGeneration": 12,
  "databaseNames": ["main", "users"],
  "peerEndpoint": "https://10.0.0.7:8443",
  "apiKey": "random-per-term-secret",
  "applicationData": {}
}
```

A session is leader only while it holds the lease and the record names its term and session. Selection rereads the
record under the owned lease and conditionally writes term + 1, revision + 1, a fresh session ID and API key, its
endpoint, generation and database names, preserving unknown fields and `applicationData`. A lost or rejected response
is reconciled by comparing the exact intended JSON; only that proves selection. Lease renewal never rewrites the JSON.

`apiKey` authenticates peer requests within the storage trust domain; it is rotated with every term, compared in
constant time and never logged. `applicationData` is opaque application JSON: any node may read a version-bound
snapshot, only the active leader replaces it (lease plus ETag), and replication never interprets it
(`ReplicationApplicationData`). Backup restore is an operator procedure: scale to zero, copy the backup into the
primary Blob namespace, scale up.

### Lease authority

`LeaseAuthority` holds a conservative local deadline: request dispatch time plus the provider-guaranteed duration,
scaled by the maximum clock drift and reduced by a safety margin. A late response never extends it, and a fenced
authority never becomes valid again. `LeaseSessionController` renews at least halfway through the remaining lifetime;
after a failed request it retries no later than that, so one transient error cannot outlast the lease. Disqualification
(newer generation observed, schema detachment, transfer, shutdown) is permanent for the node session.

### Confirmation grants

Every poll carries a challenge; the leader answers with a grant only when the whole grant window, bounded for clock
drift, fits inside its own authority deadline. A grant is leader-liveness evidence and a split-brain diagnostic, never
a vote: missing grants block neither local commits nor takeover. Planned handoff stops issuing grants and waits until
the latest issued grant has expired before transferring the lease. The clock model and race tests are in
[M1Evidence.md](M1Evidence.md).

## Node lifecycle and transitions

`ReplicationNodeCoordinator` owns one node's transitions; the host owns databases, input processing and process
restart.

| Role | Behavior |
| --- | --- |
| `Restoring` | `IReplicationNodeHost.RestoreAsync` restores every required published database from Blob Storage. No lease contention. |
| `Follower` | Discover the leader record, connect to the leader and compare. Lease maintenance runs; acquiring the lease starts activation. |
| `Activating` | Select the term, validate and adopt every database, create missing databases and schema, publish required cuts. |
| `Leader` | Serve peers, publish canonical TRL per database concurrently, run checkpoint maintenance. |
| `RestartRequired` / `Stopped` | Replication has ended for this node session; the host restarts or stops the process. |

Every node starts as a follower and restores all required published databases before its first lease request, so an
incomplete restore never permits candidacy. Discovery validates cluster and format; a record with a newer generation
disqualifies the node and marks databases missing from its name set as removed (`DatabaseRemoved`). An equal
generation with a different name set is a configuration fault.

### Activation

After acquiring the lease, `LeadershipSession` keeps one selection/activation lane per lease and retries it on later
steps while the host keeps executing input:

1. Select the leader record (above). A selection conflict fences the lease.
2. For each existing database, list the shared native TRL files and compare
   the complete local TRL prefix from the node's canonical base with canonical Blob bytes (several reads in flight).
   A candidate that lags behind canonical history returns "not yet" and retries; verified ranges are remembered for
   the lease. Any mismatch requires restore.
3. Once every database has matched, adopt each canonical tail: CAS the same bytes to obtain a fresh version token.
   This fences every predecessor request that expected the old version. A lagging database therefore never makes
   retries adopt the databases before it again.
4. For a database without published history, capture the initialization input end from the host and commit genesis
   (`CommitUlong` = predecessor event ID). Then let the host prepare schema (`PrepareSchemaAsync`, for example ObjectDB
   `InitializeRelations`). Publish each resulting cut immediately.
5. Start serving. Local optimistic transactions beyond the adopted end are ordinary local TRL after the verified
   prefix and are published by the next publication step; nothing is re-executed.

Preparation runs once per lease; later steps only publish. Losing authority at any point drops the leadership session;
its selection, verified prefixes and publishers are never reused under another lease.
An initialization/schema publication conflict fences authority and requests canonical restore immediately. For example,
a predecessor's delayed genesis may win after empty-root discovery. A conflicting publisher cannot make progress by
retrying, whereas an unresolved write still reconciles the same intent on later steps.

### Planned upgrade handoff

A follower whose host reports a `PreparedHandoff` of its own, higher application generation offers it to the leader
together with a proposed lease ID. The host reports it only after compatibility, retained input, continued database
lag and added databases are prepared. The leader accepts the highest offered generation above its own, stops issuing
grants and publication, waits for the grant drain and transfers the lease with Azure `Change Lease`. The target proves
ownership by renewing the proposed ID and then runs ordinary activation. A lost transfer response leaves the old leader
permanently disqualified; the target probes by renewal and otherwise the lease expires.

Same-generation handoff on graceful shutdown is not implemented: shutdown fences authority and publication
synchronously and the lease expires. Local work after the stop remains ordinary local files that the next startup
validates or discards.

## Canonical TRL publication

One `CanonicalTrlPublisher` per database and term consumes `TransactionLogCapture`. Capture tracks only the latest
complete local position and the acknowledged position; unacknowledged TRL files are retained by compaction. There is no
per-transaction record, queue or index.

- A plan covers a fixed complete prefix: the tail append plus successor TRLs in native lineage order. Write the
  oldest file first; create each new `{id}.trl` only if absent. All terms use the same key for an ID.
- A rejected create reads and compares the existing object against the local native TRL in bounded chunks. Identical
  content is accepted; an identical shorter prefix is extended with CAS against its verified version. A different
  byte, incompatible length or conflicting append fences authority and requests restart for canonical restore.
- Every dispatch checks authority. Ambiguous outcomes retain the exact token and source cut for reconciliation and
  retry; after authority loss only reconciliation reads happen. Capture is acknowledged after the entire plan lands.
- A crash or lost authority between files may leave an incomplete final transaction. Writable open keeps its
  published bytes and ends it with a native rollback at the start of the deterministic successor of the last published
  TRL; every node restored from that history writes the same terminator, and the application input then executes the
  transaction again after it. `ReplicationRestoredPosition` is the published end, so activation adopts the tail without
  regenerating bytes that another leader published: regenerating in place would let a nondeterministic or changed
  execution block every takeover (`RestartDuringMultiFilePublicationTerminatesTheUnfinishedTransactionWithARollback`
  diverged with it). A live follower that executed the transaction completely diverges and restores; an old delayed
  create that wins the successor ID conflicts, and the next restore terminates one file later. Only a leader
  publishes the terminator. An unfinished legacy tail predates replication and is still rewound before its transition.
- `PublishThroughAsync` covers a given complete cut (genesis, schema preparation, checkpoint prerequisite);
  the coordinator publishes all databases concurrently each step.
- Adoption changes the tail's ETag with an unchanged-content CAS. It stores no term metadata and fences delayed writes
  that expected its predecessor version. Terms remain in `leader.json` and live sessions.

Native TRL rules used by publication:

- Replication TRLs start at 1 and each new successor is exactly the previous TRL ID +2, independently of local or
  remote inventory maxima, compaction and restart. Every other new file uses an even ID. Exact-ID collisions fail;
  never skip an ID. Existing chains with gaps remain readable through their native links.
- Legacy conversion runs automatically during writable open, before local application work. Nodes select the same
  odd successor above the legacy tail and retained odd IDs from the remote inventory, then conditionally publish its
  native header. Concurrent creates compare the existing header; a conflict fails restore and an extended successor
  requires rediscovery. This bootstrap create needs no leader authority and never overwrites existing content.
  Generation zero marks completion; a header-only successor survives restart without changing the application cursor.
  Subsequent allocation always uses +2. See [core startup](../Doc/ReplicationCore.md).
- `ITransactionLogSizeStrategy` maps a TRL file ID to fixed soft and hard limits, identical on every node. Soft limits
  rotate between transactions; hard limits may split a transaction between commands.
- Replication writes no `TemporaryEndOfFile` or `EndOfFile` markers; a reopened tail continues at its exact committed end.
- TRL keys are exactly `{fileId}.trl` with positive unpadded decimal IDs. PVL and KVI names are
  `{fileId}.pvl` and `{fileId}.kvi`. There are no term directories, successor links or recovery-root
  metadata. Listing provides candidates; native TRL headers and KVI references determine the recovery closure.

## Following the leader

Followers talk only to the leader named by `leader.json`. Each step sends one poll for all compared databases, newly
selected databases and the authority heartbeat:

| Poll answer field | Meaning |
| --- | --- |
| Challenge, grant | Leader liveness evidence for this request. |
| Progress `(eventId, trlFileId, trlPosition)` | Latest complete leader-local cut; coalesced and repeated, not Blob durability. |
| Published | The leader's canonical Blob cut. |
| Schema | End of the latest non-application commit the leader replayed or committed. |
| Inline chunks | Leader TRL bytes from the follower's requested position, within the poll budget (4 MiB, the maximum a leader honours). |

Remaining bytes are read by range (at most 256 KiB per HTTP request). Every request is bound to cluster, term,
session, endpoint and API key; the leader reauthenticates after every await, and only complete captured bytes are
served. The HTTP adapter is stateless per request, maps into the host's Kestrel pipeline and requires HTTPS except on
loopback; see [BTDB.Replication.Http](../BTDB.Replication.Http/README.md).

- **Comparison.** The coordinator calls `FollowerComparisonSession.CompareAsync` serially with each poll's progress
  cut; there is no separate progress notification queue or comparison semaphore. It compares leader bytes with local
  TRL bytes from its resume position, following native lineage across rotations. A lagging follower compares the complete local prefix the leader's cut
  already covers. A match advances the compared position; a mismatch requests restart.
- **Retention.** `RetainingLeaderTrlReader` keeps up to 8 MiB (two poll budgets) of received leader bytes per
  database across steps, so a lagging comparison never fetches them twice. An inline chunk that continues in a later file shows where the
  previous file ended, so rotations need no end-of-file range read.
- **Canonical base.** Bytes compared up to `min(compared, published)` are canonical. The base advances capture
  acknowledgement (releasing local TRL retention), seeds the next leader's activation validation and survives
  reconnects. Comparison progress with one leader session survives reconnects; a new leader rechecks from the base.
- **Schema detachment.** If `Schema` lies beyond the canonical base, the follower detaches that database before
  comparing: it stops following it, keeps executing locally (`SchemaDetached`, status not ready) with its whole
  local TRL acknowledged for compaction, is permanently disqualified from leadership for this session, and requests a graceful restart after 15 minutes
  (`DetachedLeaderTimeout`) without valid leader evidence. A position covered by the base, including genesis, is a
  duplicate. An older schema commit the leader did not replay surfaces as ordinary divergence and restarts the follower.
- **New databases.** A database the leader selected but this node has not restored is polled for progress only; once
  the leader reports progress for it (after genesis) the follower restarts to restore it.

## Upgrades and database sets

Every application build declares a monotonic `applicationGeneration` and its database names. The leader record selects
both atomically with the term. The selected generation is a permanent election floor: older nodes may keep following
compatible databases but never lead again, even when every newer node is unavailable.

- **Added database.** A newer follower waits without provisional writes or reads. The leader that selects the new set
  initializes it (genesis) and publishes it immediately; followers restore the published history. Its first
  publication is that leader's canonical base, so after losing the lease it follows the next leader without a
  restore. An unpublished genesis may be recreated by a later leader at a newer input end; published genesis is never
  replaced.
- **Removed database.** A database missing from the selected set is outside coordination: no adoption, publication,
  compaction or cleanup by the new leader, and no retirement record or frozen boundary. An old node continues it
  locally until shutdown and acknowledges its capture, as for a detached database, so local compaction keeps no TRL
  for replication. Delayed old writes to its namespace are harmless.
- **Schema transactions.** A newer application may write non-application transactions (for example secondary-index
  changes) with ordinary commands and an unchanged `CommitUlong`, only under leader authority: ObjectDB checks all
  relations read-only and persists new schemas and index upgrades in at most one startup writer after the host waits
  for leadership. They publish immediately. Running followers detach (above); compatible replacements restore them by
  ordinary replay or checkpoint. Rollback terminators never count as schema commits.

## Checkpoints and remote maintenance

A checkpoint is a native KVI plus the files it needs; there is no manifest, pointer or selection CAS.
`ReplicationMaintenance` runs on the leader per database at a configured interval, on its own lane that never holds
back canonical TRL publication or the maintenance of other databases:

1. Capture a pinned `KeyIndexSnapshot`. If its TRL cut and source files equal the last published checkpoint, skip
   the export and only collect garbage.
2. Publish canonical TRL through the snapshot cut.
3. For each sealed PVL, reuse a confirmed placement or upload the whole file under a fresh even remote ID (IDs come from
   one maintenance listing per checkpoint); then protect it: a deletion mark is cleared (changing the version), an
   unmarked object keeps its version so concurrent restores continue, and a reused copy listed unmarked needs no
   request. An absent remote copy gets a fresh identity; retired keys
   are never recreated. TRL IDs, offsets and bytes are never remapped.
4. Only after every prerequisite is confirmed, stream the native KVI with remapped PVL IDs to a fresh immutable object.
   Native KVI references identify its required files; no recovery-root metadata is stored. One namespace listing
   supplies TRLs and deletion marks. An ambiguous immutable upload is checked by identity, length and SHA-256.
5. Collect garbage: list remote files and, unless a newer KVI has appeared, keep the published KVI, its dependencies,
   the TRL chain from its oldest dependency, the highest even ID (allocation anchor) and files marked for discovery.
   Mark every other file with a deletion deadline and delete it only when the deadline has passed and its version
   still matches. The delay must be positive (production assumes at least a day), so a late mark from a fenced
   predecessor cannot become due before this leader's cleanup clears it, and unmarked files never need a version
   change that would invalidate restores reading them.
6. Optionally collect ObjectDB leak-removal candidates and hand them to the application, which publishes an ordinary
   ordered event whose handler erases the listed keys idempotently on every replica. Replication never erases them.

A running follower never receives a KVI; checkpoints are consumed only by a later open or rebuild.

Local compaction (`Compact`, every `CompactionInterval`, default five minutes) runs on every node independently. It
rewrites values into new local PVLs without TRL output or a local KVI, protects readers, the current tree, capture and
export snapshots, and has its own cancellation independent of remote authority.

## Restore

`ReplicationFileSet` is the `IFileReplicatedCollection` of a replicated database: a local file storage plus a separate
remote inventory (`CanonicalTrlInventory` for TRLs and the storage listing for PVL/KVI).

1. `InitializeAsync` lists the remote inventory, removes local files without a remote counterpart and candidates
   whose extension, length or remote metadata rule them out, without reading file bytes. The inventory becomes
   visible only after a complete successful attempt. Every selected file keeps its remote ID locally.
2. `BTreeKeyValueDB.OpenAsync` tries KVIs in descending ID order, reads only needed headers, replays TRL from the
   selected cut in lineage order and prefetches every referenced file before returning. The KVI loads while it
   downloads or while its cached copy is hashed, and prefetch of the replayed TRLs and each referenced file starts
   during the load, so downloads and cached-file checksums overlap it. Downloads use 4 MiB ranges
   with four in flight per file and a bounded number of files; sealed files are verified while written, the growing
   tail is always downloaded again, and a partial download is removed.
3. The attempt keeps its selected state while the leader continues. A newer version of a selected file still serves
   the selected bytes when it provably keeps them: a TRL at least as long (canonical TRLs are append-only, so appends,
   sealing, adoption and deletion marks change only its version), or a PVL/KVI with the same length and SHA-256
   (marks change only metadata). A KVI whose TRL cut lies beyond the discovered TRL history was published after
   discovery and is ignored, so the attempt opens the newest checkpoint that existed at discovery. A removed file or
   any other change fails the attempt; the host disposes it and restores again from the newest published state. The
   deletion delay therefore bounds how long a restore may take. There is no remote restore pin and no fallback to an
   older KVI when the selected closure is broken; a broken current closure keeps the node unavailable, and published
   history is never replaced by initialization.

The target is complete startup of about 100 GB within 15 minutes, including cold download and replay; measured in
[Measurements.md](Measurements.md).

## Failures and progress

| Failure | Behavior |
| --- | --- |
| Lease lost, fenced or expired | Cancel remote work, drop leadership, continue as follower; local work continues. |
| Selection conflict | Fence the lease; another node or a later lease reconciles. |
| TRL publication conflict | Fence the lease and request restart immediately, independently of other database publications; cancel and drain outstanding work before releasing its files. |
| Ambiguous storage result | Reconcile the exact intent; retry the same intent while authority is valid. |
| Follower divergence or missing retained TRL | Request restart; restore rebuilds from Blob Storage. |
| Leader unreachable | Reconnect via `leader.json`; lease expiry lets an eligible follower take over. |
| Local disk exhaustion | Fatal node error; restart and restore; insufficient space keeps the node unavailable. |
| Remote file missing during restore | Restart the restore onto the newest checkpoint. |
| Activation, publication or maintenance makes no forward progress | Optional watchdogs fence authority immediately and request fatal restart after `RestartDelay` (`IReplicationNodeHost.RequestFatalRestart`). |

Watchdogs measure only replication work that is pending; idle input and an advancing restore are healthy. Application
event timeouts, skips, failure classification and restart policy belong to the application. `ReplicationStatus`
reports role, readiness and per-database local, compared and published cuts; readiness means available for local work,
not caught up.

## Open work

Blocker register: engineering work that must be closed, with linked evidence, before the mechanism is relied on in
production. The IDs are stable references used by other documents; closed entries stay for their evidence.

| ID | Area | Remaining work |
| --- | --- | --- |
| B1 | Authority qualification | Closed 2026-09-28: the production clock (`SystemReplicationScheduler`), its rate against NTP and the service, process pauses and lease margins are qualified on Azure ([M1Evidence.md](M1Evidence.md), [ObjectStorages.md](ObjectStorages.md)). Documented limits: hosts that suspend are unsupported; Azure lease break is never used. |
| B3 | Application integration | A sample ObjectDB application qualifies rollbacks inside virtual batches, input replay across failover and cold restore, and a schema upgrade published by a handed-off upgraded leader that detaches old nodes (`BTDB.Replication.Process.Test`). A rolling schema upgrade under continuous input works through the handoff, the activation deadline and a restore of the frozen published history (`RollingSchemaUpgradeUnderInputRestoresTheLaggingUpgradedLeaderOntoPublishedHistory`); an upgraded node must not execute before its schema is published (`UpgradedFollowerExecutingBeforeItsSchemaIsPublishedRestartsWithoutAffectingHistory`). Open: qualification with the production application. |
| B5 | Publication and cleanup races | Closed 2026-09-28: stale predecessor appends, renewals, leader-record writes, PVL uploads, deletes and marks, and restores concurrent with publication and cleanup, are qualified on live Azure (`AzureQualificationTest`). A fenced predecessor's PVL/KVI commit landing first on a successor's identity makes the successor fence itself on the SHA conflict; that costs one takeover (a lease expiry plus activation, 15–20 s in the live subprocess tests) and needs a predecessor request delayed beyond its lease and the successor's activation, so no mechanism is added. |
| B6 | Progress and operations | Closed 2026-09-28: recommended settings, recovery metrics, alerts and input-retention rules are documented in [ReplicationHosting.md](../Doc/ReplicationHosting.md), with checkpoint cadence and deadlines derived from the measured 100 GiB export and restore times ([Measurements.md](Measurements.md)). |

Backlog within the selected design:

| Area | Remaining work |
| --- | --- |
| Restore performance | Measured on Azure ([Measurements.md](Measurements.md)): 100 GiB cold in 4.4 minutes, warm in 100 s; 30 GiB with production file sizes cold in 83–156 s. Warm starts hash only files used by the selected recovery closure; no trusted-cache receipt is needed. Qualify the production workload separately. |
| Graceful shutdown | Same-generation lease handoff on shutdown instead of waiting for lease expiry, if the pause proves significant. |
| Providers | S3 adapter research (ObjectStorages.md); production Kestrel/proxy qualification of the HTTP transport. |
| Stronger reads | Confirmed-only, bounded-lag or minimum-position reads are optional future APIs, not requirements. |

## Design history

| Date | Decision |
| --- | --- |
| 2026-08-30 | Azure-first cluster-wide leadership with leased, CAS-selected `leader.json`; leader-centered follower sessions; injected ports. |
| 2026-09-06 | One publisher per database, one transition engine, one follower acceptance path. |
| 2026-09-07 | Database names inline in `leader.json`; genesis starts empty at the predecessor event ID; removed names never reused and old nodes continue alone (no retirement freeze). |
| 2026-09-07 | Follower mismatch restarts and rebuilds; no retained historical roots or live suffix repair. |
| 2026-09-12 | Use native virtual batching; every event keeps its own TRL transaction, so no normalization or reframing. |
| 2026-09-14 | Canonical TRL CAS is the durability point; no per-batch state object. The original successor-first publication was superseded by shared native IDs on 2026-09-28. |
| 2026-09-14 | Native KVI written last is the checkpoint; no manifest or pointer. Only a published KVI permits remote deletion. |
| 2026-09-14 | Unchanged `CommitUlong` identifies non-application commits; rollbacks are kept in TRL and compared; writers use asynchronous `StartWritingTransaction`, reads `StartReadOnlyTransaction`. |
| 2026-09-14 | Genesis and schema writes run only under leader authority and publish immediately; commits stay local. Schema transactions detach live followers. |
| 2026-09-14 | Odd replication TRL IDs, even IDs for other files; legacy files accepted unchanged. |
| 2026-09-14 | Short-lived confirmation grants (variant A); independent local compaction without KVI; leader-only remote compaction with remapped PVL IDs. |
| 2026-09-14 | Reuse verified local files by identity, length and SHA-256; always redownload the growing tail. Local disk is scarcer than remote space. |
| 2026-09-17 | ObjectDB registers all relations at startup: read-only check, then at most one schema writer. |
| 2026-09-22 | Delayed deletion by per-object deadline metadata; KVI recovery-root hints (removed 2026-09-28). Separate activation, publication and maintenance watchdogs; application owns event timeouts. |
| 2026-09-27 | Poll-based binary peer protocol: one request per follower step carries the grant and all databases; inline TRL bytes with polls (previously deferred as an optimization). |
| 2026-09-27 | The leader announces its latest non-application commit in every poll; followers no longer decode leader TRL (the scanner cost about 5.8 s per 23 MB of small transactions). |
| 2026-09-27 | Deletion delay must be positive; unchanged checkpoints are not re-exported; unmarked TRLs and PVLs are never rewritten by checkpoint protection. |
| 2026-09-27 | A steady-state checkpoint needs no request per retained file (measured 173 HEADs before). Removed and detached databases release local TRL retention; a node that initialized a database follows after losing the lease instead of restarting. |
| 2026-09-28 | Shared `{id}.trl` names across terms; conditional create with byte comparison on collision. Publish in native order and regenerate unfinished transactions after restart. Only SHA-256 and deletion deadlines remain Blob metadata; this supersedes successor links and KVI recovery-root hints. Sealed TRLs record their SHA-256 in the sealing write, so warm restores reuse them. |
| 2026-09-30 | Terminate an unfinished published transaction with a rollback in the successor TRL instead of regenerating it in place; a divergent unfinished suffix no longer blocks takeover. |

Superseded alternatives, kept only as reasons: manifest- or chunk-based checkpoint publication and whole-file TRL
replacement (native KVI-last and conditional tail append are cheaper and portable); a per-batch `state.json` CAS
(the TRL CAS already selects history); transaction-boundary-only rotation (large transactions need to span files);
leader-distributed compaction and follower-produced KVIs (each node's physical layout is independent); per-event marker
commands for batching (native batching already preserves per-event transactions); a server-streaming session with
follower status reports (a stateless poll carries the same information with less state); and follower-side decoding
of leader TRL for schema detection (the leader already classifies its commits).
