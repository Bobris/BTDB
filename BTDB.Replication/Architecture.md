# BTDB.Replication Architecture

Current capture rule (2026-09-16): retain only completed and acknowledged (fileId, offset) positions, with no event ID
or per-transaction side index. Publish/compare contiguous native prefixes to a fixed complete position. Older capture
queue, index, sequence and wakeup sketches are superseded; native TRL files themselves retain unpublished bytes.


Current comparison rule (2026-09-16): compare corresponding native TRL byte ranges directly in bounded chunks.
Older structural/decoded-command comparison sketches below are superseded. Capture supplies local transaction
boundaries; the publisher does not parse its own transactions again. Native command decoding belongs to replay.


Current local-file rule (2026-09-15): stopping publication leaves ordinary local writes and compaction running.
Earlier volatile/scratch-generation sketches below are superseded: no scratch file collection, `.temptrl` requirement,
write barrier or special cleanup. Startup validates the ordinary local cache against selected Blob history.


Status: Replication protocol remains an architecture proposal. Opt-in core preparation APIs are implemented; see
[core preparation](../Doc/ReplicationCore.md). The [M0 test foundation](Testing.md) is implemented; no distributed
replication runtime is implemented. Normative requirements below describe the intended
version one, not verified runtime guarantees. Implementation blockers are listed under [Open decisions](#open-decisions).

Provider research snapshot: 2026-08-29. Architecture consolidated on 2026-09-06. The provider contract,
`denoland/celld` study, and Azure/S3 details are maintained in [ObjectStorages.md](ObjectStorages.md).

## Problem statement

`BTDB.Replication` runs one or more logical `BTreeKeyValueDB` databases on disposable compute nodes. One cluster-wide
leader authors their canonical histories. Application durability/replay is supplied by the application's ordered event
log (Kafka is one example). Blob KVI/TRL is the shared recovery base that avoids replaying the entire input history.
Losing an unpublished BTDB tail is expected: obtain valid recent bytes from the leader with a short timeout, reuse an
eligible validated optimistic suffix, or apply the retained inputs again. No additional application durability
acknowledgement, Blob wait or end-to-end delivery protocol is required from BTDB.Replication.

In this document, a Blob-durable/published boundary means only the complete BTDB history already available in object
storage. It is an internal restore/publication watermark, not an application acceptance requirement. Retention of input
needed after that boundary belongs to the application's replay contract. Keep native KVI/TRL completeness,
branch selection and lease/CAS fencing checks because replay must start from a valid base and the correct input cursor.

Followers synchronously execute the same events into local speculation without external I/O on the commit path.
All nodes consume the same ordered events and write local BTDB files. Virtual transaction batching preserves the same
TRL transaction payloads for identical execution; only the leader publishes canonical Azure Blob Storage history.
A match advances confirmation metadata without BTree work; a structural mismatch restarts the follower and rebuilds it
from canonical storage. Replication retains no historical BTree roots for confirmation or repair. Compaction is an optional physical optimization outside TRL.

A `Deployment` with ephemeral disks and a `StatefulSet` with stable disks use the same protocol. Node count, pod ordinals,
PVCs, and local file survival do not establish authority. One replica uses the same durable format as many replicas.

### How to use this specification

Decision recorded 2026-09-14: implement only mechanisms whose necessity has been established. A proposed field,
message, state, persisted record or optimization needs a concrete failure of the simpler design or evidence of a
substantial performance impact. Prefer a reproducible deterministic counterexample for correctness and representative
measurements for performance. Record the short argument and supporting test/measurement with the decision; a formal
proof document is not required for every implementation detail.

The agreed external behavior and safety invariants remain requirements. Detailed mechanism/API sketches and backlog
entries below are candidates, not unconditional implementation requirements merely because they were written down.
Start from the smallest working path, derive existing information instead of duplicating it, and defer additions
without evidence. Simplifications must preserve the guarantees and be reflected in the owning section. For example,
peer progress needs only the committed event/file/position tuple; internal Blob publication bookkeeping does not
justify sending a separate durability classification to followers.

The following sections own the rules; other sections explain their application and must not define competing algorithms:

| Concern | Normative owner |
| --- | --- |
| Safety and progress identities | [Target safety properties](#target-safety-properties), [Shared identities](#shared-identities) |
| Bootstrap, takeover, and upgrade | [Cluster transition engine](#cluster-transition-engine) |
| Every canonical TRL publication | [TRL publisher](#trl-publisher) |
| Live follower acceptance and repair | [Progress notification and comparison](#progress-notification-and-pull-based-comparison) |
| Opening or rebuilding a database | [Per-database replay hierarchy](#per-database-replay-hierarchy), [Restart or new replica](#restart-or-new-replica) |
| Physical maintenance and file lifetime | [Local and remote compaction](#independent-local-compaction-and-leader-only-remote-compaction) |
| Provider behavior | [ObjectStorages.md](ObjectStorages.md) |

DTOs are semantic sketches using the shared types below, not final public APIs or wire layouts. Historical alternatives
are collected at the end; they do not reopen a selected version-one decision. An unresolved safety mechanism is an
implementation blocker, even when its desired outcome is already an invariant.

## Initial cluster and database model

Decision recorded 2026-08-30:

- A **failover cluster** is one group of nodes sharing one Azure container, leader record, leadership term, and
  leader-centered follower sessions.
- One process may host multiple `BTreeKeyValueDB` instances. Each database has a short, path-safe name unique inside
  the cluster, such as `main`, `users`, or `jobs`; the exact length and character validation remains to be fixed.
- Leader election is cluster-wide, not per database. The elected node alone authors canonical TRL and runs remote
  Blob compaction. Every node may independently run full local physical compaction without creating a KVI or sending
  its results to other nodes.
- One ordered application-event stream is shared by the whole cluster and all replicas. Database event cursors refer
  to this same stream. Canonical transaction sequence, TRL lineage/offset, KVI, checkpoint, and replay progress remain
  per database; databases may consume the shared input at different speeds and with different transaction boundaries.
- Every application build declares a monotonic application generation and a database-name set. Nodes may
  temporarily host different database sets during a rolling upgrade. The database set selected by the leader record defines
  which database instances are canonical; a newer prepared follower has handoff priority over same-generation nodes.
- Once a newer application generation has been selected by a successful leader-record CAS, that generation is a
  persistent election floor. Older nodes may keep following compatible database instances, but they can never become
  cluster leader again.

The shared leader record contains cluster authority, the active database-name array, and connection information. Per-database durable progress is
stored separately so database commits do not rewrite or create update contention on the election record.

## Rolling application upgrades and database-set transitions

Decision recorded 2026-08-31: a rolling application upgrade may add and remove logical databases. Different application
generations are allowed to overlap, but there is still exactly one cluster-wide leader and one leader-selected canonical
database set. A database-set difference is an intentional database set transition, not a reason to reject the newer
follower or to wait for every old replica to disappear.

### Database names and downgrade fence

Human-readable package versions are not ordered by the protocol. Every deployable application configuration supplies a
monotonic `applicationGeneration` and its database names. The leader JSON stores the complete active set directly:

```json
{
  "applicationGeneration": 12,
  "databaseNames": ["main", "users", "jobs"]
}
```

Names are unique, case-sensitive short database names; array order has no protocol meaning. There are no separate
catalog or catalog-transition objects, keys, or hashes. The leader-record CAS selects the generation and names together
with the new term and session. Increasing the generation is valid even when the names remain unchanged.

The application configuration maps each name to its `databaseInstanceId` and compatibility fingerprint;
`eventStreamId` is shared cluster-wide, not configured separately for each database;
published per-database metadata persists these identities. Equal generations must declare the same name set and identity mapping;
a conflict is a configuration safety fault. Database names are never reused after removal within the cluster.
Names alone must never authorize opening an old instance's history. A higher-generation candidate must support the
current protocol and the transition from the selected configuration.

### Per-database activation and restart

Selecting a generation and its names does not select an initial database snapshot. Creating a database is a
non-application write transaction and may run only under selected leader authority. The leader captures the current
application-input end and determines the first input to apply. Initialization sets `CommitUlong` to the previous
`eventId`, immediately preceding that first input under the application's ordering convention. The application supplies
this predecessor value; do not blindly subtract one from an opaque/non-contiguous event ID. The initialized empty
state contains no effects of earlier input.

Immediately after local commit, `CreateGenesis` schedules its initial TRL/file prerequisites and conditional TRL
publication without the lazy flush delay. No separate empty checkpoint is required. Local initialization and dependent
application work do not wait for Blob publication; remote genesis becomes durable only when its publication is proven. Input after the captured predecessor must remain
recoverable while publication is pending. Published initialization fixes the cursor; an unpublished retry may capture
a later input end after reconciling earlier publication attempts.

Genesis establishes canonical sequence zero and binds database instance, stream, initial `CommitUlong` and bytes into
its history identity. It is a non-application initialization transaction, not an applied/skipped event or a live schema
change to an already open database. Followers bootstrap it from canonical storage rather than detaching on genesis.

A successor resolves activation independently for each active database:

- No published state and no database files: initialize from scratch with an empty transaction and starting event cursor.
- No published state, but incomplete or unselected uploads: those files do not establish canonical history. Reconcile any
  ambiguous publication, then initialize using a fresh attempt namespace under the normal publication fencing rules.
- Published state: validate and restore its selected files and cursor, then adopt it. Never initialize over existing
  published history; missing or corrupt referenced files require recovery or unavailability, not silent data loss.

A crash after selecting the name list but before publishing any new database therefore needs no durable seed-selection
plan. A crash after activating some databases preserves those published databases and initializes only the others.
Publication ambiguity and competing attempts use the common publisher's conditional-write and fencing rules.

Removing a database requires no separate publication fence or final shared recovery boundary. A previously dispatched
old-leader write may still land in its abandoned namespace; that does not affect any active database. Names are never reused.

After the leader CAS succeeds, `applicationGeneration` is the cluster's election floor. A node below the floor is
permanently ineligible for leadership, even if every newer node is unavailable. This deliberately trades availability
for preventing an old binary from resurrecting a retired database or leading without a newly added one.

### Added database while the leader is older

An added database remains `awaitingInitialization` on a newer follower. Its initialization write waits inside BTDB
until that node obtains leader authority; the follower must not execute it provisionally or consume application inputs
against an uninitialized database. Existing compatible databases may continue application processing while it waits.

After leadership selection, the activating leader creates and immediately publishes each missing database through
`CreateGenesis`. If another leader has already published that instance, restore its state and re-evaluate the pending
request instead of repeating initialization. A follower may bootstrap the now-published database without authoring any
initialization transaction. No provisional writable copy or provisional read mode exists in the selected design.

Initial cluster bootstrap uses exactly the same per-database initialization rule with an empty predecessor database set.

### Removed database under a newer leader

A database present in the previous database set but absent from the newly selected active set is **retired**. Its name
is permanently out of use in this cluster. The new leader does not adopt, replicate, compact, or checkpoint that database
and does not wait for old nodes or in-flight writes to it. No exact final shared position, retirement-state publication,
or retirement CAS is required. A previously dispatched old-leader update may complete harmlessly in the abandoned
namespace. Active databases still require the ordinary authority and publication fencing.

An older node that still hosts the database is on its own until shutdown, which is expected soon. After observing the
new leader's database set, it stops accepting canonical frames and confirmation progress for that database and continues
from its own current local view; it need not rewind or synchronize to any common retirement boundary. At the next
complete transaction boundary, subsequent writes use a disposable local `.temptrl` generation. They do not publish,
checkpoint, hard-flush, acknowledge cluster durability, or become candidates for promotion. Existing local readers
retain their pins. Old nodes may diverge from one another without affecting the active cluster.

Scratch is deleted on clean close and before reopening after restart. There is no cluster availability or recovery
promise for a removed database and no requirement for the new leader to keep it recoverable for old binaries. Any
optional old-node recovery must validate available files and remains purely local. Deletion of abandoned remote data
is a separate retention/administrative policy, not a rollout prerequisite; this change does not authorize immediate GC.
The generation floor prevents the old application from becoming cluster leader again.

### Reopening followers from the selected new leader

Every node of the new application generation opens an added database from published initialization or a later canonical
checkpoint and tail. Pending creation is re-evaluated against that state; it never overwrites an already initialized DB. Later follower transactions use the normal
native-TRL comparison protocol.

An older follower that lacks a newly added database simply ignores that database's stream and remains non-ready for the
full new database set; it may still follow and serve compatible continued databases. For a database it still hosts but the
new database set retired, it uses the disposable behavior above. For continued databases, a former old leader discards any
temporary handoff suffix, closes and reopens against the new leader, and then behaves as an ordinary follower.

### Leader-only non-application write barrier

Decision recorded 2026-09-14: genesis and schema changes are non-application writing transactions. BTDB must wait for
selected leader authority before starting them, including when a newer application starts as a follower and opening
ObjectDB would otherwise add or remove secondary keys. No follower, detached/retired local generation or draining
session may execute such writes. The wait is cancellable and occurs before obtaining the database writer reservation
or writing any TRL bytes; it must not block lease coordination, follower restore, or ordinary compatible transactions.
After wakeup, revalidate authority and the current database/schema state so queued duplicate migrations are not rerun.

Immediately after local non-application commit, signal the serialized TRL publisher to flush without waiting for its
normal lazy timer or batch-size threshold. Publication is asynchronous: Commit returns on local completion and does
not wait for Blob, and dependent local application transactions may proceed. Publish required predecessor history in
order; a flush signal is neither a durability acknowledgement nor permission to bypass an unresolved earlier CAS.
Reconcile ambiguous results in the publisher, preserving already published history. Authority loss fences further
canonical publication. This replaces the earlier synchronous durable-completion/dependent-write barrier.

A follower may request the recent unpublished tail from the current leader with a short bounded timeout. Returned bytes
use the normal authority, lineage and transaction checks; they are not automatically Blob-durable. If unavailable,
recover unpublished work by applying the application's retained input again. Losing unpublished local work remains
allowed, including recreation of unpublished initialization/schema work through its normal lifecycle.

Genesis runs under `Activating` authority. Its local completion is required for local activation; its immediate
asynchronous publication is not a readiness wait. Published-existing bases still require completed adoption fencing. Continued
schemas run after base adoption/restoration under the selected leader; migrations needed to open ObjectDB must not be a
prerequisite for acquiring that authority. Follower-first eligibility validates/downloads the existing canonical bases
without executing new migrations. A queued migration or unpublished addition therefore cannot deadlock election waiting
for itself to finish. If activation needs a schema commit, allow that immediate publication only after every existing
active base has been restored/adopted, while ordinary application writing remains disabled until activation completes.

Ordinary application transactions retain local-only commit semantics on leaders and followers. Immediate remote
publication and leadership waiting apply specifically to non-application writes. Startup replay of already published
non-application TRL is recovery, not execution of a new write, and is allowed on a follower.

ObjectDB initialization first inspects state using a read-only transaction. Ordinary initialization/registration metadata
is retained in memory and written by the first application writer that performs an upsert. If a secondary-index upgrade
requires a separate non-application write, dispose the read transaction and wait for leadership in startup orchestration.
Then use `StartWritingTransaction()` for all required index reconciliation, at most the first writer after ObjectDB open,
before processing application events. There is no core admission gate or per-mutation authority check. Do not upgrade
the read transaction or retain its lock/root while awaiting leadership. Read-only restoration must still complete before
election without waiting for migrations. Current ObjectDB initialization uses `StartTransaction()` for its initial
metadata reads; migrating that call and other internal uses is future implementation work. Standalone APIs are unchanged.

### Non-application schema transactions during upgrades

Decision recorded 2026-09-14: a newer application version may create a non-application schema transaction, for example
adding or removing secondary keys/indexes. Under the leader-only write barrier, the selected leader publishes an ordinary committed BTDB
transaction in canonical TRL. It uses existing mutation and commit commands, not a new `KVCommandType`. It is an explicit
exception to identical application-transaction execution on all currently running nodes, not a physical compaction operation.

A non-application committed transaction leaves `CommitUlong` unchanged. Application commits must change
`CommitUlong`, including commits with no application mutations and explicit skips. The first transaction creating the
DB is the exception: it is non-application genesis and sets the predecessor input cursor. Recognize genesis from the
creation context, not the ordinary unchanged-value rule. Decode the transaction terminator first: a rollback is not a
committed non-application transaction and must never trigger schema detachment.

Schema commits advance internal canonical sequence/history identity without advancing the input cursor. Derive the
transaction kind by decoding fetched native TRL; no separate wire kind is sent; no reserved Ulong slots, schema-kind
sidecar, or new KV command is required. This rule compares committed before/after values, not merely presence or absence
of a metadata command. Application/schema compatibility remains the application's concern and the native ObjectDB
schema's contract, not an additional replication discriminator.

When an already running follower receives a valid schema transaction for its database, it detaches that local database
from canonical following before applying or confirming the transaction. This includes a transaction decoded from a
published TRL during live catch-up. The decision does not wait for local speculative event coverage to reach the
schema boundary. Validate source authority, database/lineage and transaction identity before allowing a message to trigger
detachment. Duplicates already covered by the installed bootstrap history are harmless duplicates, not a new boundary.

At its next safe local transaction boundary, the follower switches subsequent writes to disposable `.temptrl` using
the existing volatile core path. It continues independently from its own local view, including any speculative head,
and may keep serving application-local reads. It neither applies the schema transaction nor accepts later canonical
frames for that database; it reports `schemaDetachedVolatile`, not confirmed/canonical readiness.
This is an intentional upgrade transition, not structural divergence requiring an immediate restart. The old application
will eventually shut down and be replaced. No common frozen event position or reader rollback is required.

The detached database remains active in the cluster; its name is not retired and the leader continues publishing it.
Other compatible databases on that process may keep following. Once schema detachment occurs, that node session is
permanently ineligible to contend for leadership: no lease-acquisition attempt, candidate registration, or acceptance
of planned leadership handoff. Do not wait until after acquiring a lease to reject it. A timeout, reconnection, or later
database-set change cannot clear this prohibition. The detached generation is never promoted or used for tail adoption.

If the detached node cannot establish the presence of a current valid leader continuously for 15 minutes, request a
graceful process restart through the injected host lifecycle port. Use the injected monotonic clock; reset the absence
interval only on valid current-leader evidence under the normal authority rules, never stale frames or a cached record.
At detachment, start this interval if no valid leader is observable; otherwise start it when that evidence is lost.
Leader monitoring does not resume canonical acceptance for the detached database. The 15-minute timeout triggers a
restart request, not a guaranteed exit deadline: finish current application work at a safe transaction boundary and
close readers/writers normally. Do not publish or promote the volatile suffix during shutdown, and do not convert this
policy into an automatic fail-fast timeout. An observed leader permits continued independent volatile execution.

On restart, discard scratch and restore through the normal verified-cache/Blob path. Only a new compatible session
that fully restores all required databases and satisfies the generation floor may become eligible again. An incompatible
binary remains ineligible. No live suffix repair or later KVI push is allowed. Ordinary transport disconnection without
schema detachment still follows normal failover rules and does not disqualify an otherwise eligible follower.

A replacement starts through the normal follower-first Blob restore. A compatible version replays the schema transaction
from TRL before resuming application transactions, or starts from a selected checkpoint that already includes it. Startup
replay does not take the live-follower detachment branch. An incompatible binary cannot serve the restored schema or
reverse it by publishing its old index definition. The application and native ObjectDB schema determine whether the restored state is usable; a checkpoint need not
retain a separate classification record for every earlier schema transaction.

Already published schema history is preserved across failover. Unpublished schema work has the same durability limits
as other leader-local history; the compatible application's migration lifecycle must recreate missing schema work before
subsequent dependent application transactions. An optimistic tail based on a pre-change schema cannot be grafted onto
post-change history merely because the event cursor is equal. Migration lifecycle integration remains Q2/Q3; no additional persistent transaction-kind metadata is selected.

### Upgrade handoff readiness and selection

An old leader treats a connected higher-generation follower as an upgrade target when:

- its protocol range and every continued database compatibility fingerprint are accepted;
- every continued database is within the configured handoff lag and the target retains the required replay inputs;
- every database added by the target database set can initialize from the input end captured at initialization and consume the required inputs;
- databases removed by the target database set are explicitly accounted for in the transition, not accidentally missing;
- its candidate endpoint and proposed lease-transfer ID are usable.

The target does not need to host databases its own database set retires. As soon as such a follower is prepared, the leader
voluntarily starts a planned upgrade handoff; it does not wait for process shutdown. If several compatible newer
generations are prepared, select the highest observed generation and randomize among equivalent targets. During the
transfer the old leader uses the same transaction-boundary persistent-to-`.temptrl` cut and authority-only lease lane as
graceful shutdown. After the new leader is ready, the old process may remain alive: it discards temporary suffixes and
reopens continued databases as a follower, while every retired database remains in disposable `.temptrl` mode. No
volatile byte is transferred or promoted.

## Provisional non-goals

- Arbitrary logical mutations outside ordered application transactions or explicit leader-only schema transactions. Local compaction may change only physical
  value references while preserving logical contents.
- Multi-primary conflict resolution or merging independently ordered histories.
- Providing event ordering, consensus, or retention; the upstream log owns those guarantees.
- Treating loss of an HTTP leader session as proof that the previous leader lost authority.
- A peer-to-peer membership, discovery, or failure-detection mesh between followers.
- A distributed transaction spanning multiple logical BTDB databases.
- Treating an object store as a low-latency random-access filesystem.
- Allowing followers to publish independently pointer-rewritten roots or independently authored TRL bytes. Deleting
  completely unused files from a follower's disposable local cache is explicitly allowed and is not canonical
  compaction.
- Byzantine fault tolerance. A compromised current leader is outside the first design, although equivocation should
  be detected and cause a safety stop.

## Terminology

- **Database**: one logical BTDB database and its complete history.
- **Database name**: the short, path-safe human identifier of one database inside the failover cluster. Together with
  the instance ID it is used in object keys, transport routes, metrics, and follower progress vectors.
- **Database instance ID**: the stable identity of a database within the cluster. Database names are never reused after
  removal; the instance ID remains part of existing file and protocol identity checks.
- **Application generation**: an application-supplied strictly increasing rollout generation, independent of a display
  version. The generation selected by the leader record is a persistent lower bound for future leaders.
- **Database set**: the active names in the leader record's `databaseNames` array, selected together with an
  application generation. Instance identities and lifecycle metadata live in published per-database metadata.
- **Database awaiting initialization**: an unpublished addition whose non-application creation write waits for leader
  authority. It exposes no usable local database or provisional reads; a published instance is restored instead.
- **Retired database**: an instance removed from the active leader-selected database set. It has no required final shared
  boundary; older nodes continue independently through disposable local `.temptrl` writes until shutdown.
- **Failover cluster**: the nodes and named databases governed by one shared leader record and leadership term.
- **Node**: one independently scoped failover runtime capable of hosting the database. Production normally hosts one
  node per process, while deterministic tests may host many isolated nodes in one process.
- **Node ID**: a unique node identity used for session correlation and observability. A disposable replica may receive
  a new node ID after replacement; it is never a fencing token.
- **Peer endpoint**: an opaque transport-defined address used to reach one node. Only a transport adapter interprets
  it; it is not an authority or fencing token. A follower advertises the endpoint it would publish if it became leader,
  but other followers do not connect to it.
- **Session**: one activation lifetime of a node runtime, identified by a random unique ID. A restarted or recreated
  runtime is a new session even in the same process or on the same StatefulSet ordinal.
- **Follower connection**: one authenticated, leader-centered logical transport session. It has its own random
  connection ID and monotonically increasing status sequence so delayed messages from an older connection can be
  rejected.
- **Connected-follower registry**: ephemeral leader-local state derived from current follower connections and status
  reports. It helps direct distribution, observability, and graceful-handoff selection, but is neither durable nor an
  authority source.
- **Event position**: a totally ordered cursor identifying the last consumed upstream event, including an explicitly
  skipped individual failure, at a committed transaction boundary.
- **Canonical transaction sequence**: a contiguous number assigned by the leader to each committed application or schema
  transaction. Schema commits leave the input cursor unchanged; virtual batching does not combine log transactions.
  Retried failed attempts consume no sequence, while a singleton metadata-only skip consumes one.
- **Confirmed position**: metadata describing the latest contiguous leader event range whose decoded TRL structure
  matches local execution. It is not a pinned BTree root or a separately readable historical snapshot.
- **Speculative head**: the follower's single current writable BTree state, possibly ahead of confirmation. Replication
  does not retain its previous roots; ordinary open read transactions retain their normal BTDB snapshots.
- **Speculation generation**: the identity of one disposable local execution generation. Restart/rebuild abandons it;
  local entries never use reused BTDB `TransactionId` values as globally unique identities.
- **Speculative transaction entry**: a disk-indexed event boundary and local TRL range used for structural comparison.
  It contains no retained resulting-root handle.
- **Local speculative TRL cache**: follower-produced bytes and bounded comparison metadata. Matching transaction payloads does not grant local files independent publication authority. Canonical bytes are stored separately.
- **Shutdown canonical cut**: one database's last complete canonical transaction accepted before a graceful-shutdown
  leader switches that database away from persistent canonical writing. Its durable recovery base is the newest
  canonical-TRL-derived boundary that was already committed, or whose previously dispatched CAS is later reconciled as
  committed; shutdown does not force a final data publication. The **shutdown cut vector** carries this pair of positions
  for every database active at that shutdown or transition cut.
- **Volatile shutdown generation**: the disposable process-local BTree head and local `.temptrl` scratch files created
  after a stepping-down leader's shutdown canonical cut. Application transactions continue normally and newly written
  values remain readable, but the generation is never hard-flushed, uploaded, checkpointed, replayed after restart, or
  treated as canonical. Its files are deleted on clean exit or before the next database open. The ordered event log is the
  successor's recovery source.
- **Volatile database-set transition generation**: a disposable `.temptrl` generation used either by a stepping-down old
  leader while an application upgrade transfers authority or by an old follower for a database retired by the selected
  database set. It obeys the same no-flush, no-upload, no-promotion rules as a volatile shutdown generation. A continued
  database may leave this mode only by discarding the complete volatile generation and reopening from the new leader;
  a retired database remains volatile for the rest of that local open lifetime.
- **Application transaction**: an application-owned BTDB transaction attempt with an ordinary commit or rollback; an
  explicitly skipped input is recorded by a separate metadata-only commit. All replicas may batch memory-root publication independently of these log boundaries.
- **Skipped event**: an individually failed application event whose failed attempt was rolled back and whose cursor is
  advanced by a fresh ordinary metadata-only transaction. It is an explicit consumed outcome, never an input gap.
- **Leak-removal publication request**: a non-authoritative, bounded exact-key candidate discovered against a pinned
  accepted root and submitted through an injected parent-system port. It never mutates BTDB directly. The parent system
  deduplicates and publishes an ordinary ordered application event containing the exact removal command.
- **Leak-removal event**: an ordinary application event whose normal handler removes the encoded exact keys
  idempotently in the same transaction that stores its event position. It has no special TRL encoding or replication
  path.
- **Local compaction operation**: a node-private bounded physical rewrite under its database writer gate. It preserves
  logical state, emits no TRL or peer message and consumes no canonical sequence or event position.
- **Bootstrap KVI**: the one canonical KVI obtained from a selected checkpoint when a follower opens or rebuilds a
  database. It establishes that local open's initial physical root. An already running follower never receives a later
  KVI as a live update; a subsequent restart or rebuild may select a newer checkpoint and use its KVI as the new open's
  bootstrap artifact.
- **Remote compaction KVI**: a native KVI serialized for the Blob file inventory and destination file identities,
  possibly from the same local compaction pass. PVL references resolve in the remote layout; TRL IDs and offsets remain unchanged.
  It is published after all prerequisites and is transferred only as a later open/rebuild's bootstrap artifact.
- **Node-local compaction**: maintenance of the disposable node file inventory without creating a KVI. It preserves
  current roots, reader pins and in-process replay dependencies; restart uses the selected remote recovery closure.
  Full local rewrites are allowed on leaders and followers alike, with independent local file layouts.
- **Replica**: one local `BTreeKeyValueDB`, cache of canonical TRL/PVL and restored checkpoint files, event consumer, and
  failover coordinator. Local maintenance creates no KVI; a remote KVI is obtained only for open/rebuild.
- **Canonical TRL**: the byte stream whose identity is authored only by the current leader. Followers may mirror the
  received bytes or promote byte-identical locally cached transaction bytes after exact comparison, but they never
  publish unmatched speculative output as canonical.
- **TRL progress notification**: the latest complete `(eventId, trlFileId, trlPosition)` boundary in an established
  database/authority session. It carries no transaction bytes; the follower fetches native TRL ranges for comparison.
- **Decoded canonical transaction**: an internal validated transaction reconstructed from fetched native TRL, including
  its ordering, cursor and ancestry evidence. It is not a separately pushed HTTP transaction envelope.
- **Checkpoint**: shorthand for a published native KVI and the BTDB files needed to restore from it; it is not a
  separate object, manifest or pointer. The KVI is written successfully only after its prerequisites are available.
- **Leader**: the session currently named by the leased and CAS-protected leader record. While write-ready, it leads every
  database in the failover cluster, authors canonical TRL bytes, publishes checkpoints, runs remote compaction and
  owns remote garbage collection. Followers execute application code speculatively, serve local snapshots and run
  full local compaction independently.
- **Stepping-down leader**: the still-named lease holder after its irreversible shutdown switch. It keeps authority only
  long enough to fence and transfer leadership, performs no canonical or database-persistence work, and executes later
  application transactions only against its volatile shutdown generation.
- **Leadership term**: a monotonically increasing cluster-wide fencing generation published by the lease holder's
  successful leader-record CAS.
- **Leader Blob lease**: the finite Azure Blob lease held on the shared leader record. Holding its lease ID is necessary
  but not sufficient for leadership; the record must also name the same term and session.
- **Leader record**: the small mutable Azure blob that names the current cluster leader, transport-defined peer endpoint,
  and API key. Its ETag is the CAS token; its native Blob lease provides server-enforced liveness and fast planned
  transfer.
- **Published database view**: the verified in-memory view of canonical TRL publication and checkpoint metadata.
  It is derived from stored objects and is not a separate per-batch commit document.
- **Durable TRL boundary**: the canonical-TRL-derived recovery cursor immediately after a complete canonical
  transaction. It is derived from the CAS-published canonical TRL chain and its exact final `(fileId, offset)`, sequence
  and frame-chain hash. It is not selected by a separate mutable state object.
- **TRL transaction range**: the single contiguous byte range containing one entire transaction in one physical TRL
  file, through its commit or rollback terminator. A transaction never continues in another file.
- **Conditional TRL append**: a semantic storage operation that makes an exact byte suffix visible only if the remote
  file still has the opaque version and length previously observed by the caller. The architecture does not expose how
  a provider realizes this operation.
- **Local working copy**: the ordinary BTDB files used by the in-process `BTreeKeyValueDB`. It is an untrusted,
  disposable cache until its complete file graph and boundary identity have been validated.

## Target safety properties

These invariant IDs are the common vocabulary for transition validation, adapter conformance, and fault tests.

| ID | Invariant |
| --- | --- |
| I1 Authority | Only the lease-owning, leader-record-selected session may author canonical work. Ordinary application writing waits for all active databases to activate; activation may publish required genesis/schema transactions after existing bases are restored/adopted. Loss of a peer session never grants authority. Stale authority stops canonical acceptance and publication. |
| I2 Ordered history | Initialization establishes sequence zero with an empty transaction and starting cursor. Thereafter each event transaction atomically stores its mutations and cursor; all replicas may batch independently; each event has an ordinary committed TRL transaction; virtual batching only defers memory-root publication. Only the failed event transaction rolls back; earlier virtual-batch commits survive; rollback outcomes must agree across nodes; only an explicit application skip becomes a metadata-only commit. Canonical sequence advances per committed application or schema transaction, independently of memory-batch publication. Explicit schema transactions leave the input cursor unchanged and detach live followers under the upgrade rules. No new BTDB command kind exists; compaction and term changes consume no application position. |
| I3 Confirmation | Compare identical transaction payloads over matching event coverage; virtual batching does not change framing. A match advances metadata only; a structural mismatch fences reads/commits and restarts the follower for canonical rebuild. No historical confirmation roots or live suffix repair. |
| I4 Local execution | Ordinary application commits perform local synchronous work and never wait for leader progress, external I/O, or a fixed speculation window. Non-application writes wait before starting for leader authority and trigger immediate asynchronous canonical TRL publication without delaying local completion. Disk exhaustion fails the node and triggers validated-cache recovery; it cannot promote speculative data. Comparison indexes and pending TRL may spill to disk; no replication-owned historical roots accumulate. |
| I5 Durable closure | Canonical TRL CAS directly publishes complete transactions; a transaction may span TRL files, but durability requires its complete reachable recovery closure. Physical length or listing alone never proves a committed transaction; published KVIs are discovered from native files. The checkpoint belongs to the selected ancestry and cannot be ahead of its durable boundary. |
| I6 Publication fence | A serialized publisher validates canonical TRL publication. Adoption on the TRL excludes pending predecessor ETags; ambiguity is reconciled without assuming that cancellation or a read of old state drained an outstanding write. |
| I7 Database set | Generation never decreases and equal generations require equal name sets and configured instance identities. Published initialization fixes each added database's initial history; unpublished additions may restart from scratch. Removed names are never reused; old nodes continue independently and delayed writes to abandoned state are harmless. Unpublished additions wait for leader-only initialization; no provisional writes. |
| I8 Recovery | After old readers terminate, restart reuses only sealed files matching the selected Blob identity, length and whole-file checksum; the last growing TRL is always downloaded again, as are missing/mismatching files. Across terms, copy only validated complete optimistic events beyond the adopted canonical end; regenerate unavailable events from retained input. |
| I9 Reader lifetime | Every live root retains its original readable bytes, including abandoned speculation and optimistic readers. Local deletion depends only on that node's complete pins and recovery cut; remote deletion protects the currently published recovery closure, not in-progress follower restores; superseded files become eligible after replacement publication; deletion may be delayed operationally and affected restores restart. |
| I10 Maintenance | Every node may compact its local files without creating KVI. Only the leader compacts/publishes remote files and remapped KVI. Neither operation distributes physical rewrites, results or local deletion commands to peers. Leak removal enters only as a parent-published ordinary event. |
| I11 Volatile execution | Shutdown/retirement/schema detachment switches at a transaction boundary to readable `.temptrl` scratch. It never publishes, hard-flushes, checkpoints, acknowledges durable progress, or promotes that suffix. Startup removes scratch before constructing canonical files; cleanup failure prevents opening. |
| I12 Isolation | Protocol state and every external/nondeterministic boundary are injected and node-scoped. Standalone BTDB format, synchronous semantics, and hot-path cost remain unchanged when replication is disabled. |

Recoverability requires a valid checkpoint, its selected closed canonical tail, and every necessary ordered event after
that durable boundary. Version one does not retain old remote checkpoint closures just for fallback or slow followers. Retention must cover publication
outages and recovery delay; a checkpoint-age metric alone does not establish that coverage. Missing required input is
an explicit unrecoverable gap, never permission to skip an event.

### Shared identities

Use these semantic types in storage records, frames, progress reports, and resume/read tokens:

```text
DatabaseIdentity
    clusterId / databaseName / databaseInstanceId

AuthorityIdentity
    clusterId / leadershipTerm / leaderSessionId
    applicationGeneration

EventRange
    eventStreamId                   // the same cluster-wide stream identity for every database
    afterEventPosition / lastEventPosition
    eventCount / orderedEventIdentityHash

CanonicalPosition
    database: DatabaseIdentity
    eventStreamId                   // the same cluster-wide stream identity for every database
    canonicalSequence / eventPosition / frameChainHash

TrlCursor
    lineageId / fileId / offset

RecoveryBoundary
    position: CanonicalPosition
    trlObjectKey / publishedPrefixHash
    end: TrlCursor

CheckpointReference
    position: CanonicalPosition
    kviObjectKey / kviHash
    physicalLayoutIdentity

ResumeToken
    authority: AuthorityIdentity
    accepted: CanonicalPosition
    cursor: TrlCursor
    canonicalAllocationWatermark
```

A canonical position names application history, not permission to extend it and not a cryptographic audit of the whole
BTree. Authority is validated separately. A term adoption preserves its predecessor position while changing authority
and opening a new TRL lineage. The published TRL continuation proves that connection; implementations must not synthesize ancestry
from equal integers. The published initial empty transaction establishes sequence zero and its unique genesis frame-chain identity.

Leader-record revision/ETag is observation and CAS metadata, not part of authority identity. A same-term skip-list
update must not invalidate database adoption, existing frames, or resume ancestry. Observers reconcile newer control
metadata within the same authority; they still validate lease freshness under B1. A revision change neither renews
authority nor permits an older observation to overwrite newer skip decisions.

Positions are ordered only within the same database/stream and proven ancestry. A new term can restart from an older
durable position; sequence or event position alone does not compare two branches. A read token always includes stream
and database identity. `physicalLayoutIdentity` describes a selected remote checkpoint, not a requirement that local
layouts match. Local compaction results/identities are not reported through resume tokens or follower status.

Every progress surface distinguishes `confirmed: CanonicalPosition`, `durable: RecoveryBoundary`, and a local speculative
cursor `(nodeSession, speculationGeneration, eventStreamId, eventPosition)`. Pending-initialization, retired-volatile and schema-detached progress
also has no canonical position. A resume token's authority must be refreshed after takeover; a retained historical
position can still be the new term's proven base. Peer-supplied deadlines are not authority evidence. `EventRange` denotes contiguous ordered coverage after the base
cursor through the result cursor, not numeric cursor subtraction. Decoded application transactions and TRL-bound recovery metadata bind the same range,
count, and outcome into their hashes; the decoded transaction's stored cursor must equal the range end. Coverage is
verified against input when available and against published TRL ancestry during durable replay. Schema transactions
instead bind their explicit kind and compatibility metadata, preserve the input cursor, and advance canonical sequence.

Repeated scalar names in explanatory prose are projections of these types. A wire codec may encode shared context once,
but decoding must reconstruct and validate the complete identities. Their exact encoding and the independent logical
audit/physical-layout hashes remain open; each message must not invent another position tuple.

## Testability and ports/adapters boundary

Decision recorded 2026-08-30: every correctness-relevant part of `BTDB.Replication` must run without a real network,
Kestrel listener, cloud SDK, wall clock, or second process. The protocol is a set of deterministic state machines wired
to explicit injected ports. HTTP, Azure, process hosting, and other technologies are adapters at the edge, never types
or control flow embedded in the core.

The required boundaries include at least:

- object-store coordination, conditional TRL append, range reads, checkpoint transfer, leader-only remote garbage
  collection, and leader leases;
- the application generation/database set provider, compatibility decisions, pending database initialization, and current-input-end capture for initialization;
- leader-centered follower sessions, frame/control streaming, follower status reports, artifact reads, handoff responses,
  and their authentication context;
- the ordered application-event source, explicit seek/acknowledgement operations, and a parent-system publication port
  for requesting an ordinary bounded leak-removal event;
- monotonic and wall-clock time, delays, deadlines, and timer scheduling;
- randomized election backoff, target selection, node/session IDs, and operation IDs;
- local BTDB file collections and cleanup, checkpoint destinations, and process-lifecycle/crash notifications;
- observability sinks, which may observe decisions but must never be required to make them.

The object-store adapter must not hand an unrestricted deletion capability to follower state machines. Remote GC is a
separate leader-authorized port whose in-memory implementation rejects follower and stale-term callers. Local follower
cleanup uses only that node's injected BTDB file collection and pin inventory, is testable without an object-store
mutation path, and has no peer-transport operation through which the leader could request deletion.

Names and API shapes remain open, but the peer boundary needs semantics equivalent to:

```text
PeerTransport.OpenLeaderSession(peerEndpoint, credentials, followerHello, resumeVector, cancellation)
PeerTransport.ReadArtifactRange(peerEndpoint, credentials, artifact, range, cancellation)
PeerTransport.ReportFollowerStatus(peerEndpoint, credentials, connectionId, status, cancellation)
PeerTransport.RespondToHandoff(peerEndpoint, credentials, connectionId, response, cancellation)

PeerEndpoint.HandleLeaderSession(requestContext, followerHello, resumeVector, cancellation)
PeerEndpoint.HandleArtifactRange(requestContext, artifact, range, cancellation)
PeerEndpoint.HandleFollowerStatus(requestContext, connectionId, status, cancellation)
PeerEndpoint.HandleHandoffResponse(requestContext, connectionId, response, cancellation)

ParentEventPublisher.RequestLeakRemovalEvent(databaseInstance, candidate, cancellation)
    -> accepted(publicationIdentity) | duplicate(existingIdentity) | rejected(reason) | ambiguous
```

`peerEndpoint` is an opaque value to the coordinator and is interpreted only by the selected transport adapter. The
version-one production adapter can encode an HTTP base URI containing the leader's routable IP address and port. An
in-process adapter can encode a test-local address and route it to another isolated node instance without opening a
socket.

The in-process path must not become a privileged shortcut. It uses the same versioned request/frame DTOs, canonical
binary codec, authentication decisions, cancellation, backpressure, resume, range, and typed failure semantics as the
HTTP adapter. It must be able to inject fragmentation, delay, reordering where allowed, disconnection, lost responses,
and bounded queues. HTTP status codes, headers, pipelines, and socket exceptions are translated at the adapter boundary
into transport-neutral outcomes before the core sees them.

A deterministic cluster harness creates `N` complete node runtimes in one process. Each node has a separate dependency
scope, node/session identity, BTDB file collection, current trees and confirmed metadata, timers, and local failure state. The harness supplies
shared but controllable in-memory object storage and event logs, independently switchable follower-to-leader paths,
virtual time, seeded randomness, and an explicit scheduler that can run until quiescence. Tests use no real sleeps,
ports, DNS, static
singletons, or ambient process identity.

Every port must have a reusable conformance suite. The in-memory adapter establishes exhaustive state-machine and
fault-injection coverage; the HTTP/Kestrel, Azure, and eventual broker adapters must pass the same semantic suites plus
their own real-provider integration tests. Multi-process tests remain valuable for serialization, hosting, proxy, and
deployment mistakes, but safety must not depend on them being the only way to exercise multiple nodes.

## Event-log contract and deterministic replay

Decision recorded 2026-09-14: the application is responsible for producing identical ordered transactions on every node
hosting the same database, including identical rollback attempts and outcomes. BTDB.Replication applies/captures local
transactions, compares their histories and publishes the leader's canonical files. It does not own a Kafka consumer,
command dispatcher, application handler execution, or external effects. Kafka is only an example of how an application
could provide ordered, recoverable input; it is not a required dependency or deployment component.

The application integration supplies transaction/input identities, ordered coverage, resume from a restored cursor,
and the current input end for initializing a new database. The existing shared stream/cursor types describe that
integration contract, not a requirement to use a particular broker. The application ensures the same inputs, mutations,
metadata, transaction boundaries, rollbacks and ordering on compatible replicas. It also handles nondeterminism,
duplicate delivery and recovery of unpublished work. A gap is not permission for replication to invent an outcome.

The application owns any command submission acknowledgement; local BTDB commit success is defined in
[Read-serving and command consistency](#read-serving-and-command-consistency).

Input needed beyond each published database cursor must remain recoverable through the application's mechanism.
A new database starts empty with `CommitUlong` set to the event ID preceding the first input to apply,
using the current-input-end initialization policy above. Replication reports restored
positions to the application; it neither defines broker acknowledgement semantics nor supplies an exactly-once external
side-effect protocol.

### Virtual transaction batching and application failures

Decision updated 2026-09-12: use the implemented `StartWritingTransaction(inBatch: true)` optimization. Each event still
has its own ordinary BTDB transaction and commit/cursor in TRL. A node may reuse a writable memory root across these
transactions and defer publishing it to readers. `FinishTransactionBatchAfterCurrentTransaction()` requests publication
at the next safe boundary; an ordinary non-batched writer also finishes the pending batch. Local batching choices do
not change the event transaction framing or require extra event-end commands.

Current source evidence: `CommitWritingTransaction()` writes ordinary commit bytes for every transaction, using
`WriteBatchUlongsDiff()` and `_batchCommitUlong` to track the last logged metadata separately from the published root.
`RevertWritingTransaction()` rolls back the failed transaction and can rebuild the already committed prefix from TRL.
Earlier successful transactions in a memory batch remain committed; do not retry all their handlers. The test
`TransactionBatchingTest.LogBytesAreIdenticalIncludingRollbackAndDecreasingMetadata` compares log payloads after skipping
file headers, including rollback and decreasing metadata. This verifies batching independence for that scenario, not
an implemented replication protocol or universal equality of node-specific file headers, allocation and configuration.

The intended replication contract is identical event TRL for identical ordered execution on compatible nodes. Independent
memory batching is a performance choice, not a cause of log divergence. Validate canonical lineage, transaction closure,
input coverage and actual log content. Genuine mutation/outcome differences still restart the follower for canonical
rebuild. File headers/identities and any failed-attempt bytes require explicit handling; do not mask real differences
under obsolete different-batch normalization rules.

The application opens and commits or rolls back ordinary transactions. It decides whether a failure means a rollback,
a retry, an explicit skipped input, or fatal process termination. Replication must not turn a thrown exception or an
ordinary rollback into a metadata-only skip. An explicit application skip may use the existing ordinary metadata-only
transaction representation; only that committed outcome advances the consumed cursor.

Rollback itself belongs to the identical execution contract: compare the ordered failed-attempt transaction content
and rollback outcome as well as successful commits. Identical final key/value state alone is insufficient. Rolled-back
mutations never enter the committed BTree, but their comparison evidence must not be silently discarded. They do not
advance the canonical committed sequence or consumed cursor. Preserve the ordinary TRL writes through each Rollback
terminator and compare them in order together with the next committed transaction (selected R4-B). Do not normalize
away failed operations or replace them with an attempt side channel. With no later commit, confirmation may remain
pending. A rollback that emits no bytes adds no synthetic record. B3 must cover cross-file attempts and virtual-batch
rollback paths so required comparison bytes are not lost while the committed BTree is restored.

Earlier committed transactions in a virtual memory batch survive rollback of a later transaction. ObjectDB/application
state restoration remains part of the integration proof. For failures such as out-of-memory, the application chooses
fail-fast and whether to request an exact-input skip marker; replication does not infer that policy. Marker persistence
is optional application-directed control work and cannot delay required termination indefinitely or bypass lease fencing.

### Per-event TRL boundaries and independent batching

Every event transaction has the same normal start/commit boundary regardless of memory batching. The previous proposal
for per-event `DeltaUlongs` markers inside a multi-event log transaction is superseded. The comparator does not strip
transaction boundaries or normalize different log batches to accommodate `inBatch`; it compares the common event log.
A match advances confirmation metadata without retaining historical replication roots. A genuine mismatch fences and
restarts the follower. Ordinary readers retain their own BTDB snapshots; their visible root may lag already logged commits
until the memory batch is published, and replication/readiness must not confuse log progress with read visibility.

Optimistic TRL stays separate from the canonical cache. During a live takeover, only validated complete transactions
strictly after the reconciled canonical end can be reused; a local memory batch crossing that end needs no new TRL
framing or delta rebasing. Preserve ordinary append fencing and retry deduplication. A restart instead discards local
cache and restores from Blob Storage, regenerating any missing unpublished tail from retained ordered events.
The input/replay mechanism belongs to the application; Kafka is an illustrative adapter only.

An explicitly application-selected durable skipped-event transaction replays from TRL without calling the handler. If an unpublished tail is lost
at takeover, the successor reconsumes those inputs and may choose different batches or a different applied/skipped outcome
if the earlier failure was transient. This follows the existing allowance for unpublished-tail rollback. An irrevocable
skip/result acknowledgement requires its durable boundary or an authoritative upstream outcome record; a local failure
or a leader-local progress notification alone cannot promise that stronger guarantee.

## Leader-centered follower sessions and election triggering

Decision recorded 2026-08-30: version one has no peer-to-peer membership or failure-detection mesh. Every follower
communicates only with the current leader through the injected peer transport. The production mapping is authenticated
HTTP hosted by the parent application's Kestrel pipeline; the deterministic mapping is an in-process adapter with the
same protocol semantics.

A joining or reconnecting follower:

1. reads `cluster/leader.json` from Azure and treats its term, session, generation, database
   instance set, endpoint, API key, and lease observation as the only leader-discovery/authority source;
2. validates that active published per-database metadata records agree with the selected leader or expose an activation in progress;
   removed databases are excluded from this check and continue independently on old nodes;
3. opens one long-lived logical leader session carrying its identity, compatibility information, per-database resume
   vector, and candidate endpoint;
4. receives leader heartbeats, latest TRL progress and handoff controls, then pulls TRL bytes needed for comparison;
5. periodically reports its accepted and speculative progress to the same
   leader through the transport abstraction.

There is no follower-to-follower discovery or health exchange. A stream failure, heartbeat timeout, authentication
failure, or rejected authority refresh makes that follower stop accepting frames and reread the Azure leader record. If
the record now names another session, it connects to the new endpoint. If it still names the unreachable leader, an
eligible follower may begin randomized election backoff and attempt to acquire the Azure lease. Other disconnected
followers may do the same. An active lease rejects them; only lease ownership plus the leader-record CAS can make one
of them leader. Followers whose leader session remains healthy do not start an election merely because another follower
lost its connection.

Follower application transactions continue into the local speculative cache throughout disconnect, reconnect, and
election contention. Transport liveness controls only direct canonical distribution and when a node tries storage
coordination; it never gates local application commit and never proves that leadership has ended.

A database-set difference does not reject a session when the follower presents a valid higher-generation database set whose
predecessor is the leader-selected database set. The old leader announces TRL progress only for continued matching instance IDs. It
does not send a retired-by-target database to a follower that no longer hosts it, and it sends no progress for the target's
unpublished additions. Such a node is not ready for the old database set as an ordinary same-generation failover candidate,
but it can become ready for upgrade handoff under its proposed database set.

### Session and progress state

The initial session request should carry semantics equivalent to:

```text
FollowerHello
    clusterId
    nodeId
    nodeSessionId
    connectionId
    candidatePeerEndpoint
    applicationGeneration
    databaseNames[]
    protocolVersionRange
    hostedDatabaseInstances[] = {
        databaseName
        databaseInstanceId
        replicationCompatibilityFingerprint
        initialMode = replicated | awaitingInitialization | retiredVolatile | schemaDetachedVolatile
    }
    lastObservedLeaderTerm
    lastObservedLeaderRevision
    databaseResumeVector[]: ResumeToken
```

While that logical connection remains current, the follower reports:

```text
FollowerStatus
    nodeSessionId
    connectionId
    statusSequence
    readiness
    applicationGeneration
    databaseNames[]
    upgradeHandoffReadiness
    databaseProgress[] = {
        databaseName
        databaseInstanceId
        mode = replicated | awaitingInitialization | retiredVolatile | schemaDetachedVolatile | reopeningFromLeader
        confirmed: CanonicalPosition?
        durable: RecoveryBoundary?
        speculativeHeadEventPosition
        speculativeTransactionCount
        speculativeBytes
        cursor: TrlCursor?
        canonicalAllocationWatermark
        initialEventCursor?
    }
```

The leader accepts a status report only for the currently registered `(nodeSessionId, connectionId)` and only when
`statusSequence` increases. Closing or replacing a session removes its entry after a bounded liveness timeout. The
resulting connected-follower registry is intentionally ephemeral. It supports distribution metrics, lag tracking, and
graceful-handoff candidate selection; it does not define cluster membership and is not reconstructed from object
storage after a leader restart.

The leader-to-follower stream carries versioned messages such as:

```text
LeaderMessage
    authority: AuthorityIdentity
    messageSequence
    kind = heartbeat | termStart | databaseSet | databaseLifecycle | databaseGenesis |
           trlProgress | steppingDown |
           handoffOffer | handoffFinal
    payload

TrlProgress
    eventId
    trlFileId
    trlPosition
```

`databaseSet` is the first state-bearing message on a new session and must match the names and generation in the
leader record. `databaseLifecycle` reports additions and removals derived from the generation and name sets, with active instance identities from published per-database metadata. It requires no durable retirement record. `databaseGenesis`
names the published initialization boundary from which waiting followers restore and open; it never authorizes a follower initialization write.

Database bootstrap reads the selected native KVI and required files directly from Blob Storage. There is no
`databaseBootstrap` peer message or HTTP KVI/PVL transfer through the leader. A future open/rebuild may select a newer
remote checkpoint; a running follower does not receive checkpoint updates.

`trlProgress` advertises the latest complete leader-local boundary in the selected database/session context. It can
be coalesced and carries no transaction bytes or per-transaction envelope. The follower fetches the required TRL ranges
through the range endpoint, under [progress notification and comparison](#progress-notification-and-pull-based-comparison).
A numeric event/offset is not evidence of Blob durability. Unexpected authority/lineage forces resume or authoritative
rereads; missing intermediate notifications alone is not a transaction gap.

The API key is not distributed by peers. A node learns the authoritative leader endpoint and current bearer key only
by reading the Azure leader record. Possession of that key authorizes requests within the shared storage trust domain,
but neither a successful HTTP request nor a control message establishes leadership.

The session carries lightweight progress notifications. Native TRL bytes are pulled in bounded range requests,
with fallback to verified Blob history and the existing restore/input-replay path when necessary. Neither slow requests
nor disconnected followers require unbounded leader memory or waiting in application Commit.

Compaction produces no peer controls or results. Remote KVI/PVL artifacts are consumed from Blob Storage during
normal restore; local compaction remains independent of the leader session.

Identical event-transaction payload comparison is the confirmation path. Virtual batching leaves grouping unchanged;
file identities and physical layouts are validated separately. Canonical frame-chain identity comes from the leader bytes, which stay separate from local
BTree value files. Every node may independently rewrite local value pointers without changing logical history.

## What the current BTDB code implies

The existing code provides useful integration points, but it is not currently a multi-node protocol.

### BTDB file behavior

- `BTreeKeyValueDB` serializes writing transactions inside one process. That lock says nothing about another process.
- `BTreeKeyValueDBTransaction.MakeWritable()` immediately writes `MagicStartOfTransaction` to the one active TRL.
  Cursor mutations then write their commands directly to that writer before changing the writable BTree root.
- `Commit()` is synchronous. It writes metadata deltas and the commit marker, flushes or hard-flushes according to
  `DurableTransactions`, and updates the root's TRL cursor. An ordinary commit then replaces `_lastCommitted`;
  a virtual-batch commit records the committed batch boundary and defers publishing the root to readers.
- Copy-on-write roots and reference counts support ordinary read transactions. Replication does not keep an extra
  historical root for each follower commit or require an API to restore committed speculative suffixes.
- Values longer than seven bytes use `(fileId, valueOffset, valueLength)` references. Structural comparison must decode
  actual values while preserving the running tree's local file namespace; byte-coordinate equality is not required.
- Existing rollback aborts the current uncommitted writing transaction. Failed application attempts need that rollback
  plus correct private TRL/counter handling. Committed structural divergence instead restarts the process.
- Current standalone code can rotate inside a transaction: command writers call `WriteStartOfNewTransactionLogFile()`
  when reaching `MaxTrLogFileSize`. Replication must capture cross-file transaction ranges and preserve legacy replay.
  The core now supports `ITransactionLogSizeStrategy`: its immutable mapping from TRL numeric ID returns soft and hard
  limits. Soft-limit rotation happens before the next transaction; the hard limit may split a transaction between
  commands and includes headers and terminators. `1024 <= SoftLimit <= HardLimit < uint.MaxValue` respects 32-bit
  root/value offsets. A command must fit the destination file. The mapping must remain identical across all nodes,
  restarts and deployments; auto-adjustment is incompatible. See [core preparation](../Doc/ReplicationCore.md).
  Odd TRL allocation is an opt-in integration requirement, not permission to change standalone file allocation.
- `.kvi` and `.pvl` files are written as new files and become immutable after finalization.
- File IDs and generations are allocated from local collection state. In failover mode, canonical `.trl` and `.pvl`
  allocation must instead be leader-assigned for canonical objects. Followers keep separate local allocation and file
  identity because local allocation, file rotation, and independent compaction can produce different physical layouts. KVI
  files and PVL compaction outputs are not distributed to running peers. Local compaction creates no KVI. Remote export maps source
  references to a separately planned Blob inventory; a future leader must allocate canonical IDs from canonical
  cluster state rather than its local maximum file ID.
  Replication TRL allocation uses fresh odd IDs; an even legacy append target is closed before new transaction output,
  under [Odd-numbered TRL allocation and legacy databases](#odd-numbered-trl-allocation-and-legacy-databases).
- Compaction creates replacement files and removes obsolete files. Remote deletion follows publication of the replacement recoverable KVI; after that it need not wait
  for followers; an operational deletion delay is configurable.
- `LoadTransactionLog()` already decodes TRL commands into a writable root, but it is a startup-oriented routine that
  mutates loader fields and publishes commits as it scans files. The structural comparator should share validated command decoding without applying commands to a second tree.
  Restart recovery keeps canonical replay; no live historical-root restoration API is required.
- Current compaction calls `CommitFromCompactor()`, which swaps a rewritten BTree root without emitting TRL commands.
  Preserve that mechanism for independent node-local compaction, with bounded writer-gate chunks and no local KVI.
  It needs neither distributed rewrite messages nor a new TRL command or replay-time skip rule.
- `Compactor.RunCore()` currently couples local file usefulness, PVL creation, pointer replacement and KVI-based
  cleanup. Separate local pin-based cleanup without KVI from remote inventory planning and remapped KVI export.
- Current ObjectDB leak detection scans a read-only transaction and buffers a bounded sequence of length-prefixed exact
  keys. Removal later opens a separate writing transaction, finds each still-present exact key, erases it, and commits.
  Replication mode must split detection from deletion: detection requests that the parent system publish an ordinary
  event, and no node calls the local removal path until consuming that event. Applying the list after later events is
  safe because leak-removal keys are stable identities that are never reused; every handler idempotently ignores keys
  already absent.
- `KVCommandType` needs no new command value. Compaction stays outside TRL, and leak removal is encoded in the parent
  application's ordinary event payload and executed through existing erase operations.

Relevant code:

- [`BTreeKeyValueDB.cs`](../BTDB/KVDBLayer/Implementation/BTreeKeyValueDB.cs)
- [`BTreeKeyValueDBTransaction.cs`](../BTDB/KVDBLayer/Implementation/BTreeKeyValueDBTransaction.cs)
- [`RootNode12.cs`](../BTDB/BTreeLib/RootNode12.cs)
- [`Compactor.cs`](../BTDB/KVDBLayer/Implementation/Compactor.cs)
- [`CompactorLeakDetector.cs`](../BTDB/ODBLayer/CompactorLeakDetector.cs)
- [`ObjectDB.cs`](../BTDB/ODBLayer/ObjectDB.cs)
- [`IFileCollection.cs`](../BTDB/KVDBLayer/Interface/IFileCollection.cs)
- [`IFileCollectionFile.cs`](../BTDB/KVDBLayer/Interface/IFileCollectionFile.cs)
- [`IFileCollectionWithFileInfos.cs`](../BTDB/KVDBLayer/Implementation/IFileCollectionWithFileInfos.cs)
- [`InMemoryFileCollection.cs`](../BTDB/KVDBLayer/Implementation/InMemoryFileCollection.cs)
- [`OnDiskFileCollection.cs`](../BTDB/KVDBLayer/Implementation/OnDiskFileCollection.cs)

### Minimum opt-in BTDB core extensions

The exact API is still open, but the core needs capabilities equivalent to these operations:

```text
CommitFollowerSpeculation(eventRange, outcome)
    -> local TRL range + disk-indexed comparison metadata; no retained root

RollbackFailedApplicationAttempt(startRoot, startWriterCut, applicationState)
    -> restored private attempt state + retained rollback comparison evidence; no committed mutations

CommitSkippedEvent(eventIdentity)
    -> ordinary metadata-only transaction + unchanged application keys

CaptureLeaderCommit()
    -> canonical TRL range with ordinary per-event commits + event coverage/outcomes + comparison metadata

CompareFollowerRange(localEntries, fetchedCanonicalTransactions)
    -> structurally equal | awaiting coverage | structural mismatch | invalid bytes

ConfirmMatchingRange(canonicalResult)
    -> advance confirmed metadata; leave current BTree unchanged

AppendOptimisticEventsAfter(adoptedBoundary, committedLocalGroups)
    -> deduplicated new-term canonical transactions + reconciled publication result

RestartDivergentFollower(reason)
    -> fence session + bounded host restart + canonical rebuild on startup

ValidateClosedTransactionBoundary(trlRanges, expectedEnd, expectedSequence, expectedFrameHash)
    -> valid closed transaction | incomplete/corrupt

CompactNodeLocalFiles(currentHead, localInventory, pins, cancellation)
    -> bounded local physical rewrites + unused-file cleanup; no KVI, TRL or peer messages

PlanRemoteCompaction(canonicalBoundary, remoteInventory, sourcePins, destinationAllocator)
    -> remote destination plan + source-to-remote address map

ExportRemoteKvi(capturedRoot, canonicalTrlCursor, addressMap, destination)
    -> native KVI with remote references + pinned target recovery closure; no mutation of the live local root

ReplayStartupTransaction(kviIdentity, trlTransaction)
    -> applied application/schema transaction state | failed corruption

DetectLeakRemovalCandidate(acceptedAnchor, byteBudget, cancellation)
    -> bounded exact-key list + detector version + observed accepted-root identity

RequestLeakRemovalEvent(candidate, parentEventPublisher, cancellation)
    -> accepted/duplicate publication identity | rejected | ambiguous

ApplyLeakRemovalApplicationEvent(writableTransaction, eventPayload)
    -> ordinary application transaction + idempotently filtered absent keys

RewindToDurableBoundary(boundaryIdentity)
    -> close/reopen from canonical restore point; no historical replication root

EnterVolatileShutdownMode(closedCanonicalBoundary)
    -> irreversible redirection of later transaction/value bytes to disposable local temptrl files

CommitVolatileShutdownTransaction(eventRange, outcome)
    -> disposable process-local BTree root + readable temptrl ranges
```

These names are descriptive, not proposed public API names. The existing `IKeyValueDBTransaction.Commit()` remains a
synchronous local operation. During ordinary failover leader/follower operation it may perform only the local work needed
to append/cache an ordinary application transaction, commit the root, and retain disk-backed comparison metadata.
In replication mode, synchronous `StartTransaction()` is prohibited. Reads use `StartReadOnlyTransaction()`;
writes enter through the existing asynchronous `StartWritingTransaction()`. Startup orchestration for the initial secondary-index reconciliation waits
for leadership before taking the writer lock. Ordinary application writers remain available on followers. Supplying
an eventId on writer admission automatically sets CommitUlong on the writable transaction; no separate comparison
against an application-assigned value is required. Non-application commits signal immediate asynchronous flush;
no new CommitAsync or blocking Blob wait is required. Ordinary application commit must not await the peer
transport, leader progress, object storage, or a background queue with a fixed speculative-window limit. Volatile
shutdown is the explicit no-persistent-append exception described below.

The comparison coordinator is asynchronous, validates leader authority and contiguous ranges, and advances confirmed
metadata without changing the BTree. It uses bounded buffers and a disk-backed index rather than retained roots. There
is no live reconciliation replay or long suffix-repair writer critical section. Structural mismatch invokes restart.

There is no protocol limit on speculative event distance. Pending TRL/index entries grow on disk while leader progress
lags; RAM does not grow through historical root pins. Report lag in count, bytes, event distance, and age. Disk
exhaustion is a fatal node error: stop readiness/publication and use normal restart/rebuild. There is no special low-disk
mode, quota protocol or remote-progress admission window. Normal
BTDB single-writer serialization and user-held read-transaction lifetimes still apply.

The graceful-shutdown switch needs a distinct opt-in BTDB core path. Current `MakeWritable()` immediately starts a TRL
transaction, and values larger than `MaxValueSizeInlineInMemory` leave `(fileId, offset, length)` references in BTree
leaves. Suppressing every post-cut byte write would therefore create unreadable values. The shutdown path must keep old
persistent files readable while allocating new file IDs and bytes from a disposable local file collection.

The selected minimal-core-change direction is a volatile overlay on `IFileCollectionWithFileInfos`: existing file IDs
continue to resolve through the original collection, while every newly allocated transaction log resolves to one or more
`.temptrl` files in a dedicated node/session scratch directory. Routing the existing transaction writer into that overlay
preserves command encoding, large-value addressing, read-after-write behavior, and later-transaction reads. A
deterministic test adapter may use `InMemoryFileCollection`, but the production path may use ordinary buffered temporary
files to avoid unbounded RAM growth. Volatile IDs must not collide with persistent IDs or advance the canonical allocator.

The failover protocol does not use `.temptrl` commands for comparison, distribution, handoff, or recovery. The files are
never hard-flushed, uploaded, referenced by a published KVI, or renamed into the canonical file set. Clean shutdown deletes them
after the volatile root is released. Startup removes every recognized `.temptrl` file and abandoned prior-session scratch
directory **before** constructing the canonical file collection, scanning KVI/TRL metadata, or reporting readiness. It
never parses or replays those files; failure to remove them keeps the node unavailable rather than risking that
scratch data is mistaken for canonical state. Cleanup is restricted to an exact configured failover scratch root and
validated session/file names; it must never follow links or sweep an ambient operating-system temporary directory.

Entering volatile shutdown mode is irreversible for that node session. Its commits advance only the disposable BTree
head and process-local event cursor. Temporary files receive only node-local volatile identities; they do not allocate
canonical transaction sequences, advance confirmed metadata, advertise leader-local TRL progress, enqueue object-store
publication, write KVI/PVL files, or acknowledge upstream events at a durability level that the shutdown canonical cut
did not reach. A successor deterministically re-executes those events from its adopted durable base. If temporary storage
is full or lost, fail the node and use normal restart/rebuild; never switch that generation to Blob publication.

### Application state and reader generations

Follower structural divergence restarts the process and reopens canonical state, rebuilding ObjectDB allocation counters
and metadata caches naturally. There is no live BTree rebase or migration of old readers across canonical file namespaces.
Failed uncommitted application attempts still need correct rollback of private TRL, counters, and caches before a retry
or metadata-only skip; this narrower transaction integration remains B3.

Within a running process, ordinary readers and the current BTree must retain their original file-ID-to-bytes mapping.
Canonical incoming bytes remain separate from locally allocated speculative files. Local cleanup accounts for user-held
read transactions and pending comparison bytes. A matching structure never authorizes remapping local value pointers.

### No-regression boundary for existing BTDB

Failover is opt-in. A normal `BTreeKeyValueDB` must retain its current constructor behavior, transaction format,
synchronous commit semantics, allocation profile, and command-write path. In particular, standalone transactions
should not gain per-command hashing, buffering, interface dispatch, network checks, term checks, or an extra copy.

That likely means explicit specialized entry points or a transaction mode chosen outside the existing hot loop,
rather than a nullable failover callback tested on every cursor mutation. Before implementation, representative
standalone read/write benchmarks and allocations must be captured as a baseline. The acceptance gate is no measurable
regression beyond benchmark noise, in addition to functional compatibility tests for old databases.

### Local durability and checkpoint capture are separate concerns

With `DurableTransactions=true`, `BTreeKeyValueDB` calls `IFileCollectionFile.HardFlush()` after writing the commit
marker and before publishing the new in-memory root. On the leader this protects its authored local tail; on a
follower, an equivalent policy can protect the speculative cache and confirmed local prefix. Losing an unconfirmed
follower suffix after a process crash is acceptable because its ordered events can be re-executed; it does not change
canonical progress. This protects bytes during the current process lifetime; the selected restart path discards old
cache regardless of hard-flush policy. Object storage is never awaited by a follower application commit.

The important new core integration point is a consistent checkpoint export:

1. select one committed root and its atomically stored event position;
2. create or select a `.kvi` that describes exactly that root;
3. record immutable byte cuts for the referenced files, ending at the selected root's exact post-commit cursor; its
   final transaction may span several files, all included when needed for replay. A file end alone proves no commit;
4. pin every `.trl`, `.pvl`, and `.kvi` file referenced by that root while upload is in progress;
5. let normal event application continue without allowing compaction, deletion, or an accidental read beyond a
   captured file length to invalidate the checkpoint;
6. release the pin only after publication succeeds or the attempt is abandoned.

BTDB already has promising pieces of this mechanism:

- `IKeyValueDBTransaction.SetCommitUlong()` updates a special `ulong` as part of the same writing transaction. If the
  event cursor is one monotonic integer, it is a candidate for `lastAppliedPosition`; canonical transaction sequence,
  stream identity, and hashes still need separate metadata.
- `NextCommitTemporaryCloseTransactionLog()` writes and flushes a safe temporary end marker after the next commit.
  This can define a readable checkpoint cut without permanently closing the active `.trl`; an export handle would
  need to expose the exact prefix length including that marker.
- `CreateKvi()` captures a referenced committed root and records its transaction-log file and offset, but it currently
  does not return a durable checkpoint handle, immutable file lengths, or a lifetime pin for asynchronous upload.

The design should build the smallest missing export/pinning API around these semantics instead of inventing a second
BTDB transaction format.

Whether mirrored bytes use `DurableTransactions=true` is a local I/O policy. It does not change cluster durability or
authorize reusing cache after process restart. Recovery uses the selected canonical boundary and retained input under
either setting.

### Canonical transactions and parent-published leak removal

Updated 2026-09-14: after sequence-zero initialization, canonical TRL contains committed application transactions,
explicit application-selected skips, and the [schema transactions](#non-application-schema-transactions-during-upgrades)
used by newer application versions. All use ordinary BTDB command/commit encoding. Schema changes are logical key
mutations and consume canonical sequence without consuming input. Term changes, checkpoint publication, physical
local/remote physical compaction, KVI export and file-retirement decisions remain outside TRL and consume no
canonical transaction sequence.

Leak reachability is an ObjectDB-level question, while the logical deletion must enter the same ordered input path as
every other state change. Any ready node may detect leaks against a current fully confirmed read transaction, never its speculative head,
but it cannot erase accepted state or send a special command to the leader. It creates a bounded candidate with semantics
equivalent to:

```text
LeakRemovalCandidate
    publicationRequestId
    databaseName / databaseInstanceId
    detectorVersion
    observedAuthority: AuthorityIdentity
    observedPosition: CanonicalPosition
    observedLogicalAuditHash?
    keyEncodingVersion
    keyCount / encodedKeyBytes / keyListHash
    encodedKeys = repeated(keyLength, exactKeyBytes)
```

The node submits that candidate through the injected `ParentEventPublisher`. The parent system validates the database,
detector and key encoding, strict ordering, duplicate absence, key/count/byte budgets, and hash; reconciles ambiguous
publication by `publicationRequestId`; and publishes a normal application event into the shared cluster stream for that database.
The replication peer protocol is not involved. A successful request means the parent accepted or already accepted the
event, not that any BTDB mutation has happened yet.

The event payload carries the validated exact-key list and ordinary parent-assigned event identity. When it reaches a
replica through normal consumption, the application's handler uses the current event transaction, erases every key
that is still present, and ignores already-absent keys. That transaction commits the mutations and event cursor atomically.
Removed exact keys are never reused, so delay, overlap, or a second removal event cannot make an old candidate erase newly repurposed data.
Even when all keys are already absent, a successful commit consumes the event exactly once.
The application selects rollback, retry, explicit skip or termination under its ordinary failure policy.

The current leader authors the resulting ordinary canonical TRL transaction because it consumes the event as usual.
Followers may batch memory publication independently and compare ordinary event transactions, restarting on mismatch.
Startup sees only a normal application transaction. Leader change, disconnection, or graceful
shutdown needs no leak-specific recovery: the retained event is replayed in total order by the successor. Detection can
run again and the parent may deduplicate by request/list identity or publish another idempotent event.

The list-size budget bounds one handler's work; the virtual memory-batch budget bounds deferred root publication.
Larger results are split by the parent into several bounded ordered events or detected again later; this batching policy is part of the application-event contract rather
than a replication transaction format.

### Independent local compaction and leader-only remote compaction

Decision recorded 2026-09-14: every node, including followers, may run full local physical compaction independently.
Local compaction operates only on that node's disposable files and creates no KVI. Only the selected leader runs remote
compaction against Blob Storage. Neither kind sends compaction operations, PVL rewrite plans, completion results or
local deletion instructions to other nodes. The former leader-distributed compaction protocol is withdrawn.

Local compaction may create/seal PVLs and replace value pointers on an eligible current head in bounded writer-gate
chunks. It preserves logical contents, CommitUlong, ordinary transaction bytes and canonical sequence. It requires no
leader confirmation or agreement on physical layout. Never roll back or replay a committed suffix merely to compact it.
Each node has independent local file allocation and pin accounting. Local PVL IDs cannot consume or predict canonical
remote IDs, alter the deterministic TRL sizing policy, or change another node's namespace.

Local cleanup protects every live root/reader, current tree, pending virtual-batch rollback/replay range, comparison
range and active export source. No local KVI is created on either the small-waste path or after pointer rewriting.
Files needed only for a self-contained local restart cut may be removed once no in-process dependency needs them:
startup always validates/restores the selected remote closure. It must never open an incomplete local set as canonical.
File-backed values in old readers remain readable until the last local pin is released.

Local and remote work may share one physical compaction pass. Its sealed local PVLs feed the publication path below;
there is no requirement for a second rewrite pass. Remote export does not rewrite the running leader's tree into remote file IDs. A follower need not learn that a remote compaction
occurred. The resulting PVLs and KVI are published to Blob Storage only; another node obtains that layout through
normal open/rebuild from the selected remote recovery closure, never through a live compaction message.

Local compaction and remote publication have independent cancellation tokens. Losing leadership cancels only the
remote token (PVL uploads, KVI streaming/finalization and remote deletion), preserving selected remote recovery data.
The local pass may finish under its own token. Do not link the local token to leader authority or propagate a remote
cancellation/failure into it. Host shutdown or scratch detachment may separately cancel local maintenance.
A still-valid local node may continue purely local maintenance under its local lifecycle rules; no canonical authority
is implied. Graceful shutdown, retirement or schema detachment cancels maintenance at a bounded writer boundary before
entering volatile mode. After that cut no new PVL/KVI or file-retirement work starts; application scratch writes follow
the existing volatile-execution rules.

#### Separate node-local and Blob compaction modes

Decision refined 2026-09-15: all nodes compact locally without producing a local KVI; only the selected leader
publishes the resulting checkpoint remotely. The physical pass can serve both local and remote needs. Local and Blob
inventories still differ, but that difference does not require a separate physical compactor.

| Mode | Inventory and allocation | Result |
| --- | --- | --- |
| Standalone | Existing database file collection | Existing local PVL rewrites, local KVI and restart-safe cleanup remain unchanged. |
| Replication node-local | That node's actual files, physical layout and pins | Independent full local PVL/pointer rewrites on every node; no local KVI, including small-waste and cleanup-only paths. |
| Leader checkpoint publication | Sealed local outputs plus verified Blob inventory | Reuse published PVLs, upload missing PVLs under fresh remote IDs, and stream native KVI with those PVL IDs. |

Before exporting the remote KVI:

1. Capture a fixed, canonically valid root and pin its source files. Local compaction and application work may continue
   without invalidating the captured root or bytes. Use distinct local-maintenance and remote-publication tokens.
2. Remember complete, sealed files verified against Blob Storage during download. A download from the leader also
   qualifies when the same complete file is confirmed present in Blob Storage. Restore preserves the physical file ID.
   A known PVL can be reused under that ID without another upload. Partial downloads and active files do not qualify.
3. For each other sealed local PVL, reuse its confirmed local-to-remote placement from an earlier checkpoint, or
   reserve a free remote ID and upload the whole file there. Preserve its bytes and offsets; only the KVI file-ID
   reference changes. Retain the placement after successful upload, including when a subsequent KVI publication fails,
   so the next compaction does not upload it again. An uncertain upload retains its reserved destination for exact
   reconciliation/retry. Placement receipts belong to the current verified local database session, not the read path.
4. TRL IDs, offsets and bytes are never remapped. Ensure canonical TRL publication already covers the snapshot's
   recovery cursor and all referenced TRL ranges. Arbitrary local TRL bytes cannot become canonical via KVI export.
5. Once every prerequisite PVL/TRL is confirmed, serialize native KVI directly to a forward-only chunk uploader.
   No local KVI staging file is required. Preserve keys, values, compression, CommitUlong, Ulong metadata and the
   unchanged TRL cursor. KVI block staging/upload must not start before this prerequisite barrier. Select/finalize
   the remote KVI only after all chunks succeed under remote authority. Missing or colliding PVL placements fail export.
6. Only successful/reconciled remote KVI publication permits delayed deletion of superseded remote files. Failure,
   cancellation or authority loss preserves the selected closure. It does not cancel local compaction. No results
   or mappings are sent to running peers; fresh restore downloads the selected files under their remote IDs.

Remote IDs must be allocated from the Blob inventory, not the local maximum: remote objects can outlive local files.
A receipt remains reusable only while its remote object is protected from cleanup. The baseline has no remote deletion
implementation; its integration must account for active publications and selected checkpoints before retiring receipts.

`Compactor.RunCore()` currently derives usefulness from the local collection and calls `CreateIndexFile()` both on
its small-waste path and after physical rewrites. `CreateKeyIndexFile()` currently writes source file IDs/offsets and
computes generations through that same collection. The core now provides `CaptureKeyIndexSnapshot` and forward-only `WriteTo`, with whole-PVL ID substitution and pinned
sources. The internal `CheckpointPublisher` tests dependency ordering, upload receipt reuse and retry. Actual no-local-KVI
compactor mode, production canonical TRL/storage/authority integration and remote GC remain unfinished; these helpers
do not close B3/B5/Q6. See [Testing.md](Testing.md).

### Existing `BTDB.AzureStorage` is not the failover protocol

The current [`AzureBlobFileCollection`](../BTDB.AzureStorage/AzureBlobFileCollection.cs) is a useful single-node local
cache and asynchronous backup mechanism, but its current contract does not provide the conditional publication and
multi-node fencing required by this design. `BTDB.Replication` may reuse lower-level transfer ideas, but it needs
term-qualified or immutable data objects, KVI-last publication, CAS-fenced leader/TRL publication, and
reachability-aware garbage collection. The existing adapter details are recorded in
[ObjectStorages.md](ObjectStorages.md#conditional-tail-append-with-block-blob), outside the failover protocol.

## Object-store dependency

The detailed provider-neutral interface, all available `celld` `Bucket` and `BucketOwnership` operations, CAS retry
and ambiguity rules, the live capability probe, and Azure/S3 limitations are documented in
[ObjectStorages.md](ObjectStorages.md).

The architecture depends on these conclusions:

- the leader object and canonical TRL objects use conditional create/replace with opaque version tokens;
- CAS results preserve `Applied`, clean `Rejected`, and possibly committed `Ambiguous` as distinct outcomes;
- conditional writes are not transparently retried; ambiguity is reconciled against actual operation results;
- CAS on canonical TRL directly publishes complete transactions; no per-batch database-state CAS follows it;
- transactions may span TRL objects; complete-transaction recovery closure is required, and adoption
  is conditional on the TRL objects themselves;
- the Azure version-one data plane exposes conditional atomic tail append as a semantic operation; physical layout,
  staging, and commit details remain inside the provider adapter;
- bulk data is immutable or term-qualified; listing discovers published KVIs, while native references and canonical lineage validate their required files;
- a live four-step capability probe qualifies each real endpoint before leadership is enabled;
- leader-session loss never expires authority, and takeover freezes unless the storage service makes the lease
  available or its expiry can otherwise be proven safely;
- authority traffic is isolated from bulk uploads so checkpoint pressure cannot starve renewal or fencing.

## Cluster-wide Azure leadership and publication protocol

This is the narrowed version-one direction. There is exactly one Azure-selected leader for the entire failover cluster.
Application code still executes speculatively on followers, but only that one session authors canonical transactions
for databases active in the leader-record-selected database set.

### Shared leader record

The election object is one small block blob, for example `cluster/leader.json`:

```json
{
  "format": 1,
  "clusterId": "stable-cluster-identity",
  "revision": 42,
  "lastOperationId": "unique-leader-update-id",
  "term": 7,
  "nodeId": "stable-or-random-node-id",
  "sessionId": "random-node-session-id",
  "applicationGeneration": 12,
  "databaseNames": ["main", "users", "jobs"],
  "eventsToSkip": [
    {
      "databaseName": "jobs",
      "eventId": "18446744073709551000",
      "recordedAtUtc": "2026-09-07T12:00:00Z"
    }
  ],
  "peerEndpoint": "http://10.42.1.17:8080/_btdb/replication",
  "apiKey": "base64url-random-256-bit-value"
}
```

The protocol core treats `peerEndpoint` as opaque. The version-one HTTP adapter publishes the routable IP address and
port of the leader's Kestrel listener plus the mapped failover base path; it may also permit a routable DNS name. The
in-process transport uses a test-local endpoint resolved by its injected registry.

The JSON contains the active database names directly, with no per-database progress. Published TRL/checkpoint metadata retains
instance identities and the initialization cursor. `applicationGeneration` is both the selected application version and a persistent
election floor; a later replacement must not lower it. The Azure ETag serializes leader identity and database set changes,
while a finite native Blob lease on this same blob supplies server-enforced liveness. A session is authoritative only
when it
both holds the current lease ID and the JSON names its term and session. `revision` changes on record replacement;
ordinary lease renewals do not rewrite the JSON or change its ETag.

`eventsToSkip` holds rare event-consumption timeout decisions, keyed by database name and exact event ID in the one cluster-wide
event stream. No per-entry stream identity is needed; the list belongs to the current cluster incarnation. IDs are encoded losslessly (strings in this JSON example). Empty clusters normally use an empty array.
The current lease owner merges additions and cleanup through the serialized leader-record CAS lane, preserving all
unrelated entries and authority fields. Takeover and handoff preserve the list. Updating it changes revision/ETag, not
term or session; ordinary lease renewals still do not rewrite JSON. It is recovery control metadata, not database progress.
See [Consumption timeout and restart](#consumption-timeout-and-restart) for interpretation and retention.

`apiKey` is a randomly generated per-term secret used to authorize failover peer requests. Every trusted node can read
it from Azure, so it authenticates participation in this storage trust domain rather than giving per-node identity. The
shared protocol authorization component compares it in constant time, excludes it from logs and metrics, and rotates
it on every leader change. The HTTP adapter carries it as a bearer credential and uses TLS unless the deployment
explicitly accepts a trusted private-network boundary.

### Published database state

Decision recorded 2026-09-14: conditional publication of the TRL itself is the durable transaction publication point.
There is no per-database `state.json` CAS after a TRL append, and no second mutable record advancing the accepted TRL
length. A successfully published complete transaction does not wait for another write to become durable.

The publisher derives the following working state from verified canonical TRL and checkpoint metadata; this is an
in-memory view, not another mutable object to persist after every transaction:

```text
PublishedDatabaseView
    database: DatabaseIdentity
    authority: AuthorityIdentity
    activeTrl = { objectKey, lineageId, fileId, token, length }
    durableBoundary: RecoveryBoundary
    checkpoint: CheckpointReference
    canonicalAllocationWatermark
```

The active TRL ETag is the conditional-write token. Read content and the associated metadata from a consistent blob
version. Validate transaction closure, lineage and hashes; a physical length alone cannot prove a complete transaction.
Discover published native KVIs from the database files. No separate checkpoint pointer, manifest or selection CAS
is required; writing the complete KVI after its prerequisites is the publication point. A checkpoint may only describe history already published in canonical TRL.

### TRL publisher

One serialized publisher per database owns the current canonical TRL token and validates authority, ancestry and
transaction boundaries. It runs outside ordinary application commits. Non-application operations immediately signal
its asynchronous flush path without awaiting completion. A TRL write and its conditional publication are one operation, not preparation
followed by a separate TRL CAS.

| Intent | Required preconditions | Durable effect |
| --- | --- | --- |
| `CreateGenesis` | Current activating leader; no published instance; initial transaction with predecessor CommitUlong and recovery prerequisites verified | Conditionally publish the discoverable initial TRL including its complete commit; establish sequence zero |
| `AdoptTerm` | Current selected leader; existing canonical TRL chain verified; current writable predecessor token known | Fence predecessor writes on the TRL itself and establish continuation under the new authority, preserving published history |
| `PublishTail` | Permitted leader authority; whole committed transactions and all prerequisites verified | CAS the canonical TRL to expose its new committed suffix; returned/reconciled success establishes durability |
| `PublishKvi` | All required PVLs and canonical TRL through the KVI cursor have completed publication before any KVI upload starts | Write the complete native KVI last; its successful publication permits deletion of obsolete files, without a separate selection write |

1. Prepare any required immutable files and bounded transaction/index metadata. Validate current authority and the exact
   expected TRL version immediately before dispatch. Never overwrite previously published bytes.
2. Issue one conditional TRL publication with automatic retries disabled. In the ordinary single-file case this is
   `AppendIfCurrent` through the complete commit terminator. On `Applied`, update the local token/boundary and report
   durability immediately for complete transactions whose entire closure is reachable; there is no follow-up
   database-state write. Cross-file publication uses the prepare-successors ordering below and still requires failure tests.
3. On `Rejected`, reread the actual TRL winner, reconcile ancestry and authority, then derive a new permitted operation.
   Do not blindly append the same transaction again or let a former leader adopt a new token to resume writing.
4. On `Ambiguous`, stop further mutations on that publication path and reconcile blob version, content/length, hashes
   and operation identity. If the append succeeded, its complete transactions are already durable even if the caller
   lost the response. An old read or cancellation does not prove the write cannot still land.

Checkpoint capture ahead of the durable TRL boundary waits in the background or is abandoned. Older checkpoints cannot
replace newer ones; at equal canonical position a complete verified physical replacement may be selected for compaction.
Checkpoint selection never relabels history, rewrites committed TRL or adds an application transaction. Publication pins
and GC protect staging and current recovery prerequisites independently of the ordinary TRL append path.

#### Publisher operation lifecycle

Each database has one serialized mutation lane with local states `Idle`, `Preparing`, `InFlight`, `Reconciling` and
`Fenced`. Application commit only retains the immutable transaction ranges and signals that lane; it does not await it.
Ordinary commits may use lazy scheduling; non-application commits wake it immediately and bypass only the batching delay.
An immediate signal cannot overtake an earlier unresolved operation. Coalesce wakeups without losing any committed range.

| Operation stage | Required action | Interruption/result |
| --- | --- | --- |
| `Preparing` | Pin ranges and prerequisites, derive expected token/operation identity, verify authority, prepare successor files if needed | No selected history change. Unlinked staging stays protected until abandoned/reconciled; no key reuse. |
| Dispatch | Recheck authority/session and expected token; issue one conditional canonical TRL operation with hidden retries disabled | Record the operation as in flight before dispatch so synchronous completion and cancellation cannot escape reconciliation. |
| `Applied` | Validate returned identity/version, advance durable boundary only over fully reachable complete transactions | Update the internal recovery watermark independently of Commit completion; then release unnecessary pins and prepare later work. |
| `Rejected` | Read the actual winner and compare lineage/operation identity | Already published matching work is not appended twice. A stale session cannot adopt a winner token to resume writing. |
| `Ambiguous` | Enter `Reconciling`, read actual version/content and operation identity; suspend later mutations on this lane | An old read does not prove the outstanding write failed. A landed publication stays durable even if Commit caller/process is gone. |
| Authority loss | Fence new dispatch; preserve bookkeeping for already dispatched requests | Read-only reconciliation may learn an old write landed. Never use that result to re-enable canonical writing. |

A same-file append is selected by that file's CAS. A cross-file transaction is selected by predecessor CAS only after
all successor prerequisites are prepared, as specified below. Rollback bytes preceding the next commit remain in the
ordered ranges; they do not independently advance the committed event cursor. Future implementation must retain those
bytes even if core rollback rebuilds its working root. A KVI request shares publication ordering checks but does not
insert a per-transaction state update.

The internal `CanonicalTrlPublisher` implements this bounded lane over real native capture. It holds a fixed source
cut and checks `LeaseAuthority` before every dispatch. Successor preparation does not advance the selected boundary.
A returned or reconciled predecessor success acknowledges capture; ambiguous requests retain the same intent and
block later records. An explicit retry may resend that same CAS while authority is valid. After fencing, only reads
may reconcile dispatched work. A changed selected version reports conflict for coordinator rediscovery, not an
implicit rebase. See [lane tests and limits](Testing.md#canonical-trl-lane-necessity-and-limits). Production transport,
coordinator wiring and cold restore validation are not yet implemented by this helper.

#### Transaction-aligned durable boundary

Decision revised 2026-09-14: transactions may span several TRL files, including their commit or rollback
terminator. The earlier transaction-boundary-only rotation proposal is withdrawn. Preserve supported core file/offset
limits and rotate as needed; do not interpret a large transaction as permission to overflow a file offset. This applies
to application, genesis and schema transactions. Virtual memory batching remains independent of physical rotation.

A transaction's representation contains an ordered list of TRL ranges, with validated continuation between files and
exactly one final transaction terminator. Recovery and comparison may stream ranges with bounded buffers, but no partial
transaction advances the committed root, input cursor or canonical sequence. Previously completed transactions remain
committed even when later ranges are missing. A physical file end is not a transaction boundary.

CAS directly on canonical TRL remains the publication authority; there is no second per-batch state.json commit.
For a multi-file transaction, use the selected prepare-successors ordering (R1-A): upload and verify the uniquely
named successor chain through the final commit before publishing the remaining bytes and continuation reference on the
current canonical predecessor TRL with one CAS. Preserve its old prefix. The successful predecessor CAS makes the whole
verified transaction reachable and durable. Prepared cuts must remain unchanged until selected; later writes to the
new active tail require the resulting authority/token. Adoption competes on that predecessor token. Failed publication
leaves unlinked staging; ambiguous publication is reconciled from the actual predecessor TRL version.

No separate state CAS follows. Protect prepared successors from GC until publication is resolved. Genesis has no existing
predecessor: prepare its continuation prerequisites first, then conditionally create its discoverable initial TRL.
Native KVI discovery is selected in R2; the TRL continuation codec is exercised by the internal native publication lane. Qualify production interruption, old-leader races and successor ownership handover
before implementation can claim this invariant. Do not infer canonical ancestry from file numbers alone.

RecoveryBoundary names the last complete verified transaction on the selected published chain. Any required immutable
range index must exist before the TRL version referencing it is selected. It is a prerequisite, not a second durability
authority. Unlinked successors are staging and must be protected from premature GC until reconciliation completes.

#### Odd-numbered TRL allocation and legacy databases

Decision recorded 2026-09-14: newly written TRL files in replication mode use odd numeric file IDs; every newly
allocated non-TRL file uses an even ID, including KVI, PVL and sub-database files. Existing databases
need not satisfy this rule: valid historical TRL files with even or odd IDs remain readable with their original IDs,
contents and value references. Do not reject an existing database, renumber files or rewrite its history to enforce parity.
The odd-only rule governs new transaction output, not validation of historical file names.

After validating/recovering the existing committed history, check the selected append target before starting any new
writing transaction. If that TRL has an even ID, close it immediately through the ordinary file-ending/rotation path
and allocate a fresh odd-numbered TRL. Start the new transaction only in the odd file. This check precedes writing
`MagicStartOfTransaction` or any transaction payload, so parity conversion never splits a transaction and does not wait
for the file-size target. It applies when opening an existing database, restoring or adopting its canonical tail,
and whenever the writer would otherwise resume an even TRL. A valid odd append target needs no parity-driven rotation.

Use the normal allocator's collision/non-reuse rules and canonical allocation watermark, adjusted to choose a fresh
odd ID. Legacy files of any type remain reserved; an odd number alone does not make an existing identity reusable.
For example, an even tail 42 may continue in new TRL 43 only if allocation permits 43; otherwise use the next valid fresh
odd ID. This rule does not impose a new parity rule on historical KVI/PVL files or invalidate their reader pins.

Closing the even tail preserves its accepted transaction bytes; only normal file-ending and continuation mechanics
are allowed. The new odd file links to the old history normally. Rotation itself is a physical operation, not a schema
transaction: it consumes no event, changes no CommitUlong/canonical sequence, and does not detach followers. Remote
closure/continuation still requires leader authority and TRL CAS; a follower may prepare its own local writer but never
mutates canonical blobs. Interrupted rotation follows normal recovery and unused-file reconciliation before allocating
or resuming a successor. Legacy replay remains compatible, including transactions spanning files. All newly allocated TRLs use odd IDs.
The existing database is assumed to be present in Blob Storage already; importing a local database is out of scope.

#### Per-database term adoption fence

A new leadership term must fence outstanding requests against the writable predecessor TRL before authoring a new
continuation. Acquiring the leader.json lease alone does not fence another blob. Adoption therefore conditionally
changes/seals the current TRL version, preserving all published bytes and recording its successor/authority as required
by the chosen continuation codec. This CAS is on the TRL, not a database-state document. If a predecessor append or
rotation wins first, reread its result and follow the canonical continuation before retrying adoption.

Once adoption wins, pending requests carrying the old TRL ETag fail. Previously sealed files cannot be extended by a
correct session, and a stale session cannot regain permission by rereading tokens. Term/attempt-qualified successor
keys isolate unreferenced old uploads. The final accepted predecessor comes from the actual TRL CAS order, not from a
cached length or leader-local confirmation. All required databases must be adopted before ordinary canonical writing.

For unpublished additions, reconcile any ambiguous conditional genesis creation before initializing a fresh attempt.
Published genesis is preserved and restored, never replaced. Removed namespaces remain outside activation and require
no adoption. Initial creation/discovery and rotation must use the same single-winner rule; their exact representation
still requires failure tests and must not assume that listings or greater file IDs alone establish canonical ancestry.

### Cluster transition engine

Bootstrap, unplanned takeover, same-generation handoff, and application upgrade use one engine. Only preparation inputs
and lease acquisition differ. There is no second activation algorithm for shutdown or upgrades.

Every process starts as a restoring follower. Before any lease contention or acceptance of a planned lease transfer,
it obtains all published databases required for its candidate set through verified sealed-file reuse and bounded
parallel downloads, always redownloads the growing tail, and restores the complete selected recovery history. An incomplete download never permits candidacy,
even when no leader is reachable. The node does not hold the cluster lease while doing its initial bulk restore.
Removed databases are outside the candidate set; unpublished additions and an empty cluster have no published files to
download and follow genesis initialization. After acquiring authority, reread/adopt current state and reconcile any
progress since preparation before activating all databases; the initial download does not freeze the remote boundary.

| Entry | Preparation | Lease path |
| --- | --- | --- |
| Empty cluster | Empty predecessor database set; initialization cursor and inputs available for every instance; conditional leader placeholder | Acquire finite lease |
| Unplanned takeover | Reconstruct selected durable bases; recover any selected transition obligations | Randomized contention after leader-session loss or authority mismatch |
| Same-generation handoff | Exact selected database set; prepared target and final durable-base vector | Old leader's `Change Lease`, then target proves ownership |
| Higher-generation transition | Exact declared predecessor; continued bases, initialization inputs, explicit retirements | Prepared handoff preferred; eligible unplanned contention uses the same activation obligations |

| Phase | Permitted work | Completion / failure |
| --- | --- | --- |
| `Restoring` | Discover native KVI/TRL, reuse verified sealed files, download and replay; no application speculation over incomplete state | All required published bases restored -> `Following`. Missing/corrupt prerequisites keep the affected DB unavailable. |
| `Following` | Local application transactions, reads and node-local compaction on ready databases; compare canonical history; no remote publication | Eligible preparation -> `Prepared`; schema detachment sets permanent session ineligibility. Link loss alone grants no authority. |
| `Prepared` | Hold verified restore result and candidate configuration; no canonical authoring | Recheck eligibility immediately before lease acquire/transfer. Prove owned lease -> `Selecting`; failed acquisition returns to following/preparation. |
| `Selecting` | Under owned lease, reread leader record and CAS the next term/session, generation, DB set, endpoint and API key | Only proven CAS selection -> `Activating`. Ambiguity suspends progress and reconciles. Lost ownership -> `Fenced`. |
| `Activating` | Fence/adopt every existing published base, catch up any intervening durable progress, locally create missing genesis and queue immediate flush | Every existing base adopted/restored and every addition locally initialized -> `Leading`. Genesis/schema flush may still be pending; no remote-durability claim is implied. |
| `Leading` | Local commits, progress notifications and TRL range serving, serialized asynchronous publication, leader-only remote compaction and eligible remote cleanup | Authority loss -> `Fenced`; graceful departure -> `Draining`. |
| `Draining` | Per-DB transaction-boundary cut to `.temptrl`; reconcile previously issued data writes read-only; permitted authority operations only | Transfer/release/expiry then shutdown. No return to canonical writing in this session. |
| `Fenced` | No canonical authoring/publication under the lost term; reads/local work only where the existing mode permits | Rejoin through discovery/recovery, never resume lost authority. Permanent detachment/draining eligibility flags survive phase changes. |
| `RestartRequested` | Stop admitting new host work, finish existing work gracefully and close readers/writers; no detached suffix promotion | New process begins in `Restoring`; no assumed exit deadline for an application transaction. |

These are local permission states, not new storage records. Keep the following independent state explicitly:

- `leadershipForbidden` is latched for the lifetime of any schema-detached session; acquiring a lease, accepting a
  handoff and publishing candidate readiness all check it. Removing the affected DB or reconnecting cannot clear it.
- Per-DB mode is `NeedsRestore`, `CanonicalLocal`, `AwaitingGenesis`, `SchemaDetachedVolatile` or `RetiredVolatile`.
  Other DBs may continue following after one detaches, but node candidacy remains forbidden.
- Per-DB local committed, confirmed and Blob-published recovery positions are separate. Local activation, commit completion or
  successful peer delivery cannot advance the durable position. User-reader visibility also respects virtual batching.

The leader-record CAS is the term-selection point. Immediately on proven selection, reject predecessor live messages
and callbacks before they can alter confirmation/branch selection. Preserve valid already published predecessor history.
Adoption is a separate per-TRL fencing operation: lease ownership over leader.json does not fence data blobs.
After all existing bases are adopted and additions are locally initialized, send term-start controls identifying their
actual base/evidence and enter `Leading`. Do not wait for a moving input end or unpublished genesis/schema flush.
A local-only genesis can be streamed as local evidence; other nodes cannot label it Blob-durable or skip their published
base restore obligations. Ambiguous pre-existing genesis creation must be reconciled before initializing a replacement.

#### Transition ordering and interrupted execution

1. Build candidate eligibility from the selected generation/DB set, complete restore and session latches. Recheck it
   at dispatch of any acquire or handoff acceptance, not just when scheduling an election attempt.
2. Prove lease ownership, then conditionally select the leader record. A delayed result belongs to its originating
   operation/session; cancellation does not imply rejection or release ownership.
3. For every existing DB, read the canonical writable end and CAS its adoption fence. If an old append/rotation wins,
   follow its verified continuation and retry adoption there. No new canonical event authoring until every required
   predecessor is fenced; partial successful adoption is preserved for successor recovery.
4. Restore published history that advanced since preparation. Reuse only validated compatible optimistic transactions
   beyond the adopted end, otherwise obtain missing work from application input. Never copy a detached/volatile suffix.
5. Reconcile prior creation attempts for missing DBs, create local genesis under selected authority and queue immediate
   asynchronous publication. Complete required local schema changes under the same admission rules. Already published
   instances are restored rather than initialized again.
6. Enter `Leading` after those local/adoption obligations. Any publication that is still pending remains local-only;
   publisher recovery rules below decide actual durability, independently of application progress.

For detachment, latch `leadershipForbidden` before any queued election/handoff action can dispatch, stop acceptance for
that database, and switch to `.temptrl` at its next transaction boundary. Maintain the 15-minute no-valid-leader timer
using the [schema-detachment rules](#non-application-schema-transactions-during-upgrades). Valid leader evidence resets
only that timer, never the ineligibility latch. On expiry issue one graceful restart request; repeated timer ticks do
not start competing shutdowns. Stale leader responses cannot reset it or cancel a restart already requested.


Same-generation eligibility requires the selected database set and every published active durable base; unpublished
additions require the ability to initialize from scratch. Higher generations require a valid transition and available
initialization inputs; lower generations never lead. After acquiring authority, reread all bases because a pending
predecessor publication may have won in the meantime. Initial bootstrap applies the same rules to an empty database set.

The engine's recovery rule is always: inspect published per-instance results, restore existing history, and initialize
missing databases. Failure to restore/adopt or locally initialize one active instance prevents canonical writing for the entire cluster;
a pending asynchronous flush alone is not that failure. Selection/activation
deadlines remain explicit blockers, not assumptions that every retry eventually succeeds.

### Election, renewal, and fencing

Election triggering remains [leader-centered](#leader-centered-follower-sessions-and-election-triggering). A candidate
conditionally creates the initial placeholder if needed, then uses the transition engine. Lease ownership plus the
matching leader-record CAS is necessary; neither an authenticated peer message nor an available endpoint is authority.

The leader renews through an isolated authority client/pool, independently of database publication and transaction rate.
Failed or ambiguous renewal that cannot be reconciled before self-fencing stops canonical work for all databases.
Followers likewise stop new acceptance when authority cannot be proven, while existing reads and local speculation have
separate availability semantics. An ordinary leader-record read is not evidence of a fresh full lease lifetime.
The selected mechanism is short-lived confirmation grants, described below. Exact timing bounds and the implementation
proof remain B1; the choice of mechanism is settled.

A takeover first selects each database's durable predecessor, which may differ from another database's position.
Previously dispatched publication must be reconciled before deciding its end. The new authority may then copy validated
optimistic event groups after that exact end through the procedure below. Old cached physical file length is never the
cutoff. Removed databases remain outside activation and receive no tail adoption.

#### Short-lived confirmation grants

Decision recorded 2026-09-14: select variant A, short-lived leader-to-follower confirmation grants. Direct confirmation
is an optimization for validating speculative execution and detecting conflicting leader histories. It is not a vote,
a quorum acknowledgement, durable publication, or an independent source of leadership authority. Missing confirmation
does not block local event commits or eligible takeover, and matching bytes alone do not prove the absence of split brain.

The leader derives its conservative authority deadline from the monotonic time before dispatching the successful
lease acquire/renew request, with qualified clock margins. A delayed response never starts a new full local lease period.
A GET of the leader record does not refresh that deadline. Once the session self-fences, a late renewal response cannot
return it directly to canonical writing.

A follower requests a bounded confirmation grant using a unique challenge and its current connection/session identity.
Its local deadline is anchored before sending that challenge. The leader grants it only when the entire permitted
window fits within its own safe authority interval. Responses bind the challenge, connection and authority; delayed,
duplicate, expired or revoked responses cannot extend the follower's deadline. Grant duration, clock-drift margins and
pause/suspend handling must be qualified under B1, not inferred from network arrival time or wall-clock synchronization.
Expired grants stop new direct confirmation, not speculation or replay of already selected durable history.

Planned handoff stops grant issuance and waits until the maximum conservative expiry of all grants it issued,
including disconnected followers and in-flight responses. The M1 model needs only this single deadline, not individual
revocation messages or acknowledgements. Only then transfer authority. Direct confirmation and ordinary publication
need no follower acknowledgement. See [M1 authority evidence](M1Evidence.md#supported-mathematical-clock-model) for the
clock inequalities and named race tests; production clock qualification and role/commit integration remain B1.

As soon as a candidate successfully selects its own new term under the owned lease, it closes the predecessor's live
acceptance path and invalidates queued predecessor frames, grants and connection callbacks. After all active databases
are adopted and restored, it determines subsequent canonical history as leader. Old-leader messages cannot confirm,
overwrite, rewind, or veto its new history, even if they carry a formerly valid grant or a greater event cursor.
Historical bytes may be used only as TRL-published recovery data or validated optimistic-tail input under the normal
recovery rules, never as commands from the previous leader. Already published durable history remains binding; prior
direct confirmation is not a requirement to reuse an otherwise valid optimistic tail.

Role/term transitions, grant invalidation and confirmation updates must be serialized so an old callback cannot advance
confirmation after local takeover. Authority checks also cover in-flight leader work at the canonical commit/publication
boundary; local execution after fencing grants no canonical authority. Per-database TRL adoption CAS and term-qualified
object keys remain the remote publication fences. Confirmation is a consistency check layered over these mechanisms.

### Optimistic tail adoption on takeover

Optimistic TRL is reusable event execution, not an independent authority source. After acquiring the lease, selecting
the new term, and adopting/restoring all active durable bases, pin the candidate's committed optimistic groups at a
complete local transaction boundary. Pause fresh execution while preparing each database's continuation. An unfinished
transaction is excluded or completed before capture; committed transactions in an unpublished memory batch remain
individually eligible after validation. The memory publication boundary does not replace the TRL commit boundary.

1. Determine `C`, the last consumed event in the currently selected canonical database boundary, after reconciling
   pending publication. Load `eventsToSkip`. Verify the candidate's database/stream identity, canonical base lineage,
   contiguous event coverage, checksums, committed transaction status, and structural agreement with every overlapping
   canonical event. Cached evidence must bind the actual adopted history; any mismatch rejects the candidate's tail.
2. Exclude all optimistic events at or before `C`. Select the contiguous complete event groups strictly after `C`, up
   to the captured committed head. If a pending skip entry conflicts with a locally applied event, use input recovery
   from that event rather than copying it or dependent later effects. Missing/invalid groups likewise require recovery.
3. Append only the selected complete event transactions after the adopted end in the new term's lineage. For example,
   canonical history through 102 plus locally committed events 101-105 copies only 103-105. Their boundaries already
   exist in TRL even if the memory batch spans 101-105; no batching-driven reframing or delta rebasing is needed.
4. Reuse validated transaction payloads without rerunning handlers. Verify predecessor metadata and physical file/lineage
   identity as part of the normal append contract. Do not blindly concatenate files or relabel local value pointers.
5. Publish only complete canonical transactions through the existing serialized publisher. Use conditional append/CAS
   and operation identities; reconcile a lost response before retrying. A restart rereads the selected end and copies
   only events beyond that end, so a partially published attempt cannot duplicate already selected events. Unselected
   append bytes remain staging data and are handled by the existing append reconciliation rules.
6. Reopen/replay the resulting canonical file generation before admitting fresh handlers, so BTree value pointers,
   ObjectDB counters, caches and the next input cursor correspond to the copied canonical bytes. No application handler
   runs again for the copied events. No historical BTree root is needed. After cursor `E`, consume only inputs after `E`.

This copying path is permitted only for validated optimistic TRL from the same stream and adopted logical history.
Rollback-attempt evidence associated with the copied history must satisfy B3; copying successful commits must not hide
a different rollback history.
Pending additions have no writable seed to promote. Shutdown/retired/schema-detached `.temptrl` remains disposable and cannot be promoted. If no eligible optimistic
cache survives, restore the durable base and consume missing upstream events normally. All-active-base activation and
normal authority checks still precede canonical publication; tail adoption runs before fresh application execution.
Exact transaction selection, retry and physical reopen mechanics are part of B3, not a second publication protocol.

### Fast graceful handoff

A planned process shutdown stops remote publication while ordinary local transactions and compaction continue.
No scratch collection, temporary extension, allocation switch or core transaction barrier is required.

1. The leader sends `steppingDown` over active follower sessions and stops new remote publication, checkpoint uploads
   and remote deletion through their independent cancellation/authority controls. Local maintenance may continue.
2. Already dispatched storage requests may still land. Reconcile ambiguous canonical TRL writes using the existing
   rules; cancellation cannot undo a completed remote write. No shutdown flush of later local transactions is required.
3. Application commits and rollbacks continue in ordinary local files. The departing session never resumes publication.
4. On restart, ordinary cache validation against selected Blob history discards unselected files and replaces divergent
   tails. There is no special `.temptrl` cleanup, separate directory, or frozen local generation.
5. In parallel with stopping publication, the leader builds the eligible set from its connected-follower registry. A
   same-generation candidate must match the selected database set, run a compatible protocol/BTDB format, expose a usable
   candidate endpoint, be within the handoff lag limit for every active database, and retain the events needed after
   every final durable base. It chooses randomly and sends the handoff offer over that follower's existing authenticated
   session. A prepared higher-generation target instead follows the database-set transition readiness and initialization rules above
   and has priority over same-generation shutdown targets.
6. The target pulls and verifies TRL bytes and prerequisites through each relevant shutdown canonical cut when available,
   and independently verifies every object-store-selected durable base. A higher-generation target additionally
   verifies starting cursors and input availability for its added databases and explicitly lists retirements. The old leader
   may serve already existing bytes but cannot create or persist new canonical ones. The target responds `prepared` with
   progress vectors, proposed initialization cursors, and a freshly generated proposed Azure lease ID. An unpublished old-leader
   tail remains an optimization only; correctness permits dropping it and replaying its ordered application events.
7. The old leader keeps only the authority/control lane alive: leader-record reads, lease renewal, reconciliation reads,
   `Change Lease`, and eventual lease release. These operations preserve or transfer fencing authority and are the sole
   Blob-plane exception to the no-persistence rule; no TRL append, index, checkpoint metadata update, PVL/KVI
   write, or object deletion is dispatched. After the confirmation-grant drain defined in
   [Short-lived confirmation grants](#short-lived-confirmation-grants), it calls `Change Lease` with the target's proposed ID.
8. The target proves ownership by renewing with that ID and enters `Selecting` in the
   [cluster transition engine](#cluster-transition-engine). The common `Activating` phase restores/adopts every required
   durable base and completes the selected database set obligations before canonical work. Handoff bytes are an optimization,
   never a substitute for a published durable base.
9. The old process may continue executing application events against its volatile generation until exit. It closes its
   sessions after observing the new leader record or reaching its local shutdown deadline, releases the volatile root,
   and leaves disposable local files for ordinary startup cache validation. None of its post-cut work needs to be drained or transferred;
   restart cleanup is authoritative if clean deletion was interrupted.

There is no cluster-wide application quiesce and no core transaction-boundary switch. An application/host deadline
may instead terminate the process through non-graceful recovery; replication does not synthesize a rollback or skip. Once a node session enters volatile shutdown mode it never resumes
canonical persistence, even if a target disconnects or shutdown cancellation is withdrawn. A failed prepared target is
removed and another eligible follower may be selected while the old leader continues volatile execution. If no target
succeeds before the deadline, the old leader releases or lets its finite lease expire and exits; normal election adopts
the final durable vector and replays later events.

If `Change Lease` has an ambiguous response, the old leader remains permanently non-canonical and the target probes by
renewing with the proposed ID. If the target owns it, handoff continues; otherwise no node authors canonical work until
ownership is reconciled or the finite lease expires. The old node may continue volatile execution, and followers may
continue non-authoritative speculation. If the target crashes after transfer, normal lease-expiry election recovers. The
common failure mode is a canonical-publication pause and event replay, never overlapping leaders or blocked application
execution.

### Canonical TRL publication

The leader captures the ordered TRL ranges through the final commit marker of each application transaction or explicit
schema transaction. Application commits store their input cursor; schema commits preserve it under the upgrade rules. Only a complete transaction gets a canonical position; progress announces its TRL end. Local accepted
progress for ordinary application transactions can be streamed immediately under valid authority and queued for
background durability. Non-application commits instead trigger the immediate asynchronous flush request.

The [TRL publisher](#trl-publisher) batches ordinary committed transactions and conditionally publishes their TRL
suffixes. A successful canonical TRL CAS is durable publication and supplies `objectStorePublished`; no subsequent
state document selects that result. Genesis/schema operations request immediate publication after their local commit.

Rotation within or between transactions follows the publisher's transaction-aligned boundary rules. A rejected or ambiguous
append is reconciled against actual version, content, length and identity before retry. Already published complete
transactions cannot be abandoned as an unselected tail simply because a separate state update was not performed.
Graceful drain dispatches no new data request and reconciles earlier TRL CAS operations read-only.

### Progress notification and pull-based comparison

Decision recorded 2026-09-14: the baseline peer protocol announces a committed TRL boundary and lets the follower
fetch the bytes. It does not push per-transaction envelopes or transaction payloads. A notification, within the
already established database-instance and authority/session context, contains only:

```text
TrlProgress
    eventId
    trlFileId
    trlPosition
```

`trlPosition` is the exclusive end of a complete committed transaction in the leader's canonical local TRL, not the
current length of a writer's partially emitted transaction. `eventId` is its committed CommitUlong. This advertises
leader-local progress; it does not establish Blob publication or advance a durable recovery boundary. Protocol version,
database identity and selected authority/lineage are connection/control context, not repeated transaction descriptions.
No sequence, transaction-kind field, range list, payload or per-notification hash is required in this progress payload.
Internal canonical ordering/ancestry and storage publication proofs remain separate from this notification format.

A follower retains its last compared position in the leader's TRL namespace, independently of its own local file IDs
and offsets. It requests bounded byte ranges from there through the announced end and follows verified file
continuations across rotation. Range requests/responses bind the selected session/term and exact file identity;
changing leader or reusing a numeric file ID in another lineage must never substitute different bytes. Appending later
transactions may extend the file but cannot change an already advertised prefix. Transfer pins protect bytes while a
response is being read; missing retained ranges return an explicit unavailable result rather than fabricated data.

Decode the fetched native TRL to recover transaction boundaries, cursor/metadata changes and ordinary rollback
attempts. Compare complete equal event coverage, including rollback evidence, with the local history. A partial HTTP
chunk or a file boundary is not a transaction boundary. Verify closure and continuation before advancing confirmation;
revalidate authority/grant and session when accepting the result. A read that outlives its session cannot confirm work
under a replacement leader. Local application commits never wait for these requests or the comparator.

Notifications may be coalesced: the newest complete boundary subsumes earlier notifications on the same verified
lineage. A schema commit changes the TRL position without changing eventId and must still notify/fetch; never suppress
progress just because eventId is unchanged. It is discovered from native TRL and follows normal live-follower detachment.
Rollback bytes preceding the next commit remain in the downloaded interval without a synthetic rollback message.
A lost notification is recovered by reannouncing the latest boundary on reconnect or subsequent progress.

If leader bytes are unavailable, use the verified published Blob TRL where available; otherwise use the existing
recovery/input-replay path. Input replay regenerates unavailable unpublished application work, but does not by itself
prove equality with unavailable leader bytes. Missing evidence never advances confirmation. Blob publication remains
established by the storage publisher, independently of this local-progress notification.

Defer any optimization that includes bytes with the notification or prefetches them to avoid an extra request. Such
an optimization must feed the same validated-byte comparison path and preserve the same boundary/authority semantics;
it is not part of the first implementation or a second transaction protocol.

### One follower acceptance path

Startup correctness comes from canonical checkpoint/TRL replay; a running follower confirms by structural comparison. The same validated transaction representation
is reconstructed from leader-fetched native TRL or published canonical TRL and its bound metadata. Transport/storage adapters
supply bytes and evidence; they do not implement separate acceptance rules.

| Source | Required evidence before transaction validation |
| --- | --- |
| Leader-fetched TRL range | Authenticated session, selected database set/instance, current authority under B1, and valid resume lineage |
| Durable replay | TRL-published or explicitly retained recovery closure, canonical TRL ancestry, and exact hash-verified cuts; historical author authority need not still be live |
| Local speculative bytes | Comparison evidence only over the same event coverage; structural equality does not make these canonical physical bytes |

For one candidate transaction:

This path applies only to the current follower/recovery role. A locally selected new leader discards predecessor live
messages before this path can mutate confirmation; historical durable replay uses its own source evidence below.

1. Validate the source evidence, complete shared identities, ordered range bounds/hashes and continuation links, payload and transaction
   terminator. No partial transaction can advance a root. Fetch and verify prerequisites before entering writer
   serialization; file installation must preserve retained-reader generations (I9/B3). For a new schema transaction on
   a live follower, validate its ancestry against the received canonical chain or verified published TRL chain, then
   detach under the upgrade rules and stop this path. This does not require the local confirmed watermark to have
   caught up with that received chain. A schema transaction already covered by bootstrap is only a duplicate.
2. Classify its relation to the accepted position. An exact previously accepted duplicate is a no-op. Conflicting
   fetched bytes/transactions for the same canonical history are a safety fault: freeze acceptance and retain evidence. A gap cannot
   advance; resume or rebuild. A new transaction must extend the exact confirmed position and validated lineage.
3. Compatible startup replay applies schema transactions normally. For application transactions, select the action
   from the table below. A successful comparison records the leader result identity while keeping local physical bytes separate. The comparison optimization grants no additional authority.
4. Advance confirmed progress only after the chosen action completes. Publish readiness according to the read contract,
   separately from speculative and durable progress. Report bounded/redacted logical diagnostics on divergence;
   virtual batching causes no transaction-byte difference; actual content and lineage remain validated. Applied/skipped disagreements are outcome
   mismatches. Different-range digests alone cannot establish logical divergence.

| Local condition | Action |
| --- | --- |
| New schema transaction on a live follower | Detach the affected database into local volatile execution; no schema application, confirmation or mismatch restart. |
| Schema transaction during compatible startup replay | Apply canonical TRL normally; preserve input cursor and advance canonical sequence. |
| Complete matching event coverage and validated identical transaction payloads | Advance confirmed metadata; current BTree and local files remain unchanged. |
| Follower has not consumed the complete leader range | Retain the newest announced end and bounded fetched data; await local coverage, without restarting for lag. |
| Structural mutation or outcome mismatch | Fence and restart follower; rebuild canonical state in a fresh process. |
| Unrecoverable missing/corrupt comparison data | Restart/rebuild; do not accept unverified progress. |

The [structural comparison rules](#per-event-trl-boundaries-and-independent-batching) define byte comparison and restart.
Virtual memory batching does not change log grouping. Physical identity is validated separately from transaction content. A matching result does not grant the
local bytes canonical physical identity or provide a readable historical root. Comparison encoding is B3.

Local compaction is optional and independent on every node. It works on an eligible local current head without
waiting for confirmation or performing live rebase; it never enters follower acceptance.

### Peer transport abstraction and Kestrel adapter

The core depends only on the peer transport and peer endpoint contracts described in the testability section. It does
not reference HTTP, ASP.NET Core, Kestrel, URI routing, headers, or sockets. Reconnect and resume are expressed in terms
of the shared `ResumeToken` and typed transport outcomes.

The preferred version-one production adapter uses resumable server-streaming HTTP plus small follower-to-leader HTTP
requests rather than WebSockets. Progress notifications and controls flow from leader to follower on one long-lived
response; status and handoff responses flow back as separate authenticated requests. Native TRL data is fetched through
separate bounded HTTP range requests. Together
they form one logical leader session and do not require follower-to-follower connections. A small
`BTDB.Replication.AspNetCore` integration package registers the HTTP adapter and maps it into the parent application's
existing pipeline, conceptually:

```csharp
builder.Services.AddBTDBReplication(...);

var app = builder.Build();
app.MapBTDBReplication("/_btdb/replication");
```

The parent owns Kestrel listeners, TLS or mTLS, request limits, logging, and deployment routing. The adapter extracts
the current leader-record bearer API key and invokes the same transport-neutral authorization and peer endpoint used by
the in-process adapter, while allowing the parent to add stricter authorization. The library must not start a second
hidden web server or reserve a port. Endpoint routing supports library extension methods on `IEndpointRouteBuilder`,
which fits this composition model.

A strawman HTTP wire mapping for the transport-neutral operations is:

```text
Authorization: Bearer <apiKey-from-cluster/leader.json>

POST /_btdb/replication/v1/session
    request body: FollowerHello + per-database resume vector
    response body: length-delimited LeaderMessage stream

POST /_btdb/replication/v1/sessions/{connectionId}/status
POST /_btdb/replication/v1/sessions/{connectionId}/handoff-response

GET /_btdb/replication/v1/databases/{databaseName}/{databaseInstanceId}/artifacts/{artifactId}
    Range: bytes=<start>-<end>
```

Database routes accept only short names and instance IDs active in the selected database set and never map arbitrary path
text to a filesystem path. Session IDs must belong to the currently open authenticated node/session connection.
Handoff offers and the shutdown progress vectors are leader-stream control messages: they distinguish each continued
database's last direct canonical cut from
its possibly older durable base and carry initialization/retirement declarations for a database set transition. A target's response is
cluster-wide and covers every database active in the database set it proposes.

There is no compaction-result route: neither local nor remote compaction sends results to running peers. Leak
removal uses the separately injected parent event publisher, not a replication HTTP route.

The shared codec produces a binary content type and length-delimited frames. The HTTP adapter can expose
`HttpResponse.BodyWriter` as a high-performance `PipeWriter` and explicitly call `FlushAsync` at bounded byte/time
intervals so small transactions do not remain buffered indefinitely. HTTP/2 is preferred for multiplexing streams and
artifact requests, but the framing and resume cursor do not depend on one TCP connection surviving.

Heartbeat frames keep an otherwise idle stream observable. The shared transport contract defines cancellation,
backpressure, bounded queues, maximum frame/artifact sizes, status sequencing, session replacement, and reconnect
behavior. HTTP adapter qualification adds reverse-proxy buffering, idle/request timeouts, slow consumers, and reconnect
storms. A slow follower must reconnect from canonical storage or an available retained range rather than forcing
unbounded leader memory.

WebSockets remain a possible adapter if version two benefits from carrying both directions on one upgraded connection.
They add upgraded-connection limits, ping/pong timeout policy, and different proxy behavior; version one does not need
them for a server stream plus occasional status requests. gRPC server streaming is another viable adapter, but requiring
HTTP/2 and Protobuf is unnecessary for the transport-neutral core.

ASP.NET Core references:

- [Request and response body pipelines](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/middleware/request-response?view=aspnetcore-10.0)
- [`IEndpointRouteBuilder` mapping extensions](https://learn.microsoft.com/en-us/dotnet/api/microsoft.aspnetcore.builder.endpointroutebuilderextensions?view=aspnetcore-10.0)
- [WebSockets support and keep-alive behavior](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/websockets?view=aspnetcore-10.0)
- [gRPC services with ASP.NET Core and Kestrel](https://learn.microsoft.com/en-us/aspnet/core/grpc/aspnetcore?view=aspnetcore-10.0)

### Native KVI publication

Decision recorded 2026-09-14: use the native KVI as the published restore point. Do not introduce a separate checkpoint
object, manifest, current-checkpoint pointer or checkpoint-selection CAS. References to a checkpoint elsewhere mean
this KVI and its required BTDB files, not another persisted protocol object.

1. Capture one committed root on durable canonical ancestry and pin its source files/byte cuts. Plan the target Blob
   inventory and serialize KVI with remote references through the [remote export path](#separate-node-local-and-blob-compaction-modes);
   this is not a local-compaction checkpoint or a rewrite of the live local root.
2. Finish uploading every required PVL and publishing the canonical TRL chain through the exact `(fileId, offset)`
   stored in the KVI. Prove those operations completed, including reconciliation of ambiguous responses. Keep these
   prerequisites available throughout KVI upload. Queued writes or uncommitted remote blocks are insufficient.
3. Only after step 2 completes may any KVI upload request be dispatched, including staging its first remote block.
   Local staging/serialization of the remote KVI may happen earlier; remote KVI transfer must not overlap unfinished prerequisites.
   The barrier uses the KVI's fixed cursor, not a continually moving tail: later TRL bytes need not finish first.
   Under the current leader's serialized publisher, write the complete native KVI to its fresh canonical file identity.
   Successful object publication is the KVI publication point. No subsequent pointer/manifest update is required.
4. Only then may the leader delete old files no longer needed by this KVI, the required canonical tail or pending
   publication work. Retain files still referenced by any of those. Local reader pins remain a separate concern.

A failed KVI write cannot enable deletion; an ambiguous result must be reconciled before deletion is allowed. Discovery
lists native database files and selects a valid published KVI using BTDB file identity/generation and canonical lineage,
then follows the canonical TRL. A listing is candidate discovery, not evidence that an arbitrary unrelated file belongs
to that history. Preserve compatibility with the existing DB already in Blob Storage. Exact stale-leader publication,
file-selection and delayed-listing races require B5 tests, not a second checkpoint publication mechanism.

The in-memory `CheckpointReference` may retain the selected KVI identity/hash, position and physical layout; it is not
another uploaded object. Ahead-of-durable captures wait or are abandoned, stale captures are rejected, and equal-position
physical replacements remain allowed. Initialization uses ordinary genesis TRL without a separate seed manifest.
Before the first KVI exists, recovery uses the complete published genesis TRL chain; it does not wait for an empty KVI.

## Candidate object layout

An illustrative, deliberately non-final namespace is:

```text
cluster/
    format.json
    leader.json
databases/<database-name>/<database-instance-id>/
    kvi/<generation>-<file-id>.kvi # native KVI, published after its prerequisite files
    trl/terms/<term>/<file-id>-<lineage-id>.trl
    trl/indexes/<term>/<attempt-id>/<index-hash>.json
    objects/<term>/<attempt-id>/sha256/<content-hash>
    attempts/<term>/<leader-session>/<attempt-id>/...
    gc/...
```

Important properties are more significant than the exact names:

- Published KVIs are immutable; term-qualified active TRLs preserve every previously selected prefix.
- The leader record selects the generation and database names; published per-database metadata retains published initialization metadata. The leader record contains no mutable database progress. A published database view never references an object that was
  not uploaded first.
- Leadership term and lineage are explicit in mutable-TRL or range keys, so an old leader cannot extend a new term's
  accepted lineage.
- Leader-authored `.trl` bytes and every referenced file must be available on each replica that uses them. Local
  compaction independently changes each replica's `.pvl` layout; remote compaction exports a separately mapped KVI.
  TRL contains no physical compaction rewrite. Every replica may have a different physical layout, but
  every reference must resolve to the same logical value and remain within files protected by retention rules.
- Use attempt-qualified immutable object keys. Once retired for deletion, a key is never selected or reused again,
  even for identical bytes; a delayed old delete must not remove a future selected object.
- The published KVI and canonical TRL must resolve every required file from Blob Storage; recovery must not depend
  on an unpublished local cache file.
- The current leader makes superseded remote files eligible for deletion only after publishing a complete replacement
  recovery closure. Plan a configurable delay, provisionally about one day from becoming obsolete, before actual Blob
  deletion. This reduces restore/cleanup races without tracking followers or adding restore leases. The delay is an
  operational policy, not a correctness requirement or a guarantee that an old KVI remains recoverable.
  Local cleanup remains independent and protects ordinary local readers and current-tree references.

### KVI publication and deletion interruption cases

| Interruption | Recovery requirement |
| --- | --- |
| Prerequisite upload fails or is cancelled | KVI upload has not started; keep current recovery files. Staged artifacts do not authorize cleanup. |
| KVI write is rejected/failed definitively | No cleanup based on that candidate. Re-evaluate the currently published KVI and ancestry. |
| KVI response is lost | Reconcile that exact immutable file identity/content before treating it as published and deleting anything. |
| KVI succeeds, process dies before cleanup | Restore can use the new KVI; extra old remote files are harmless and may remain. |
| Cleanup stops halfway | Current KVI and required tail must remain complete; resume deletion only from a freshly verified reachability set. |
| Old session's delayed KVI write lands after adoption | Must not make an unrelated or regressive restore point eligible, or allow a delayed delete to remove current recovery files. This needs the B5 qualification below. |

Native KVI publication removes the need for a pointer/manifest; it does not by itself prove cross-session GC fencing.
B5 must establish how file generation, canonical lineage and stale publication are qualified during native discovery.
Local publisher serialization alone cannot fence another session. Likewise, permanent key non-reuse protects newly
allocated objects from old deletes, but does not by itself protect old objects that a successor still needs. The protocol
must qualify inherited in-flight deletes against the successor's recovery set before relying on them being harmless.
These are proof/codec tasks within KVI-last publication, not authorization to introduce another state CAS.

### Immediate remote cleanup and interrupted restore

Publish and verify all prerequisite value files and required canonical TRL first, then write the new KVI last.
Only after that KVI is successfully published may remote files absent from the current recovery closure be deleted
immediately. There is no separate checkpoint selection write. The KVI must restore the database through the selected
durable boundary without the removed files. Files shared with the new KVI or its required tail remain necessary.

A follower restore does not pin remote objects. If a file disappears while it is opening/downloading, the follower
abandons that restore attempt and restarts through the host port. Startup rereads current database state and opens the
newest published KVI and its required files; it does not retry the old KVI indefinitely or ask GC to wait.
The restarted process re-inventories its cache against the latest selected closure, preserving only verified reusable files.
An already running follower keeps using its local files according to its own reader/current-tree pins; remote deletion
is not a command to delete those local files.

No automatic old-KVI fallback retention is required. Keeping an old KVI identity for diagnostics does not pin
its files or promise recoverability. Backups remain a separate administrative facility. A file missing from the still
current published closure is a publication/storage fault, not an expected GC race; keep the node unavailable and report
it instead of serving partial state or asserting a newer checkpoint exists. Repeated restore restarts are observable.
The operational deletion delay reduces these races; restore must still handle missing superseded files.

B5 covers direct TRL publication/continuation/fencing as well as publish-before-delete ordering, staging protection
and permanent key non-reuse. Old-term in-flight deletes must remain harmless to the current closure; follower restore tracking is absent.

## Event application, checkpoint, and recovery sequences

### Normal event application

The [event-log contract](#event-log-contract-and-deterministic-replay) owns synchronous event execution.
[Canonical TRL publication](#canonical-trl-publication) owns the leader's capture/preparation path;
[pull-based comparison](#progress-notification-and-pull-based-comparison) owns follower confirmation/repair. These are the
same paths during ordinary operation and catch-up. A draining or retired instance instead follows the
[volatile core path](#minimum-opt-in-btdb-core-extensions), returning explicitly local results with no canonical position.

### Per-database replay hierarchy

Replay is independent for every active database instance; the shared leadership term does not create one cross-database
replay cursor. A replica reconstructs one database in this order:

1. After the previous process/readers terminate, discover the current remote recovery set, reuse matching verified
   local files and download only missing/mismatching files using the startup transfer pipeline below.
2. If a published KVI exists, fetch that selected native KVI and its verified required files directly from Blob Storage.
   Without KVI, restore the selected genesis/TRL chain. No peer bootstrap message or separate manifest/pointer selects recovery.
3. Follow the published TRL continuation chain and exact validated cuts from the KVI's TRL cursor. Use the
   [common acceptance path](#one-follower-acceptance-path) in durable-replay mode for ordinary TRL transactions and
   validated file transitions inside or between transactions. Unselected bytes/objects are ignored even when physically present.
4. Stop exactly at the verified published TRL chain's final `(fileId, offset)`. Require the decoder to have no open transaction and the
   resulting canonical sequence, frame-chain hash, event position, and root identity to match the selected boundary.
5. After reaching the Azure durable boundary, connect to the endpoint from `cluster/leader.json`, receive the latest
   TRL progress and pull bytes starting at the verified leader-TRL resume position.
6. Consume the corresponding upstream application events to build the follower's speculative suffix and divergence
   evidence. A new leader first reuses validated optimistic TRL beyond the adopted end; unavailable events are regenerated
   from the ordered input source.

A planned handoff verifies the final Azure durable cursor for every continued active database before transferring the
leader lease and also reports how much of each newer shutdown canonical cut the target already has. A database set transition
additionally verifies each added database can initialize and excludes removed databases from activation. Exact direct bytes may
reduce replay, but transfer does not wait for the canonical cut to become durable and never treats an unselected tail as
recovery truth. Both planned and unplanned takeover adopt each continued published TRL boundary, copy validated optimistic
events beyond that end, and consume missing inputs only where cache reuse is unavailable; unpublished additions initialize from scratch, and the planned path is faster because the target and its caches
are already prepared.

A node may serve an already accepted local read while another database is replaying, subject to per-database readiness.
It cannot become cluster leader until every database active in the database set it would select satisfies election or initialization
eligibility and its application generation is not below the current floor.

### Checkpoint publication

A leader-side interval or remote maintenance request can trigger [checkpoint capture](#native-kvi-publication). Every result uses the common
publisher's `PublishKvi` intent. Follower delivery remains bootstrap-only: selecting a newer checkpoint never
pushes its KVI into an already running follower.

### Leader transition

Use the [cluster transition engine](#cluster-transition-engine) for all entry paths. Lease transfer changes only how the
target proves ownership. It does not bypass per-database activation, initialization, durable-base restoration, or the
all-active-databases readiness gate. Remote checkpoints/compaction use leader paths after activation; local compaction remains node-private.

### Restart or new replica

1. Read `cluster/leader.json`, validate its selected generation and database names, compare them with the
   node's own application generation/database set, and read state only for relevant database instance IDs.
   Compare application generations and name sets: an older node ignores unknown additions and treats its names omitted
   by the newer generation as removed. A valid newer configuration leaves unpublished additions awaiting leader-only initialization.
   Equal-generation name-set differences are configuration faults. No durable retirement history is needed.
2. For an active database, a state naming another term/session means the current lease holder is activating or has
   failed. The node may retain or prefetch the previous accepted state, but it does not accept current-term frames or report
   current-term readiness until an adopted state is observed.
3. Ensure the previous process/readers/writers have terminated. Delete volatile `.temptrl` and unneeded/invalid local
   files, but preserve candidates for checksum-verified reuse. Never upload a longer local file or treat local-only
   files as recovery authority. No simultaneous old/new full cache copies are required.
4. Discover the latest published native KVI and canonical TRL boundary. Use the startup transfer pipeline to verify
   reusable files, download missing prerequisites in parallel, and replay available TRLs in order. With no KVI, obtain
   every TRL in the selected database's canonical history, starting with the lowest file ID. If remote cleanup removes
   a required file, rediscover the latest recovery set and retain still-valid verified local files.
5. Verify object hashes, sizes, database identity, BTDB format, TRL/optional-index linkage, ordered cross-file transaction ranges, and transaction
   closure before opening the restored database.
6. Use the common canonical validation/replay path in the temporary generation to the exact selected durable boundary.
   Require no open transaction at end
   and verify canonical sequence, frame-chain hash, TRL cursor, root identity, and event position.
7. Atomically install the fully verified local generation. A crash before installation leaves it unselected; a later
   restart discards the partial local generation and rereads current Blob state; a rename is not recovery authority.
8. Verify that every file referenced by the installed root remains protected, and initialize ObjectDB and
   application state before new event execution (B3). Step 6 already replayed post-KVI transactions; do not apply them
   again. Later local cleanup creates no KVI and has no canonical effect.
9. Read the current endpoint and API key from the leader record, connect the resumable leader session, and start
   proposing the corresponding upstream events from each accepted event position. An unpublished addition instead waits for leader-only initialization; a removed instance is outside cluster recovery and any optional local reopen uses `.temptrl`.
10. Only after every required published database has been fully downloaded from Blob Storage and validated/restored
   locally may the node attempt leadership. Apply generation and initialization eligibility as well. Startup is always
   follower-only until this gate completes; an unavailable leader does not waive it.

If a restore file disappears after a newer checkpoint was selected, restart and discover the newest published KVI.
There is no guaranteed old-KVI fallback. If the current selected closure itself is incomplete or corrupt, report
unavailability; never initialize over published history. Only an unpublished new database follows the empty-initialization
rule. Followers never independently declare their replay canonical.

A missing or corrupt local file discovered while running invalidates the affected local generation. The node stops
reporting that database ready and rebuilds from the selected durable boundary. If the current leader loses any part of
an accepted but not yet object-store-published canonical tail and cannot prove the exact bytes and roots from another
verified source, it self-fences instead of inventing a replacement in the same term; a higher term rewinds to the last
durable boundary and replays retained events. Losing every node's local disk is therefore recoverable under the stated
object-store and event-retention assumptions, while insufficient durable inputs cause explicit unavailability rather
than a corrupted database.

### Fast startup with verified cache reuse and ordered replay

Performance target: complete startup within 15 minutes for a large database of approximately 100 GB, including a
cold Blob-to-local restore. Measure from startup through discovery, download, local writes, validation, KVI open and
TRL replay to database readiness, not just the transfer interval. This is a target to qualify, not a measured guarantee.

Transferring 100 GB in 900 seconds alone requires about 111 MB/s of sustained payload throughput (decimal units).
Allow headroom for the remaining work and overlap download, disk writes, validation and ordered replay where possible.
Optimize bounded cross-file and large-file range concurrency against real Azure/network/disk measurements. Record
end-to-end time, transferred bytes, sustained throughput, phase times and peak memory; include cold and verified-cache
starts. Choose transfer sizes and concurrency from these measurements rather than fixed speculative tuning.


Decision revised 2026-09-14: local disk is the scarce resource; remote space need not be minimized aggressively.
The blanket delete-all-cache restart rule is withdrawn. Blob Storage still selects truth; surviving local files only
avoid transfers when verified against that selected history. Remote cleanup may be deferred operationally, while
KVI-last publication remains its earliest permitted point. Do not retain extra remote history as a mandatory restore
pin/grace policy, and do not download obsolete remote files merely because they are still listed.

For each required sealed file considered for reuse, compare database/file identity, selected object version, byte length
and a whole-file checksum. The last growing TRL is excluded from local reuse and always downloaded again.
Use SHA-256 for newly published checksum metadata. Recompute the local checksum by streaming the actual local bytes;
a cached checksum, timestamp, filename, equal length or ETag alone is not enough. Match permits reuse in place without
network payload transfer or a second local copy. Checksum verification reads the whole local file, so it saves network
traffic rather than eliminating disk I/O. Limit concurrent hashing to avoid starving replay. Keep verified files stable
until handed to the reader; active old writers must already be stopped.

Publish checksum and length with the exact blob contents/version, preferably in the same final object publication.
Sealed TRL/PVL/KVI files need one final whole-file hash. Do not calculate or maintain a whole-file checksum while a TRL
is growing. On every restore, download the last active TRL again from its selected published version, even if a local
copy has the same length or incidental checksum metadata. Read its ranges against one version token; if it changes,
reconcile/retry rather than mixing bytes from different versions. Ordinary transfer integrity and transaction validation
still apply. After sealing, calculate its final checksum and make it eligible for verified reuse on subsequent starts.
A legacy sealed blob without a trustworthy checksum falls back to normal version-bound download and validation.
The adapter details are in [ObjectStorages.md](ObjectStorages.md#whole-file-checksums-for-cache-reuse).

Download with bounded parallelism across files and optionally within a large file. Prioritize the next replay dependency
and schedule subsequent TRLs in ascending numeric file-ID order, allowing a bounded byte-based lookahead. Completion may
be out of order, but a single decoder consumes only the next complete verified file; replay of N can overlap download of
N+2/N+4. Never skip a missing predecessor or expose a partial download as a complete file. A transaction spanning files
keeps its decoder state until its commit/rollback terminator is available, without publishing a partial root.

With no KVI, all canonical TRLs are required: start at the lowest ID and replay upward while subsequent files download.
Exclude unlinked staging and abandoned branches using canonical lineage, not number alone. With a KVI, fetch it and its
required value/log files and replay the selected suffix; old retained remote KVIs/TRLs outside that recovery set need
not consume local disk. KVI restore can start once its prerequisites are available; background download/replay may
continue, but election remains forbidden until all required databases are fully restored and verified.

Use one local file per download, marked incomplete until length/hash/version checks pass; installation can rename it
without copying. After old readers terminate, delete an unusable old destination before fetching its replacement when
it is not required elsewhere. Bound download lookahead, RAM buffers and hashing concurrency rather than keeping an
extra full cache generation. Replaying a TRL does not make it locally deletable: the BTree may still reference its values.
Cleanup must preserve current roots, readers, pending comparison and replay dependencies. Actual disk exhaustion still
fails the node; this pipeline is normal startup scheduling, not a special low-disk mode.

Source check: the existing Azure file collection already uses `DownloadParallelism`, but its initial reconciliation
compares lengths and treats some local files as upload candidates. Reuse that concurrency infrastructure only after
replacing those standalone assumptions for replication; equal-length content is not currently checked there.

### Fatal local disk exhaustion

Local disk exhaustion is a fatal node error, including exhaustion during optimistic append, compaction, scratch writing
or restore. Use the existing node-unavailability/restart path: fence canonical publication, stop serving as ready and
terminate the old process/readers before removing obsolete or invalid local files. Restart reuses verified files from
the latest published recovery set, downloads missing files, replays canonical TRL, and uses application-provided input for any missing unpublished
tail. Losing unpublished history is acceptable because the retained ordered input can regenerate it; neither storage
fencing nor event retention can be bypassed by reporting a skip for an I/O failure.

Deleting obsolete locally pinned or uncompacted files before download can make the rebuilt state smaller. This is
possible, not guaranteed: if the current database plus download/replay/normal working space still cannot fit, the node
remains unavailable. No special quota mode, low-disk compaction mode, preservation of obsolete cache, or simultaneous
old/new full copies is required. Valid needed files may be reused in place. Failure/restart diagnostics remain ordinary operational observability.

## Read-serving and command consistency

A follower has one current BTree plus a confirmed-position watermark. Replication retains no old confirmed snapshot.
Write transactions start from the current local head; leader distribution does not throttle application execution.

- **Local confirmed read** is available only when a newly opened ordinary read transaction's event position is already
  structurally confirmed. If the head is ahead, route a strict read to the leader or wait for that specific read snapshot
  to become confirmed. Such a request owns an ordinary bounded-lifetime reader; replication does not retain every root.
- **Optimistic local read** reads the current local state and reports its event position and the confirmed watermark.
  If ahead, its contents remain speculative. Structural mismatch closes the session and restarts; returned results may
  be invalidated. No live corrected suffix with accepted drift remains in this design.
- **Schema-detached local read** continues against the old application's local view and reports `schemaDetachedVolatile`.
  It provides no canonical confirmation or durability for the detached suffix.
- **Retired local read** is labeled `retiredVolatile` with application generation, database instance and local position.
  It has no canonical or durability claim. A database awaiting initialization offers no reads until canonically opened.
- **Bounded-lag read** is admitted only while the replica is within a configured canonical-sequence/event/time distance
  from leader progress; otherwise readiness fails or the request waits.
- **Minimum-position read** carries a required event position, often returned after accepting a command, and waits or
  redirects until a read snapshot containing it is structurally confirmed, or redirect to the leader. This can provide read-your-writes after the event is replayed even
  if an unpublished old-term tail was rolled back.
- **Barrier/latest read** requires an event-log barrier or another authoritative high-watermark protocol. Merely
  checking an eventually changing end offset does not by itself make arbitrary reads linearizable with concurrent
  command producers.

Read admission evaluates the position and execution generation of the actual pinned read snapshot, not the latest
logged commit or confirmed watermark alone. Virtual batching can leave the readable root behind both. A snapshot at
event 100 cannot satisfy a minimum-position read for 105 merely because transactions through 105 are logged and
confirmed; wait for normal memory-batch publication and open a qualifying snapshot. A snapshot from abandoned
speculation cannot regain confirmation by numeric cursor equality after takeover; its history must match the accepted
ancestry. Routing to the leader still requires these snapshot checks.

Decision recorded 2026-09-14: ordinary application transaction success requires only the local commit.
Non-application operations use leader-only admission and immediately request asynchronous publication. Replication does not
wait for input-broker acknowledgement, another node's progress, direct confirmation, or Blob publication before
returning that local result. The application knows which transactions it has performed; it is responsible for making
other nodes execute the same ordered transactions through its own mechanism.

Default reads use the current local readable snapshot, including speculation, without consulting other nodes. Stronger
read modes discussed here are optional extensions and do not block implementation of the agreed local-read contract. Ordinary BTDB visibility still applies: an existing reader
keeps its snapshot, and virtual batching may defer publishing committed transactions to new readers. An application
requiring immediate fresh read visibility finishes its memory batch and opens a new reader; no remote wait is added.

External effects are entirely application-owned. Replication neither emits them nor implements an outbox, deduplication,
or a business command-success contract. Its local commit result is not a cross-node or Blob-durability acknowledgement.

Once a leader has crossed its shutdown canonical cut, any transaction result it returns is explicitly local and
volatile. It may report the event identity and disposable head for diagnostics, but it cannot claim leader-canonicalized,
object-store-published, or externally acknowledged durability. The application owns resubmission/replay and any stronger acknowledgement or read policy it needs on the successor.

The same restriction applies to retired databases and schema-detached `.temptrl` execution; unpublished additions do not execute locally. Retired/detached results are
useful process-local outcomes only and do not establish published canonical history.

A recent direct-stream tail may need to be regenerated on leader loss. The event log retains the input, so this is
replay work rather than a requirement to wait for Blob before completing a transaction. BTDB.Replication does not add
a client durability-acknowledgement protocol; local reader and virtual-batch semantics remain unchanged.

## Expected failure behavior

This table applies the shared state machines; it does not introduce alternate recovery procedures.

| Failure class | Canonical progress | Local execution / recovery |
| --- | --- | --- |
| Competing candidates, stale sessions, or lost leader-record response | Only proven lease ownership plus selected identity permits activation | Reconcile selection; other nodes keep following/speculating. |
| Leader pause, unsafe authority observation, or Azure outage | Stop by the proven self-fence deadline; no peer timeout grants takeover | Accepted reads remain policy-dependent; speculation may continue. B1 must establish the deadline. |
| Leader endpoint fails while its lease renews | Followers cannot revoke the lease | Read selected remote progress where available. B6 defines unhealthy-leader abdication. |
| State write races adoption | Only the CAS-selected predecessor is adopted | Publisher rereads a winning predecessor update; old ETags fail after adoption. |
| Selection succeeds but some databases never activate | No canonical writing for any active database | Resume selected obligations idempotently or relinquish authority under B6. |
| Upgrade target fails before / after initialization publication | Unpublished additions can initialize from scratch; published databases retain their history | Test crash after name selection, partial uploads, ambiguous initial publication, and activation of only some databases. Removed databases do not delay activation. |
| All nodes meeting the generation floor are lost | Canonical progress stops | Older nodes never lead; restore an eligible generation. |
| Partial staged upload or lost TRL CAS response | Apply complete transactions only | Reconcile actual TRL version; successful complete canonical append is durable without a second write. |
| Checkpoint is ahead, stale, or at the same position | Ahead waits; stale is rejected; equal-position verified physical replacement is allowed | Application processing continues; only `PublishKvi` selects the result. |
| Application transaction failure | Application chooses rollback/retry, explicit skip or fail-fast; no automatic skip | Preserve prior virtual-batch commits; compare rollback attempts/outcomes across nodes; structural disagreement restarts follower. |
| Missing/duplicate/conflicting canonical data | Gap cannot advance; exact duplicate is idempotent; conflict is a safety fault | Resume from confirmed metadata or restart for canonical rebuild using the common acceptance path. |
| Event-structure match / mismatch | Validate ordinary transaction framing and payloads; actual structural or outcome mismatch restarts follower | Matches advance metadata only; no live suffix repair. |
| Long speculative lag or structural mismatch | No speculative state becomes canonical by age or volume | Disk-backed comparison; no historical replication roots. Mismatch restarts and rebuilds. |
| Independent local/remote compaction | Application frame processing continues without compaction messages | Keep each node's valid local layout and pins. Future open restores the selected remote KVI. |
| Shutdown at any transaction/compaction boundary | No new canonical work after the per-database cut | Continue `.temptrl`; cancel compaction at a bounded safe point; use authority lane only. |
| Failed target or ambiguous lease transfer | Draining never resumes canonical work | Reconcile proposed lease ownership, try another target only when authority permits, or await expiry. |
| Scratch cleanup failure / scratch I/O failure | No promotion or remote fallback | Cleanup failure stays unavailable; disk exhaustion is fatal and uses restart with old cache removed first. |
| Duplicate/delayed leak-removal request or event | Only the normal ordered event can mutate state | Parent reconciles publication identity; handler ignores absent non-reused exact keys. |
| Event source gap, unavailable source, or expired retention | Never skip required events | Accepted reads and available canonical replay may continue; expose degraded readiness or unrecoverable gap. |
| Missing/corrupt local files or disk full | No inferred local truth | Fail node; terminate old readers, remove obsolete/invalid cache, reuse verified files and restore latest Blob closure and replay input. If space remains insufficient, stay unavailable. |
| Remote object disappears during restore | No incomplete database is served | Restart; reread latest state and use its published KVI. If the current closure itself is broken, report unavailability. |
| Restore/publication races GC | Publish replacement closure before deleting superseded files; no restore pin | Follower restarts on missing old files. B5 verifies staging protection and key non-reuse, not delayed deletion. |

### Availability and progress budgets

Read availability, speculative throughput, canonical progress, and durable progress are separate signals. A lease renewal
alone is not application health, and an event stream with no new events is not a stalled leader. Define activation and
required-work deadlines before relying on automatic recovery (B6).

A conservative failover budget includes detection, remaining lease lifetime, backoff, authority/adoption, and required
restore/replay. Measure warm and cold recovery separately; overlapping work may reduce the observed time. One failed
active database gates cluster leadership. Per-database reads can remain available under their own read policy.
Decision recorded 2026-09-14: use separate progress watchdogs for activation, application-reported transaction work,
and Blob publication. Expire a configured no-progress deadline only when work is pending; an idle input is healthy,
and a restore that is advancing is not a stalled activation. An unhealthy leader fences canonical work and relinquishes
authority through bounded host recovery. Startup restore happens as a follower, before lease contention. Failed
activation uses increasing retry backoff to avoid repeatedly monopolizing authority. Application failure classification
and optional skip decisions remain application-owned; a storage/activation stall is never itself a skip request.

Deployment policy must retain enough nodes eligible for the selected generation; the monotonic floor intentionally
prevents old binaries from rescuing a failed new generation. Storage-account regional recovery is a separate B6 scope,
not ordinary compute-node election.

Required observability covers authority/lease margin and phase duration; per-database speculative/confirmed/durable
positions and lag; publication ambiguity; checkpoint/recovery age; structural mismatch/restart count and comparison lag; memory/disk
retention; replay throughput; compaction state; and GC backlog. Metrics observe state-machine decisions and never grant
permissions. Numeric SLOs and thresholds are Q4.

### Consumption timeout and restart

Source study on 2026-09-07: inspirecloud's Skymamba `DeadlockedEventConsumeHealthIssueAnalyzer` detects an event handler
running for more than five minutes. It calls `IgnoreEvent.IgnoreEventAsync` before requesting restart through
`EventConsumeTooLongException`, whose preferred action is `FastNonGracefulRestart`. `IgnoreEvent` writes empty marker
blobs under `ignored-events-{SubCluster}`, named by event ID and, where applicable, company ID plus event type. Startup
loads them and `ContinentEventChannel.ApplyEvent` bypasses matching handlers. Persistence errors are logged and swallowed,
so the existing restart is still attempted if writing a marker fails. This class contains no time-based expiry; deployed
storage lifecycle configuration was not established by this source study. Disaster recovery explicitly clears markers.

Application-selected recovery integration: the application may request an exact-input skip marker and process restart,
using the leader JSON instead of separate marker blobs. The following is an optional mechanism, not automatic exception
classification by BTDB. The application decides whether a handler timeout, out-of-memory or another failure calls for
rollback, fail-fast, and possibly a skip marker. There is no mandatory five-minute handler timeout in replication.
When this integration is enabled, a watchdog observes application-reported per-database execution tokens independently
of the handler: attempt identity,
current event identity, start time, and batch range. It measures the reported attempt with an injected monotonic clock; idle
input, storage waits, and transaction commit stalls must not be mistaken for one poisonous application event.

When the application requests fatal leader recovery with an exact-input skip marker:

1. Atomically fence that execution attempt and the session against later canonical commits/publication. A handler that
   returns after timeout cannot commit. Completion racing the timeout must have one winner; do not record a skip for an
   attempt whose successful transaction already won. Keep only the bounded authority/control work needed below.
2. Persist the exact current event identity in `eventsToSkip` using the owned lease and leader-record ETag. Reconcile an
   ambiguous response by rereading the operation identity/list; do not treat a cancelled request as rejected. If authority
   is lost, do not write as the old owner. No other events in its batch are marked as skipped.
3. Request a bounded non-graceful process restart through the injected host port. Do not wait for the stuck handler to
   cooperate or try to reuse its private BTree/TRL state. Stop renewal and fail over even if marker persistence fails;
   report that failure, because another timeout/restart may be required. The host must prevent this session resuming.
4. A successor reads the skip list before consuming recovery events. It restores the selected durable boundary and
   processes outstanding events as ordinary individual log transactions; virtual memory batching may remain enabled.
   For the named event it does not call the handler: it publishes the ordinary singleton metadata-only cursor commit
   with `outcome=skipped`. The timed-out transaction has no partial canonical effect; earlier committed virtual-batch transactions remain committed. Followers replay that TRL.

The timeout marker affects future application execution only; it never rewrites a transaction already in published
canonical history. Canonical bytes remain authoritative even if a marker names an event already covered by them.
A follower can avoid a listed handler speculatively and can report its own timeout to the leader, but cannot edit the
leased JSON or decide canonical history. Its watchdog may restart that follower independently. A leader receiving a
follower report must reconcile it with its own canonical progress; a follower timeout alone is not permission to undo
an accepted leader outcome. Removed databases remain outside this mechanism's cluster coordination.

Proposed automatic cleanup: retain a marker for at least one day from `recordedAtUtc`, and remove it only after the
published database cursor has passed or includes that event (under the stream's ordering convention). The published
history then contains the outcome and ordinary recovery does not execute the event again. Do not ignore an entry merely
because its timestamp is old while the database is still behind it. Removed-database entries may be removed after the
same age because names are never reused. Current-leader cleanup can be piggybacked on record writes, with a low-frequency
sweep if needed; no per-renewal JSON write is required.

Backup restore is an explicit new cluster incarnation: the operator deletes `leader.json` before starting the restored
cluster and creates a new event stream. The old `eventsToSkip` list is intentionally discarded; event IDs in the new
stream must not inherit old skip decisions. This is an offline restore procedure after stopping the previous cluster,
not ordinary failover or a live leader-record deletion. The restored database contents provide the starting data; old
stream cursors and authority are not interpreted as progress in the new stream. Initialization must bind the restored
base to the new stream's starting cursor. Detailed backup import belongs to the recovery integration contract.
Normal restart and election retain the existing stream and preserve `leader.json` skip entries.

This defines the optional application-requested marker path, not a library-selected response to every failed transaction. Exact integration
of watchdog/commit arbitration with authority fencing belongs to B1/B3. B6 still covers activation failure, storage or
publication stalls without a handler identity, host restart guarantees, and failover/region budgets. Test timeout versus
successful commit races, timeout in a batch, marker write failure/ambiguity, takeover preservation, follower reports,
restart before skip publication, outages exceeding one day, and cleanup racing leader-record replacement.

## Testing strategy implied by the design

Implement one deterministic harness with separate node scopes, file collections, roots, event consumers, and identities.
Shared in-memory storage/event ports, virtual time, seeded randomness, and an explicit scheduler run multiple complete
nodes without sleeps, sockets, or process globals. The in-process transport uses production DTOs, codec, authorization,
resume, cancellation, and bounded queues, not a privileged bypass.

For each applicable scenario below, inject crashes before dispatch, after remote effect, and before response delivery;
delay operations independently; duplicate/reorder allowed messages; and distinguish clean rejection from ambiguous
completion. The oracle checks I1-I12 after every scheduled step, not only eventual convergence. Scenarios that depend
on B1, B3, B5, and B6 remain acceptance requirements until those mechanisms are specified and implemented.

| Scenario family | Required distinguishing cases | Invariants |
| --- | --- | --- |
| Authority | Simultaneous candidates; delayed old frames; pauses around expiry; expired cached observations; lost acquire/renew/CAS responses; immediate transfer with outstanding old observations | I1, I6 |
| Publisher | Tail/checkpoint races; stale captured intent; predecessor CAS wins before adoption; request still pending after a read of old state; operation identity reconciliation | I5, I6 |
| Durable boundaries | Transaction crosses multiple TRLs; commit/rollback in final range; missing/reordered successor; crash before/after canonical linkage or final commit publication; no partial-root visibility; virtual batches preserve prior commits; rotation/adoption ambiguity | I2, I5, I8 |
| Legacy TRL parity | Existing even/odd TRL history opens unchanged; even append target rotates before transaction start regardless of size; odd target stays; occupied odd IDs are skipped; crash before/after close/allocation; no cursor/sequence change or follower detachment; pinned old values remain readable | I2, I5, I8, I9 |
| Checkpoints | No KVI upload/staging request before required PVL and TRL publication through its fixed cursor; ambiguous prerequisite response blocks upload; later tail does not delay KVI; ahead-of-durable capture; stale capture; equal-position compaction checkpoint; conflicting logical identity; retained predecessor checkpoint after adoption; source pinned throughout upload | I5, I8, I9 |
| Transition recovery | Bootstrap and handoff use identical activation checks; every partial create/adopt combination; matching preexisting genesis versus unrelated state; one instance cannot activate | I1, I6, I7 |
| Upgrade | New DB creation waits for leadership with no provisional writes; highest prepared generation priority; restore/election do not wait for migration completion; crash before/after genesis publication; published instances restored instead of initialized again; removed names never reused; floor blocks old nodes | I7, I11 |
| Retirement | Delayed predecessor CAS may land in abandoned namespace; no freeze or retirement CAS; old nodes continue independently from local views; removed names never reused; active-database readiness unaffected | I7, I11 |
| Graceful drain | Shutdown inside application transaction, compaction chunk, upload, and ambiguous TRL CAS; per-database cuts differ; no target; target failure before/after transfer; ambiguous Change Lease | I1, I5, I11 |
| Scratch | Values above inline limit; later reads of old/new values; disjoint IDs; disk full/lost; interrupted deletion; startup cleanup failure; malformed names and links cannot escape scratch root | I8, I9, I11 |
| Application batches | Virtual batching enabled/disabled yields identical transaction payloads; rollback preserves committed prefix; published read root may lag log; header/configuration identity separately validated | I2, I3, I4 |
| Structural comparison/restart | Leader events 1-3 versus locally committed 1-5; virtual batching preserves transaction payloads; file identity checked separately; real mutations and skip outcomes mismatch; no pinned historical roots; restart rejects divergent files and reuses independently verified canonical files | I3, I4, I9 |
| Optimistic tail adoption | Canonical 102 versus local 101-105 copies complete 103-105 transactions unchanged despite memory batch boundaries; overlap mismatch rejects; retry/crash never duplicates events | I2, I3, I5, I8 |
| Invalid comparison cache | Missing/corrupt bytes reject confirmation and trigger restart/rebuild when unrecoverable | I3, I8, I9 |
| Failed consumption | Identical rollback attempts/content/outcomes versus mismatch; no implicit skip or consumed-cursor advancement; application-directed fail-fast with/without marker; prior batch commits survive; verify ObjectDB caches/counters | I2, I3, I5, I8 |
| Skip replay | Leader skip versus follower success and reverse; same-outcome exact match; crash before/after skip commit and publication; durable skip never invokes handler; unpublished skip may be re-decided after takeover | I2, I3, I5, I8 |
| Follower acceptance | Exact match does no BTree work/copy; live input lag coalesces progress until local coverage; bootstrap replays selected canonical history; duplicate/gap/conflict; logical mismatch; equivalent transactions on different valid physical layouts | I2, I3 |
| Restart recovery | Structural mismatch first/middle/last; fence racing a commit; host termination; canonical rebuild ignores divergent cache; fresh ObjectDB counters/caches; restart-loop diagnostics | I3, I4, I9 |
| Speculation resources | Extended disconnection and slow delivery; bounded comparison RAM and pending TRL disk growth; bounded streaming comparison; explicit storage failure without remote admission dependencies | I4, I9 |
| Leak events | Every replica detects only accepted roots; malformed/hash-invalid/duplicate/over-budget candidate; bounded batching; delayed, overlapping and ambiguous parent publication; normal event replay including a transaction spanning several TRLs | I2, I3, I10 |
| Physical compaction | Independent leader/follower rewrites with interleaved events; no KVI/peer output from local mode; remote PVL reuse/repacking and file-ID/offset remapping; different local/remote inventories; interrupted export retains remote closure | I3, I9, I10 |
| KVI and local cleanup | Exactly one bootstrap KVI per open; no later live KVI; older/newer local startup layouts; full local compaction on followers; zero local KVI creation; independent remote inventory and remapped export | I8, I9, I10 |
| Reader/file lifetime | Long-lived accepted and optimistic readers; ordinary user-reader/current-tree/comparison-file/recovery lifetimes; independent local/remote allocation after compaction; no distributed deletion decisions | I9, I10 |
| Startup pipeline | Equal-length corrupt file; valid reusable file; absent/stale checksum; active tail always redownloaded without whole-file hashing; version change during download; sealed tail becomes reusable; reversed download completion; cross-file transaction; no KVI; retained obsolete remote files; bounded lookahead and pinned TRL values | I5, I8, I9 |
| Cache loss | Delete/truncate/replace/mix local file types before open and while running; all caches lost; disk exhaustion during append/compaction/restore; terminate readers before deletion; reuse only matching identity/length/hash; remove obsolete files before replacement download; interrupted restore revalidates; insufficient space remains unavailable | I1, I8, I9 |
| Remote GC | Delayed old-term delete; attempted reuse of retired key; unselected initialization upload later published; current published recovery closure; configured deletion delay; deletion during a long follower download still causes restart onto newer KVI | I5, I7, I9 |
| Transport | Independent leader links fail while others work; reconnect storms; old status/connection IDs; rotated credentials; unauthorized request; secret logging; bounded queues and unavailable retained ranges | I1, I3, I12 |
| Input/read contract | One shared stream across databases; slowest required cursor protects retained input; duplicate/gap/expired input; source unavailable with canonical bytes available; read token across term rollback; logged/confirmed 105 with readable batch root 100 cannot satisfy minimum 105; speculative/volatile reads never labeled durable; pending initialization has no reads | I2, I3, I8, I11 |
| Non-application publication | Follower waits before writer lock/TRL output; genesis predecessor CommitUlong; immediate complete-transaction publication including every required TRL range after any required prior tail; dependent local writes may proceed before CAS; immediate flush bypasses lazy batching without blocking Commit; delayed/ambiguous CAS and lost authority; cancellation and duplicate migration after wakeup | I1, I2, I5, I6 |
| Detached liveness | No candidacy/lease acquisition/handoff after detachment even if DB set changes; leader absent for 15 monotonic minutes triggers graceful restart; valid leader resets timer, stale messages do not; long transaction delays graceful exit; scratch never promoted; incompatible restart remains ineligible | I1, I7, I11 |
| Schema upgrade | Index add/remove in ordinary TRL; schema sequence advances with unchanged cursor; live follower detaches even when speculative ahead/behind; no later canonical frames or tail promotion; compatible startup replay/checkpoint works; old binary cannot reverse schema; crash before/after publication | I2, I7, I8, I11 |
| Same-term control revision | Skip insertion/cleanup changes leader revision without readopting databases or invalidating frame/resume ancestry; delayed observations cannot undo newer skip decisions or renew authority | I1, I6 |
| Liveness | No lease attempt until all required Blob databases restore; unpublished genesis exception; activation stall; publication stall with healthy renewal; progressing restore and idle input remain healthy; activation backoff; warm/cold takeover and generation-floor loss | I1, I4, I7 |

Canonical logical equality must be checked against an independent logical-state oracle, not only a matching frame-chain
hash. Compare restored values as well as positions. Assert physical file resolution for every retained reader. Intercept
ports to fail tests on follower remote deletion, speculative publication, post-cut data persistence, or any live KVI/delete
message prohibited by I10/I11.

Every port has reusable semantic conformance tests. Run transport cases against both the in-process adapter and loopback
Kestrel; add HTTP fragmentation, proxy buffering, flush/idle timeouts, range reads, authentication, and slow consumers.
Run storage cases against real Azure in addition to Azurite: conditional append, unchanged selected prefixes, cross-file transactions,
rotation before/inside/after transactions, CAS ambiguity, and acquire/renew/change/release. The four-step CAS probe is a prerequisite, not proof of
multi-node correctness. S3 qualification belongs to its later adapter.

Measure standalone BTDB before and after core changes: reads/writes, inline and large values, range erases, commits,
allocation, throughput, and startup replay. No measurable regression beyond benchmark noise is acceptable. Separately
measure replication overhead, comparison memory/disk growth and restart recovery work, cold restore, and event catch-up.
Follower transport delays must not introduce commit waits for external progress. Performance results cannot excuse a
safety failure or leave byte-identity integration and fatal disk-exhaustion recovery unverified.

## Open decisions

### Implementation and proof blockers

[M1 executable evidence](M1Evidence.md) covers bounded authority and direct TRL CAS, including live Azure checks
linked from the storage research. It does not close production integration. KVI selection is only part of Blob-to-local
open/restore: publish every dependency before KVI, and restart restore if concurrent cleanup removes a needed file.
The incomplete-local-copy test does not imply any additional KVI ancestry metadata or selection protocol.

There is currently no pending application-owner policy choice. The following are engineering mechanisms and proof
obligations under the selected design; they must be resolved before the corresponding implementation is relied on.
If a proof requires changing agreed behavior, bring back that concrete conflict and alternatives rather than silently
changing policy. The documented state transitions specify intended behavior, not a completed distributed-safety proof.

| ID | Engineering work before the mechanism is relied on | Required evidence |
| --- | --- | --- |
| B1 Authority freshness | Short-lived confirmation grants are selected. Specify their exact clock/pause bounds, renewal margins, challenge invalidation and drain bounds, plus in-flight commit fencing. Confirmation is optional consistency evidence; local takeover rejects predecessor live messages. A GET does not establish a fresh lease lifetime. | Old-term acceptance excluded under pauses, delayed grant responses/frames and takeover races; disconnected-grant drain; takeover without direct confirmation; real-provider conformance. |
| B3 Replication integration and transaction rollback | Virtual batching preserves ordinary per-transaction TRL and needs no different-batch normalization/reframing. Implement cross-file transaction capture/publication and opt-in odd TRL allocation, including immediate closure of legacy even append targets; specify ordinary rollback TRL retention/comparison without cursor advancement, application transaction integration, file/header identity, ObjectDB state and idempotent tail adoption. | Core byte-equality test covers payloads, not complete distributed operation. Real divergence restarts; rollback preserves prior commits; fenced append/retry does not duplicate events. |
| B5 Publication/deletion ordering | Qualify direct TRL CAS, rotation/genesis discovery and predecessor fencing without a second state commit; implement publish-before-delete, staging protection, and permanent key non-reuse. Superseded files may be deleted after KVI publication; remote-space savings need not be aggressive and follower restores do not pin them. | Selected KVI and tail remain complete; delayed old deletes cannot remove newly selected data; a follower losing old restore files restarts onto the latest KVI. |
| B6 Progress and recovery | Separate pending-work watchdogs and follower-first full restore before contention are selected. Define no-progress deadlines, bounded host termination, abdication and activation backoff. The application owns failure classification and optional skip requests; document the application input-retention/replay assumptions without adding a separate durability protocol. | Warm/cold failover budgets; idle streams stay healthy; failed workers cannot hold authority indefinitely; missing unpublished BTDB work is regenerated from retained input. |

The selected checkpoint rules (durable ancestry and equal-position physical replacement) and idempotent activation cases
are now normative. They require tests but are no longer alternative algorithms to choose between.

### Implementation and configuration backlog

| ID | Work remaining within the selected design |
| --- | --- |
| Q1 Input integration | Application-owned identical transactions and rollbacks are selected; Kafka is illustrative only. Remaining work: injected transaction/progress/replay and current-input-end interfaces, application-requested skip integration, eventId admission with automatic CommitUlong, stream/cursor encoding and ordinary rollback TRL capture; no separate attempt protocol. |
| Q2 Core API and codecs | Opt-in capture/pin/replay/export APIs; canonical position persistence separate from reused BTDB TransactionId; exact transaction and logical-change encoding; hashes and redacted diagnostics; independent protocol/leader-record/TRL-metadata/native-format/protocol compatibility; no separate checkpoint-object format. |
| Q3 Application/read semantics | Local commit success and local snapshot reads are selected; failure policy and external effects belong entirely to the application. Remaining work: native schema lifecycle and compatible startup replay, ordinary local snapshot visibility and application-owned input recovery. Stronger read/durability APIs are not prerequisites for this design. |
| Q4 Operational budgets | Independent per-replica virtual memory-batch count/bytes/time and independent remote publication-batch bytes/time; checkpoint cadence; local hard-flush policies; root/history and event-retention budgets; warm/cold RTO; handoff lag/grace and long-transaction limits; watchdog thresholds and metric alert levels. |
| Q5 Transport | Three-field progress notification and bounded TRL range pull; native decoding, coalescing and unchanged-eventId schema notification; authority-bound requests, reconnect and range retention; byte piggyback optimization deferred; authorization beyond the shared key. |
| Q6 Maintenance | Bounded local compaction chunks and separate local/canonical allocation; separate local/remote compaction inventories; no local KVI; remote PVL destination allocation and KVI reference/cursor remapping; leak detector compatibility, trust/reachability validation, exact-key budgets, batching, and deduplication. |
| Q7 Recovery and storage | TRL continuation, initial/checkpoint discovery and optional index retention without losing ancestry; latest-checkpoint restore and complete event coverage; offline backup restore with new stream/cursor binding and leader-record reset; runtime local-integrity cadence; restart on removed restore files without remote pins; reuse of transfer code without legacy unconditional writes. |
| Q8 Deployment and lifecycle | Database-name/instance syntax; generation allocation/conflict checks; eligible rollout redundancy; current-input-end capture and retention during genesis publication; abandoned namespace retention/administration; exact scratch namespace/cleanup and fatal disk-exhaustion restart and explicit local commit result. |

Provider mechanics and future S3 alternatives stay in ObjectStorages.md. Version-one append selection, validated optimistic tail reuse with input-recovery fallback, ordinary per-event commits independent of virtual memory batching, bootstrap-only KVI transfer, and node-local deletion decisions
are settled constraints, not recurring open questions. Application-owned execution/failure policy/external effects, local
application commit success and local reads, follower-first restore, genesis predecessor `CommitUlong`, and leader-only
non-application writes that immediately trigger asynchronous publication without awaiting Blob are also settled.

### Review decisions (2026-09-14)

R1–R5 are selected below and incorporated into their normative owner sections. B1 timing proofs and operational budgets remain engineering validation work; the selected
short-lived grant policy is not reopened. The existing DB is already in Blob Storage; no import workflow is needed.

#### R1 Publishing a transaction that spans TRL files — selected A

Prepare successors first, then expose the complete transaction by CAS on the canonical predecessor TRL.
The [TRL publisher](#trl-publisher) owns ordering, ambiguity and recovery prerequisites. Do not add a state CAS.

#### R2 Native KVI publication — selected, no checkpoint pointer

The native KVI is sufficient. Upload its prerequisite files first and write the KVI last. Only a successful KVI write
allows deletion of old unused files. No checkpoint pointer, manifest or separate selection CAS is added. Restore
discovers published native KVIs; interruption by deletion restarts discovery. See [native KVI publication](#native-kvi-publication).

#### R3 Durable recognition of non-application transactions — selected native semantics

A committed transaction that leaves `CommitUlong` unchanged is non-application. Application commits change it.
Genesis is the sole classification exception: it creates the database and sets the predecessor cursor. Rollback is
identified by its terminator, not by comparing committed cursor values. No reserved Ulong slots or kind sidecars.
See [non-application schema transactions](#non-application-schema-transactions-during-upgrades).

#### R4 Comparing rollback attempts — selected B

Keep ordinary TRL writes from rolled-back transactions and compare them in order with the next committed history,
just like successful transaction operations. All nodes must perform identical rollback operations/outcomes; do not
collapse them to final-state equality. No separate attempt-record protocol or synthetic empty-rollback marker.
Idle comparison may wait for the next commit. Capture/retention during virtual-batch rollback still needs tests.

#### R5 Writer admission — selected existing asynchronous API

Use `StartWritingTransaction()`; prohibit synchronous `StartTransaction()` in replication mode. Keep explicit read-only
transactions. ObjectDB checks the complete relation list read-only and persists new schemas and index upgrades
in at most one startup writing transaction. Startup orchestration supplies leader authority before initialization. Ordinary
application followers retain their writer path. Immediate asynchronous non-application flush after commit remains required; local completion does not await Blob.

### Application API decisions after storage decisions

#### N1 Writer admission — selected automatic event cursor

Application writer admission supplies eventId to StartWritingTransaction, which immediately sets CommitUlong on the
writable transaction. No separate validation against a manually set cursor is needed. Without eventId, admission is
non-application. Secondary-index reconciliation runs at most once as the first ObjectDB writer after open; startup
orchestration waits for leadership before opening it. No generic core writer gate is needed. Genesis sets its predecessor cursor explicitly.
Rollback discards the working cursor with the transaction. No new persisted kind marker is introduced.

#### N2 Publication scheduling — selected asynchronous immediate flush

Commit remains local. Non-application commit signals immediate asynchronous TRL flush, bypassing lazy timer/size
batching. It does not wait for Blob or block subsequent application work; no CommitAsync is required for this purpose.
Followers may request the recent tail from the leader with a short timeout, otherwise regenerate unavailable
unpublished work from application input. The [write admission rules](#leader-only-non-application-write-barrier) own
publication ordering and ambiguity. Published durability is still established only by the TRL CAS.

#### N3 ObjectDB initialization writes — selected eager startup registration

After ObjectDB opens, `InitializeRelations` checks all registered relation schemas and indexes in a read-only
transaction. If changes are needed, persist them together in one startup writing transaction, including schemas
of new empty relations and creation callbacks. Do not create relation schemas during application upserts or ID allocation.

Startup orchestration waits for leader authority before initialization that may write. These are non-application
transactions with unchanged CommitUlong, immediate asynchronous flush, and the existing live-follower detachment
behavior. Follower startup uses compatible published schemas without running migrations.

## Design history and alternatives

Decisions incorporated into the normative sections:

| Date | Adopted direction |
| --- | --- |
| 2026-09-14 | Plan configurable delayed Blob deletion, provisionally about one day after files become obsolete. Keep dependency-first KVI publication and restore retry; no follower tracking is required. |
| 2026-09-17 | Replace ordinary metadata deferral with complete startup relation registration: read-only inspection, then at most one writer for new empty schemas, schema/index upgrades and creation callbacks. |
| 2026-09-14 | Application writer eventId automatically sets CommitUlong; non-application commit triggers immediate asynchronous flush without blocking local work. Ordinary metadata deferral was later superseded by eager startup registration. |
| 2026-09-14 | Select prepared-successor publication; unchanged CommitUlong identifies non-application commits except genesis; compare retained rollback TRL with later commits; require async writer admission and readonly-first ObjectDB initialization. Native KVI written last is the publication point; no checkpoint pointer or manifest. |
| 2026-08-30 | Azure-first cluster leadership, transaction-aligned publication, injected adapters, and leader-centered sessions |
| 2026-08-31 | Initial generation-fenced transition proposal; selected seeds and frozen retirement requirements superseded below |
| 2026-09-07 | Inline `databaseNames` in the leader JSON; initialization metadata stays per database |
| 2026-09-07 | Activate with an empty transaction and starting event cursor; unpublished databases may restart from scratch without preserving provisional seeds |
| 2026-09-07 | Removed names are never reused; old nodes continue independently until shutdown. Exact retirement freeze and blocker B2 removed |
| 2026-09-01 | Originally event-only TRL; schema transactions added on 2026-09-14. Parent-published leak removal remains selected. Leader-distributed compaction was replaced by independent local and leader-only remote compaction on 2026-09-14. |
| 2026-09-06 | Leader-only batching and metadata-only skips; the initial retained-root/suffix-repair proposal is superseded below. |
| 2026-09-07 | Batching-independent structural TRL comparison; follower mismatch restarts/rebuilds. No replication-owned old BTree roots or live suffix repair; B4 removed and B3 narrowed. |
| 2026-09-07 | Superseded on 2026-09-12: emit event-end Ulong updates inside TRL; all replicas batch independently. Takeover appends only validated optimistic events beyond the adopted canonical end, without duplicate consumption. |
| 2026-09-14 | New replication TRLs use odd IDs; preserve legacy files and close an even append target before starting the next transaction in a fresh odd TRL |
| 2026-09-14 | Withdrawn later the same day: transaction-boundary-only rotation. Transactions may span TRL files; complete closure publication uses prepare-successors then predecessor TRL CAS (R1-A). Existing input DB is already in Blob Storage; no import workflow. |
| 2026-09-14 | Canonical TRL CAS directly establishes durability; remove per-batch state.json publication. Rotation, discovery and adoption must preserve the direct-TRL contract. |
| 2026-09-14 | Genesis and schema writes wait for leadership and immediately publish to Blob state; initialization CommitUlong precedes the first applied event; provisional writable additions removed |
| 2026-09-14 | Non-application schema transactions remain canonical TRL; live followers detach into volatile local execution; compatible replacements replay schema from TRL or checkpoint |
| 2026-09-14 | Application-owned identical transactions including rollback; local commit/read semantics; application failure and external-effect policy; follower-first complete Blob restore; new databases start at current input end |
| 2026-09-14 | Reuse remote-matching local files by length and whole-file checksum; parallel bounded startup transfer with ascending TRL replay; no KVI requires all canonical TRLs. Local disk is prioritized over remote space. |
| 2026-09-12 | Historical cache policy superseded above. Implemented virtual batching preserves ordinary per-event TRL payloads; no batching normalization/reframing. Disk full is fatal: terminate readers, delete cache, restore latest Blob closure, replay Kafka input. |
| 2026-09-07 | Superseded by the 2026-09-14 operational-delay preference: delete superseded remote files immediately after publishing the replacement KVI; interrupted follower restores restart onto the newest checkpoint. No restore leases or grace period. |
| 2026-09-06 | Shared identities, one database publisher, one transition engine, and one follower acceptance path; checkpoint selection constrained to durable ancestry and allowed to replace physical layout at equal position |

The following alternatives explain the storage decision. Version one remains conditional atomic TRL tail append with
sealed/pinned immutable checkpoint closures. Azure mechanics and later S3 research remain in ObjectStorages.md.

## Checkpoint-file storage alternatives

Historical alternatives: manifest/chunk-based publication below is superseded by native KVI-last publication.
It does not impose a manifest or pointer on the selected design.

The event log and canonical TRL contain the same ordered application transitions, including a parent-published
leak-removal event. Compaction operations are deliberately absent from both. A valid KVI seals one physical
layout for recovery; an older KVI naturally reconstructs the older layout by replaying only application transactions,
while a later selected checkpoint can carry the leader's compacted KVI and files for a future follower open or rebuild.
It is never pushed into a database that is already running. Azure retains accepted remote TRL boundaries and checkpoints
while allowing a recent unpublished application tail to be discarded and recreated. The main storage question is how to
publish canonical progress and consistent BTDB checkpoints efficiently while the leader continues authoring
transactions.

### A. Sealed immutable BTDB files

At checkpoint capture, select one committed BTDB root, create its `.kvi`, and record exact file lengths. Finalized
files can be uploaded directly. The leader captures the complete ordered TRL file set through a cursor immediately after
a commit command; that transaction may start in an earlier file. A following temporary-end marker may be included as
file metadata, but it does not define transaction closure. In either case, every captured file cut is immutable from the
manifest's perspective even if the local active file later grows. The complete file set stays pinned until its manifest
is published or abandoned.

Advantages:

- portable across Azure Blob Storage and ordinary Amazon S3;
- stale and current leaders publish into separate term-qualified lineages;
- create-if-absent plus a content hash makes retries naturally idempotent;
- unchanged finalized `.trl`, `.kvi`, and `.pvl` content can be reused by later manifests;
- recovery opens ordinary BTDB files without reconstructing a provider-specific append format.

Costs and open issues:

- BTDB needs a checkpoint/export API that can expose the committed cut, exact file lengths, and lifetime pins;
- rotating the active `.trl` too frequently may create excess files and compaction work;
- local caches may contain different unreferenced files, but every referenced canonical file is identified by hash;
  deduplication remains opportunistic for checkpoint-only KVI files;
- manifests, not prefix listings or locally inferred file IDs, must define a complete restore set.

Native files remain selected, with KVI-last publication replacing the manifest mechanism described in this historical alternative.

### B. Immutable chunks of a captured active file

Instead of uploading a growing `.trl` from byte zero on every checkpoint, split each captured file cut into immutable,
checksummed chunks. A manifest identifies the exact ordered chunk sequence, final length, and whole-file hash needed to
reconstruct every `.trl` through the same closed transaction boundary.

Advantages:

- portable across both providers;
- only a new tail chunk normally needs uploading;
- no long-lived mutable data object or provider-specific mutable-file limit;
- missing, reordered, or corrupt chunks are detectable before BTDB opens the file.

Costs and open issues:

- more objects and storage requests than whole sealed files;
- chunk boundaries and the final partial chunk need a canonical, idempotent rule;
- recovery must reconstruct and verify the physical file before opening it;
- this duplicates some event-log data solely to improve checkpoint upload and restore time.

### C. Conditional atomic tail append — selected for version one

For each short-named database, the elected cluster leader creates a term-qualified remote TRL lineage containing one or
more physical files. Updating one active file uses this semantic operation:

```text
AppendIfCurrent(file, expectedToken, expectedLength, suffix, operationIdentity)
    -> Applied(newToken, newLength) | Rejected | Ambiguous(error)
```

`Applied` guarantees that the new visible file is byte-for-byte `oldContent || suffix`; no already-visible byte may
change. A new leadership term or physical BTDB TRL starts a new object rather than continuing the predecessor's file.
On the canonical chain, successful CAS exposing a complete transaction is its durable commit point. There is no
separate state-record publication afterward. Cross-file transaction publication, rotation and continuation links follow the normative
[TRL publisher](#trl-publisher); staged or unlinked objects alone do not establish canonical history.

The failover state machine depends only on those semantics. Provider object layout, staging calls, identifiers, and SDK
choices are intentionally outside this architecture and are recorded only in [ObjectStorages.md](ObjectStorages.md).

Advantages:

- the protocol expresses the required atomic append without coupling recovery to an Azure blob type;
- the previous token and length make a lost race detectable;
- already published prefixes stay immutable while successful later TRL appends immediately advance durability;
- range reads replay the verified canonical chain through complete transaction boundaries.

Costs and open issues:

- ambiguous success still requires token, length, byte, hash, and operation-identity reconciliation;
- a future provider that cannot implement the semantic operation efficiently may need immutable ranges instead;
- every takeover uses a separate term-qualified remote TRL lineage;
- the shared leader-blob lease grants cluster authority, but the conditional append itself serializes each database
  file; separate leases for every TRL file are not initially required.

### D. Whole-file conditional replacement

Read a `.trl` and its ETag, append locally, then upload the complete file with `If-Match`.

This exactly implements "replace only if I was the last reader," but its upload cost grows with the entire log. It is
unlikely to be viable beyond a prototype, a small metadata object, or a deliberately short rotated log. It also adds
no correctness benefit over an immutable checkpoint object followed by manifest publication.

### Historical candidate: identical event TRL and independent local compaction

The 2026-09-08 discussion explored independent event-log framing, local compaction and remote checkpoint producers.
Virtual batching was selected on 2026-09-12. On 2026-09-14 independent full local compaction without KVI and leader-only
remote compaction with remapped KVI were selected; see the [normative maintenance section](#independent-local-compaction-and-leader-only-remote-compaction).
The earlier local-KVI, follower-result distribution and dedicated remote-compaction job alternatives are not selected.

### Verified compactor flush simplification

Historical source and isolated experiment on 2026-09-08: the compactor's former call to
`NextCommitTemporaryCloseTransactionLog()` forced a writable transaction, emitted an empty commit plus
`TemporaryEndOfFile`, and hard-flushed the active file. This API does not directly rotate or permanently close the TRL;
subsequent transactions can append to the same file. File-size thresholds drive ordinary rotation.

The compactor's file eligibility is calculated from the already captured root/file generations, before that call.
Pointer rewriting and KVI creation do not require this extra transaction or temporary-end marker. They do require
appropriate durability: normal commits flush the writer buffer, but with `DurableTransactions=false` that is not an
OS/device hard flush. A durable KVI must not rely on value/log bytes lost from an unflushed file.

An isolated source copy was tested in three configurations:

| Variant | Verification | Result |
| --- | --- | --- |
| Existing compactor | `BTreeKeyValueDBTest.CompactorEvenWithLossOfNotFlushedDataWillNotAnyData` | Passed |
| Delete the complete temporary-close transaction block with no replacement | Same simulated unflushed-data-loss test | Failed reopening: `UnknownFile` could not be cast to `IFileTransactionLog` |
| Replace the block with byte-preserving hard flush under writer serialization | All `BTreeKeyValueDBTest` tests | 79 passed, including simulated data loss |

The experimental replacement acquires the normal writing-transaction reservation without making it writable, flushes
`MemWriter`, calls the active file's `HardFlush()`, reconstructs the writer from `GetAppenderWriter()` (required for
adapters that invalidate its buffer on hard flush), and releases the reservation without committing. It writes no
transaction commands and does not advance root identity, event cursor, or file allocation. This is a proposed internal
compactor operation; the production code was not modified by that experiment. The current source now uses
`FlushTransactionLog()` in `Compactor.RunCore()`, with regression coverage in
`TransactionBatchingTest.CompactionFlushDoesNotAppendLogAndSurvivesDataLoss`. These tests support the replacement,
not an exhaustive crash-consistency proof for every concurrent checkpoint/adapter scenario.

Retain the public temporary-close API and `TemporaryEndOfFile` decoder for compatibility. The loader uses this marker
to recognize a safely appendable tail when scanning a file, and `Dispose()` also emits it. Removing one compactor call
is different from removing the format command globally. A KVI whose saved cursor is exactly at a file's EOF is already
accepted for continuation by `LoadTransactionLog()` without requiring that marker.

## Provider-specific object-storage behavior

Azure Blob and Amazon S3 conditional writes, native Azure leases, throughput concerns, and the concrete Azure
realization of conditional tail append are maintained in [ObjectStorages.md](ObjectStorages.md). The failover and replay
protocols depend on the semantic append contract, not its physical block layout.
