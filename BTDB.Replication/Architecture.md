# BTDB.Replication Architecture

Status: Architecture proposal; no implementation has started. Normative requirements below describe the intended
version one, not verified runtime guarantees. Implementation blockers are listed under [Open decisions](#open-decisions).

Provider research snapshot: 2026-08-29. Architecture consolidated on 2026-09-06. The provider contract,
`denoland/celld` study, and Azure/S3 details are maintained in [ObjectStorages.md](ObjectStorages.md).

## Problem statement

`BTDB.Replication` runs one or more logical `BTreeKeyValueDB` databases on disposable compute nodes. One cluster-wide
leader authors their canonical histories. The durable upstream log orders application inputs; each database's
object-store-selected checkpoint and closed TRL boundary select the durable BTDB history. A new term may discard an
unpublished predecessor tail and recover it from validated optimistic TRL or regenerate missing events. Confirmed state is therefore distinct from durable state.

Followers synchronously execute the same events into local speculation without external I/O on the commit path.
Followers compare decoded TRL structure over matching event ranges, ignoring different batching and physical framing.
A match advances confirmation metadata without BTree work; a structural mismatch restarts the follower and rebuilds it
from canonical storage. Replication retains no historical BTree roots for confirmation or repair. Compaction is an optional physical optimization outside TRL.

A `Deployment` with ephemeral disks and a `StatefulSet` with stable disks use the same protocol. Node count, pod ordinals,
PVCs, and local file survival do not establish authority. One replica uses the same durable format as many replicas.

### How to use this specification

The following sections own the rules; other sections explain their application and must not define competing algorithms:

| Concern | Normative owner |
| --- | --- |
| Safety and progress identities | [Target safety properties](#target-safety-properties), [Shared identities](#shared-identities) |
| Bootstrap, takeover, and upgrade | [Cluster transition engine](#cluster-transition-engine) |
| Every database-state mutation | [Database state publisher](#database-state-publisher) |
| Live follower acceptance and repair | [Canonical frame and comparison protocol](#canonical-frame-and-comparison-protocol) |
| Opening or rebuilding a database | [Per-database replay hierarchy](#per-database-replay-hierarchy), [Restart or new replica](#restart-or-new-replica) |
| Physical maintenance and file lifetime | [Leader full compaction](#leader-full-compaction-over-the-follower-session) |
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
- Leader election is cluster-wide, not per database. The elected node is the only canonical TRL author and the only node
  that runs full pointer-rewriting compaction for every database active in its selected database set. Followers may run local
  cleanup-only compaction that cannot change a BTree root or remote state.
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
per-database state persists these identities. Equal generations must declare the same name set and identity mapping;
a conflict is a configuration safety fault. Database names are never reused after removal within the cluster.
Names alone must never authorize opening an old instance's history. A higher-generation candidate must support the
current protocol and the transition from the selected configuration.

### Per-database activation and restart

Selecting a generation and its names does not select an initial database snapshot. For a database without published
canonical state, the current leader starts from scratch and writes an ordinary empty BTDB transaction containing the
initial event cursor. This cursor defines where subsequent application-event consumption resumes, using the normal
stream cursor convention. The transaction changes no application keys and uses existing commit metadata encoding.
The application supplies the starting cursor; an empty database must not claim to have materialized earlier events.

`CreateGenesis` uploads the complete initial TRL/file closure and conditionally publishes its database-state record.
The closure includes an ordinary empty-database checkpoint for the existing restore path.
That publication establishes the initial canonical boundary; no provisional snapshot or selected seed manifest must
survive the leader-record CAS. The initialization transaction establishes canonical sequence zero; ordinary consumed
events start at sequence one. Its frame-chain identity binds the database instance, stream, initial cursor, and bytes.
This is an initialization boundary, not an applied/skipped application event.

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

A newer follower may open an added database in **provisional local mode**, consume its ordered application input one
event per transaction, and serve explicitly provisional reads. It has no canonical sequence, confirmed progress, or
published database state. Different followers may hold different disposable provisional copies.

After selecting the new generation, the leader initializes every unpublished addition with the empty transaction and
starting cursor described above. Provisional roots are not promoted as canonical initialization. The leader consumes
subsequent events normally; followers reopen against the published initialization and follow canonical TRL.
If initialization was never published, a later leader may start that database again from scratch.

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

Every node of the new application generation, including the chosen leader, discards or quarantines any provisional
copy before opening the new canonical database. It opens from the published initialization or a later canonical
checkpoint and tail. No provisional transaction is promoted or merged. Later follower transactions use the normal
canonical-frame comparison protocol.

An older follower that lacks a newly added database simply ignores that database's stream and remains non-ready for the
full new database set; it may still follow and serve compatible continued databases. For a database it still hosts but the
new database set retired, it uses the disposable behavior above. For continued databases, a former old leader discards any
temporary handoff suffix, closes and reopens against the new leader, and then behaves as an ordinary follower.

### Upgrade handoff readiness and selection

An old leader treats a connected higher-generation follower as an upgrade target when:

- its protocol range and every continued database compatibility fingerprint are accepted;
- every continued database is within the configured handoff lag and the target retains the required replay inputs;
- every database added by the target database set can initialize from its configured starting cursor and consume the required inputs;
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

- Arbitrary logical state mutations outside ordered application events. Out-of-band compaction may change only physical
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
  application generation. Instance identities and lifecycle metadata live in per-database state.
- **Provisional local database**: a database newly introduced by a follower's higher application generation while an
  older database set still leads. It runs against ordinary local files and ordered inputs but has no canonical sequence,
  frames, confirmed progress, or remote state. It is never promoted into canonical genesis.
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
- **Canonical transaction sequence**: a contiguous number assigned by the leader to every accepted event-consumption
  BTDB transaction. It advances once per committed event range; event position advances through that whole range.
  Retried failed attempts consume no sequence, while a singleton metadata-only skip consumes one.
- **Confirmed position**: metadata describing the latest contiguous leader event range whose decoded TRL structure
  matches local execution. It is not a pinned BTree root or a separately readable historical snapshot.
- **Speculative head**: the follower's single current writable BTree state, possibly ahead of confirmation. Replication
  does not retain its previous roots; ordinary open read transactions retain their normal BTDB snapshots.
- **Speculation generation**: the identity of one disposable local execution generation. Restart/rebuild abandons it;
  local entries never use reused BTDB `TransactionId` values as globally unique identities.
- **Speculative transaction entry**: a disk-indexed event boundary and local TRL range used for structural comparison.
  It contains no retained resulting-root handle.
- **Local speculative TRL cache**: follower-produced bytes and bounded comparison metadata. Matching normalized
  structure does not turn differently batched local bytes into canonical TRL. Canonical bytes are stored separately.
- **Shutdown canonical cut**: one database's last complete canonical transaction accepted before a graceful-shutdown
  leader switches that database away from persistent canonical writing. Its durable recovery base is the newest
  database-state-selected boundary that was already committed, or whose previously dispatched CAS is later reconciled as
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
- **Application transaction**: an ordinary BTDB transaction consuming a contiguous event range, or a singleton
  metadata-only skip after handler rollback. The leader defines its canonical boundaries; all replicas may batch independently and record every event end inside TRL.
- **Skipped event**: an individually failed application event whose failed attempt was rolled back and whose cursor is
  advanced by a fresh ordinary metadata-only transaction. It is an explicit consumed outcome, never an input gap.
- **Leak-removal publication request**: a non-authoritative, bounded exact-key candidate discovered against a pinned
  accepted root and submitted through an injected parent-system port. It never mutates BTDB directly. The parent system
  deduplicates and publishes an ordinary ordered application event containing the exact removal command.
- **Leak-removal event**: an ordinary application event whose normal handler removes the encoded exact keys
  idempotently in the same transaction that stores its event position. It has no special TRL encoding or replication
  path.
- **Compaction operation**: an authenticated, term-qualified out-of-band control sequence sent by the leader over the
  existing follower session. It names a confirmed logical base, sealed PVL artifacts, an ordered physical rewrite plan,
  and a resulting physical-layout identity. It is not a BTDB transaction, consumes no canonical sequence or event
  position, is never recovered from TRL, and never instructs a follower to delete a local file.
- **Bootstrap KVI**: the one canonical KVI obtained from a selected checkpoint when a follower opens or rebuilds a
  database. It establishes that local open's initial physical root. An already running follower never receives a later
  KVI as a live update; a subsequent restart or rebuild may select a newer checkpoint and use its KVI as the new open's
  bootstrap artifact.
- **Leader compaction-completion KVI**: the valid KVI written by the leader only after all local pointer-rewrite chunks of
  a full compaction have committed. It seals that physical layout for leader recovery and may become part of a selected
  checkpoint. It is never pushed to an already running follower; it reaches a node only when that node later opens or
  rebuilds from a checkpoint that uses it as the bootstrap KVI.
- **Follower cleanup-only compaction**: the prefix of the normal BTDB compactor that calculates file usefulness and
  deletes only completely unused files from that follower's local disposable cache. It stops before creating a PVL or
  replacing BTree value pointers, has no canonical transaction, receives no leader deletion instruction, and never
  deletes object-store data.
- **Follower-local KVI**: optional local-cache recovery metadata created by a follower when BTDB requires a newer KVI to
  make local deletion restart-safe. It is never streamed, uploaded, selected by canonical database state, or allowed to
  consume a canonical file identity.
- **Replica**: one local `BTreeKeyValueDB`, cache of canonical TRL/PVL and restored checkpoint files, event consumer, and
  failover coordinator. It need not mirror leader-local KVI files.
- **Canonical TRL**: the byte stream whose identity is authored only by the current leader. Followers may mirror the
  received bytes or promote byte-identical locally cached transaction bytes after exact comparison, but they never
  publish unmatched speculative output as canonical.
- **Canonical frame**: a length-delimited, authenticated direct-transfer record containing one protocol control or
  ordinary event-consumption transaction and its term, sequence, hashes, and TRL coordinates. Compaction operations use
  separate control messages and are not canonical frames.
- **Checkpoint**: an immutable, self-consistent canonical BTDB file set at one event position and canonical sequence.
- **Manifest**: an immutable description of every object needed to restore one checkpoint, including hashes and the
  event position.
- **Leader**: the session currently named by the leased and CAS-protected leader record. While write-ready, it leads every
  database in the failover cluster, authors their canonical TRL bytes, publishes checkpoints, runs and distributes full
  compaction, and owns remote garbage collection. Followers still execute application code
  speculatively, may serve reads from accepted roots, and may clean completely unused files from their local caches.
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
- **Database state record**: one small mutable object per database that publishes the accepted transaction-aligned TRL
  boundary, canonical sequence, event position, and checkpoint manifest under the current cluster leadership term.
- **Durable TRL boundary**: the database-state-selected recovery cursor immediately after a complete canonical
  transaction. It consists of an immutable boundary descriptor plus the exact final `(fileId, offset)`, sequence, and
  frame-chain hash. The descriptor may span several physical TRL files and is published by one database-state CAS.
- **TRL transaction segment**: one contiguous byte range belonging to a canonical transaction in one physical TRL file.
  A transaction has one or more ordered segments; file rotation does not terminate the transaction, and only the final
  segment contains its commit command.
- **Conditional TRL append**: a semantic storage operation that makes an exact byte suffix visible only if the remote
  file still has the opaque version and length previously observed by the caller. The architecture does not expose how
  a provider realizes this operation.
- **Local working copy**: the ordinary BTDB files used by the in-process `BTreeKeyValueDB`. It is an untrusted,
  disposable cache until its complete file graph and boundary identity have been validated.

## Target safety properties

These invariant IDs are the common vocabulary for transition validation, adapter conformance, and fault tests.

| ID | Invariant |
| --- | --- |
| I1 Authority | Only the lease-owning, leader-record-selected session may author canonical work, and only after every active database is activated. Loss of a peer session never grants authority. Stale authority stops canonical acceptance and publication. |
| I2 Ordered history | Initialization establishes sequence zero with an empty transaction and starting cursor. Thereafter a leader transaction atomically stores a contiguous event range's mutations and final cursor; all replicas may batch independently; an in-transaction event-end Ulong update delimits every event. Failed batches roll back and retry individually; a failed singleton becomes an ordinary metadata-only skip. Canonical sequence advances per committed transaction, not per event. No new TRL kind exists; compaction and term changes consume no application position. |
| I3 Confirmation | Compare decoded TRL structure over identical event coverage while ignoring batching/framing differences. A match advances metadata only; a structural mismatch fences reads/commits and restarts the follower for canonical rebuild. No historical confirmation roots or live suffix repair. |
| I4 Local execution | Follower commits perform local synchronous work and never wait for leader progress, external I/O, or a fixed speculation window. Resource exhaustion may fail local I/O explicitly; it cannot promote speculative data. Comparison indexes and pending TRL may spill to disk; no replication-owned historical roots accumulate. |
| I5 Durable closure | One database-state CAS selects a complete, verified recovery graph ending after a whole transaction, including every segment across TRL files. Physical length and listings never select progress. The checkpoint belongs to the selected ancestry and cannot be ahead of its durable boundary. |
| I6 Publication fence | A serialized publisher validates every database-state mutation. Adoption excludes pending predecessor ETags; ambiguity is reconciled without assuming that cancellation or a read of old state drained an outstanding write. |
| I7 Database set | Generation never decreases and equal generations require equal name sets and configured instance identities. Published initialization fixes each added database's initial history; unpublished additions may restart from scratch. Removed names are never reused; old nodes continue independently and delayed writes to abandoned state are harmless. Unselected provisional copies are discarded. |
| I8 Recovery | Every local file is untrusted cache. Open/rebuild validates one complete selected recovery identity or fails closed. Across terms, copy only validated complete optimistic events beyond the adopted canonical end; regenerate unavailable events from retained input. |
| I9 Reader lifetime | Every live root retains its original readable bytes, including abandoned speculation and optimistic readers. Local deletion depends only on that node's complete pins and recovery cut; remote deletion protects the currently published recovery closure, not in-progress follower restores; superseded files can be deleted immediately and affected followers restart. |
| I10 Maintenance | Only the leader performs full compaction. Verified PVLs and bounded physical controls are optional for a follower and never carry TRL transactions, later bootstrap KVIs, or local deletion commands. Leak removal enters only as a parent-published ordinary event. |
| I11 Volatile execution | Shutdown/retirement switches at a transaction boundary to readable `.temptrl` scratch. It never publishes, hard-flushes, checkpoints, acknowledges durable progress, or promotes that suffix. Startup removes scratch before constructing canonical files; cleanup failure prevents opening. |
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
    clusterId / leadershipTerm / leaderSessionId / leaderRecordRevision
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
    descriptorKey / descriptorHash
    end: TrlCursor

CheckpointReference
    position: CanonicalPosition
    manifestKey / manifestHash
    physicalLayoutIdentity

ResumeToken
    authority: AuthorityIdentity
    accepted: CanonicalPosition
    cursor: TrlCursor
    physicalLayoutIdentity / canonicalAllocationWatermark
```

A canonical position names application history, not permission to extend it and not a cryptographic audit of the whole
BTree. Authority is validated separately. A term adoption preserves its predecessor position while changing authority
and opening a new TRL lineage. The descriptor proves that connection; implementations must not synthesize ancestry
from equal integers. The published initial empty transaction establishes sequence zero and its unique genesis frame-chain identity.

Positions are ordered only within the same database/stream and proven ancestry. A new term can restart from an older
durable position; sequence or event position alone does not compare two branches. A read token always includes stream
and database identity. `physicalLayoutIdentity` preconditions physical work but never changes logical ordering.

Every progress surface distinguishes `confirmed: CanonicalPosition`, `durable: RecoveryBoundary`, and a local speculative
cursor `(nodeSession, speculationGeneration, eventStreamId, eventPosition)`. Provisional and retired-volatile progress
also has no canonical position. A resume token's authority must be refreshed after takeover; a retained historical
position can still be the new term's proven base. Peer-supplied deadlines are not authority evidence. `EventRange` denotes contiguous ordered coverage after the base
cursor through the result cursor, not numeric cursor subtraction. Frames and durable descriptors bind the same range,
count, and outcome into their hashes; the decoded transaction's stored cursor must equal the range end. Coverage is
verified against input when available and against selected descriptor ancestry during durable replay.

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
- the application generation/database set provider, compatibility decisions, provisional database factory, and initial event-cursor selection;
- leader-centered follower sessions, frame/control streaming, follower status reports, artifact reads, handoff responses,
  compaction-operation results, and their authentication context;
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
PeerTransport.ReportCompactionResult(peerEndpoint, credentials, connectionId, result, cancellation)
PeerTransport.RespondToHandoff(peerEndpoint, credentials, connectionId, response, cancellation)

PeerEndpoint.HandleLeaderSession(requestContext, followerHello, resumeVector, cancellation)
PeerEndpoint.HandleArtifactRange(requestContext, artifact, range, cancellation)
PeerEndpoint.HandleFollowerStatus(requestContext, connectionId, status, cancellation)
PeerEndpoint.HandleCompactionResult(requestContext, connectionId, result, cancellation)
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

The upstream system must provide more than "Kafka-like" storage in the abstract:

- Each logical BTDB database needs one total event order. Kafka orders only within a partition, so either one
  partition feeds one database or a deterministic merge/sequence must be defined above multiple partitions.
- Every replica must receive the complete stream. Replicas sharing one ordinary Kafka consumer group would divide
  events instead of broadcasting them and therefore do not satisfy this model.
- A node must be able to seek explicitly from the event position stored inside BTDB, regardless of any broker-side
  consumer offset.
- The source must retain events long enough to satisfy the checkpoint recovery window.
- Nondeterministic inputs such as current time, randomness, node identity, and external reads must be carried in the
  event payload or otherwise made deterministic.

### Event batches and application failures

Every replica may combine multiple application events in one BTDB transaction. Each independently chooses its batch
from available input and local byte/time budgets; followers never wait for the leader's batch plan. Handlers run in
stream order. Immediately after each completed event, write its event-end marker into TRL using a reserved event-cursor
Ulong update. The batch's final commit publishes all of its events atomically. The marker itself is not a commit.
Canonical sequence advances per leader transaction; event position advances through the events within it. Provisional
and disposable local execution may use the same batching without acquiring canonical publication rights.

There are two different kinds of batching: application batching combines events inside one BTDB transaction; remote
publication batching combines already committed whole BTDB transactions in one state publication. Neither determines
the other, and neither allows a partial transaction to be published.

The local execution procedure is:

1. Start from the local committed head and independently choose a contiguous event range. A follower head may be speculative.
2. Run each handler in order and emit its event-end marker. If all succeed, commit once, retaining the range, exact TRL
   segments and comparison metadata; do not pin a resulting root for replication. Return without external I/O.
3. If any handler throws during a multi-event attempt, roll back the **entire** attempt, including earlier successful
   handlers in that attempt. Restore its starting event cursor, application state, and safe TRL writer cut. No attempted
   event is considered consumed and the failed attempt gets no canonical sequence/frame.
4. Retry every event of that failed range in original order, each in its own transaction, including events before the
   throwing event and events the failed attempt had not reached. Do not recursively rebatch that retry range.
5. If an individual handler throws, roll back its attempt completely, then commit a fresh metadata-only transaction
   advancing past that one event. Its outcome is `skipped`; its application key/value mutations are empty. Continue with
   the next event. The skip commit itself must succeed before the cursor advances.
6. After the failed range has been retried individually, local batching can resume. Followers' successful and
   skipped results remain speculative until the leader's per-event structure confirms them; a mismatch restarts the follower.

Only application-handler failure follows step 5. Cancellation due to shutdown/lost authority, missing input, corrupt
state, and commit/storage failure are operational failures; they cannot be converted into a successful skip. In particular,
an ambiguous commit is reconciled, not followed by a second cursor-advancing transaction. Exception classification and
bounded diagnostics are part of the application adapter contract.

The skip is an ordinary BTDB transaction using existing metadata and commit encoding, not a new `KVCommandType` or
maintenance transaction. Store the event cursor in transaction metadata (for an integer cursor, `SetCommitUlong` can use
`CommitWithDeltaUlong`); it requires no application-key upsert/erase or BTree-key traversal. Root metadata, TRL position,
and canonical sequence still advance, so "no BTree change" means unchanged application key/value contents, not absence
of all commit work. The failed handler's allocation counters and metadata changes must not leak into the skip (B3).

A rollback also discards the uncommitted attempt's TRL suffix, including any crossed files, from the canonical writer
view. Reset/truncate only proven-private uncommitted ranges; never change a selected prefix or bytes retained by readers.
Where safe truncation cannot be proven, abandon the private generation and reopen from the retained cut. A rollback
marker alone must not cause failed mutations to enter the successful frame. The opt-in core capture API must distinguish
attempt bytes from the final committed retry/skip bytes. This is separate from abandoning an already committed follower
speculative suffix, which still follows the common reconciliation path.

Duplicate input at or before the local cursor is already consumed, whether applied or explicitly skipped. An input gap
is not a skip: stop and obtain the missing event. Broker offsets are resume hints; the cursor stored atomically with the
BTDB transaction is authoritative. A failed batch consumes no cursor until the individual retry commits do so.

### Per-event TRL boundaries and independent batching

Every committed event has a recognizable event-end Ulong update in TRL, including empty and skipped events. It records
the ordered event ID/cursor without committing the enclosing transaction. Reserve its metadata slot explicitly, disjoint
from ObjectDB counters. Initialization writes the starting cursor as before; it is not a consumed event. A failed batch's
markers are rolled back with the rest of its private bytes and never become comparison or takeover input.

Source constraint: `BTreeKeyValueDBTransaction.SetUlong()` currently changes the writable root only.
`BTreeKeyValueDB.CommitWritingTransaction()` emits `DeltaUlongs` at commit relative to `_lastCommitted`. Calling
`SetUlong()` after each event therefore does not yet produce per-event TRL markers. The opt-in event-end operation must
emit the reserved Ulong update immediately, using the existing `DeltaUlongs` encoding, and maintain a last-emitted
metadata baseline so final commit does not emit the same delta twice. Replay already applies `DeltaUlongs` inside the
open transaction. Preserve ordinary BTDB behavior when this opt-in mode is disabled.

Flush meaningful application/ObjectDB metadata changes in a consistent order before each event-end marker, so an
allocation or metadata update is attributed to its event rather than moved to an arbitrary batch end. Decode delta
values against their tracked baseline; stripping `CommitWithDeltaUlong` must not lose the final cursor. The final commit
cursor must agree with the last event-end marker. Exact reserved-slot integration and per-event ObjectDB metadata
capture remain B3. This adds an event boundary using an existing command, not a new maintenance transaction kind.

The comparison worker validates committed transaction closure, then compares ordered event command groups ending at
the same event markers. Ignore transaction start/commit framing; physical file boundaries and compression representation
are decoded away. Compare mutation kinds, keys, values, deletion ranges, meaningful metadata and event identity.
Applied/skipped outcomes remain in comparison metadata, including the distinction between successful empty events and
skips. Never ignore actual state changes merely because their encoding includes an Ulong update.

```text
Leader:   Begin  [event 1 commands, EndEvent(1)] [event 2 commands, EndEvent(2)] Commit
Follower: Begin  [event 1 commands, EndEvent(1)] Commit
          Begin  [event 2 commands, EndEvent(2)] [event 3 commands, EndEvent(3)] Commit
Compare:         event 1 == event 1; event 2 == event 2; event 3 waits for leader coverage
```

Only groups from successfully committed local and leader transactions are eligible. A marker in an open or rolled-back
batch is not enough. Comparison may stop at an event boundary inside a committed follower batch, without retaining a
BTree root at that boundary; global confirmed canonical sequence still advances only at complete leader transaction ends.
Different batching alone never causes a restart or requires application re-execution.

A follower keeps one current BTree plus a disk-backed event/TRL index. Successful comparison advances confirmation
metadata without BTree work. A genuine structural/outcome mismatch fences and restarts the follower for canonical
rebuild. Lag merely waits for committed event coverage; unrecoverable corrupt comparison bytes also trigger rebuild.
There is no live suffix correction or old replication-root retention. Ordinary user read transactions keep their pins.

The follower writes optimistic TRL to a separate local namespace alongside its canonical cache. Incoming leader bytes
never overwrite files referenced by the running speculative BTree. Keep complete unconfirmed event groups and the
comparison baselines needed to interpret them on disk; release compared data only when no read, current-tree reference,
comparison or takeover requirement needs it. Differently batched local bytes are not uploaded while following. Only the
new leader may append eligible groups through [optimistic tail adoption](#optimistic-tail-adoption-on-takeover).

On structural mismatch, discard the suspect generation and rebuild from canonical checkpoint/TRL in a fresh process,
reconstructing ObjectDB caches and counters. Do not use that mismatching tail for takeover. Covered event handlers do
not run during canonical replay. Fresh later events use normal application consumption. Failed uncommitted application
attempt rollback and cold recovery remain distinct from restarting a divergent committed follower.

A durable skipped-event transaction replays from TRL without calling the throwing handler. If an unpublished tail is lost
at takeover, the successor reconsumes those inputs and may choose different batches or a different applied/skipped outcome
if the earlier failure was transient. This follows the existing allowance for unpublished-tail rollback. An irrevocable
skip/result acknowledgement requires its durable boundary or an authoritative upstream outcome record; a local failure
or a leader-local frame alone cannot promise that stronger guarantee.

## Leader-centered follower sessions and election triggering

Decision recorded 2026-08-30: version one has no peer-to-peer membership or failure-detection mesh. Every follower
communicates only with the current leader through the injected peer transport. The production mapping is authenticated
HTTP hosted by the parent application's Kestrel pipeline; the deterministic mapping is an in-process adapter with the
same protocol semantics.

A joining or reconnecting follower:

1. reads `cluster/leader.json` from Azure and treats its term, session, generation, database
   instance set, endpoint, API key, and lease observation as the only leader-discovery/authority source;
2. validates that active per-database state records agree with the selected leader or expose an activation in progress;
   removed databases are excluded from this check and continue independently on old nodes;
3. opens one long-lived logical leader session carrying its identity, compatibility information, per-database resume
   vector, and candidate endpoint;
4. receives leader heartbeats, canonical frames, durability progress, and handoff control messages on that session;
5. periodically reports its accepted and speculative progress and out-of-band compaction-operation results to the same
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
predecessor is the leader-selected database set. The old leader streams frames only for continued matching instance IDs. It
does not send a retired-by-target database to a follower that no longer hosts it, and it sends no frame for the target's
provisional additions. Such a node is not ready for the old database set as an ordinary same-generation failover candidate,
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
        initialMode = replicated | provisionalLocal | retiredVolatile
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
        mode = replicated | provisionalLocal | retiredVolatile | reopeningFromLeader
        confirmed: CanonicalPosition?
        durable: RecoveryBoundary?
        speculativeHeadEventPosition
        speculativeTransactionCount
        speculativeBytes
        cursor: TrlCursor?
        physicalLayoutIdentity / canonicalAllocationWatermark
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
    kind = heartbeat | termStart | databaseSet | databaseLifecycle | databaseGenesis | databaseBootstrap |
           canonicalFrame | trlProgress | compactionStart | compactionChunk | compactionComplete | steppingDown |
           handoffOffer | handoffFinal
    payload

TrlProgress
    base: CanonicalPosition
    result: CanonicalPosition
    segments[] = { fileId, startOffset, endOffset, segmentHash }
    end: TrlCursor
    durableBoundary: RecoveryBoundary?
    rangeHash
    durability = leaderLocal | objectStorePublished
    databaseStateRevision
```

`databaseSet` is the first state-bearing message on a new session and must match the names and generation in the
leader record. `databaseLifecycle` reports additions and removals derived from the generation and name sets, with active instance identities from per-database state. It requires no durable retirement record. `databaseGenesis`
names the published initialization boundary from which provisional followers must close, restore, and reopen; it never asks a
receiver to merge provisional state.

`databaseBootstrap` is emitted only while a follower database is opening or rebuilding and has no accepted usable local
KVI. It names exactly one state-selected checkpoint KVI plus its immutable manifest/file closure. The follower obtains
the hash-identified artifacts through the leader's HTTP artifact endpoint, with the same object-store-selected closure
remaining the authoritative fallback, and opens the database from that one KVI before normal frame streaming. Once the
database reports running, the leader never sends another `databaseBootstrap` or later-created KVI during that open
lifetime. A future restart or explicit rebuild begins a new bootstrap and may select a newer checkpoint KVI.

The distinction between `leaderLocal` and `objectStorePublished` prevents fast direct delivery from being mistaken for
durable remote progress. Because a recent object-store gap is recoverable from the event log, the leader can stream
local progress immediately and publish remote progress in batches. Duplicate messages are idempotent; gaps or an
unexpected term/session force resume or authoritative rereads rather than best-effort merging.

The API key is not distributed by peers. A node learns the authoritative leader endpoint and current bearer key only
by reading the Azure leader record. Possession of that key authorizes requests within the shared storage trust domain,
but neither a successful HTTP request nor a control message establishes leadership.

The leader primarily distributes exact TRL frames through the authenticated resumable session. A follower falls back
to the database-state-selected range of the remote TRL file, then to checkpoint restore and upstream-event replay when
the direct retained range is unavailable. Large artifacts use separate range requests so stream backpressure does not
require the leader to retain unbounded data in memory.

Full-compaction controls use the same authenticated session and artifact range endpoint, but a distinct message family.
They are term-qualified, resumable by compaction operation/chunk identity, and never inserted into `TrlProgress` or the
canonical frame chain. Losing that delivery may forgo a physical optimization; it cannot create an application gap.

Decoded structural comparison is the confirmation path. Physical locations and transaction grouping may differ while
normalized mutations agree. Canonical frame-chain identity comes from the leader bytes, which stay separate from local
BTree value files. Followers never initiate pointer rewriting independently.

## What the current BTDB code implies

The existing code provides useful integration points, but it is not currently a multi-node protocol.

### BTDB file behavior

- `BTreeKeyValueDB` serializes writing transactions inside one process. That lock says nothing about another process.
- `BTreeKeyValueDBTransaction.MakeWritable()` immediately writes `MagicStartOfTransaction` to the one active TRL.
  Cursor mutations then write their commands directly to that writer before changing the writable BTree root.
- `Commit()` is synchronous. It writes metadata deltas and the commit marker, flushes or hard-flushes according to
  `DurableTransactions`, updates the root's TRL cursor, and only then replaces `_lastCommitted`.
- Copy-on-write roots and reference counts support ordinary read transactions. Replication does not keep an extra
  historical root for each follower commit or require an API to restore committed speculative suffixes.
- Values longer than seven bytes use `(fileId, valueOffset, valueLength)` references. Structural comparison must decode
  actual values while preserving the running tree's local file namespace; byte-coordinate equality is not required.
- Existing rollback aborts the current uncommitted writing transaction. Failed application attempts need that rollback
  plus correct private TRL/counter handling. Committed structural divergence instead restarts the process.
- A large transaction can cross physical TRL files. Several command writers call `WriteStartOfNewTransactionLogFile()`
  when the current file reaches `MaxTrLogFileSize`, even though the transaction is still open. Startup replay preserves
  `_nextRoot` across that file boundary and publishes it only after a later `Commit` or `CommitWithDeltaUlong`. Therefore
  `EndOfFile`, a new TRL header, or a file-length cut is not by itself a transaction boundary.
- `.kvi` and `.pvl` files are written as new files and become immutable after finalization.
- File IDs and generations are allocated from local collection state. In failover mode, canonical `.trl` and `.pvl`
  allocation must instead be leader-assigned for canonical objects. Followers keep separate local allocation and file
  identity because batching can produce different physical bytes. KVI
  files are not part of live compaction distribution. Any follower-local KVI must use a disjoint cache identity, and a
  future leader must allocate canonical IDs from canonical cluster state rather than its local maximum file ID.
- Compaction creates replacement files and removes obsolete files. Remote deletion follows publication of the replacement recoverable KVI/manifest; after that it need not wait
  for followers or a grace period.
- `LoadTransactionLog()` already decodes TRL commands into a writable root, but it is a startup-oriented routine that
  mutates loader fields and publishes commits as it scans files. The structural comparator should share validated command decoding without applying commands to a second tree.
  Restart recovery keeps canonical replay; no live historical-root restoration API is required.
- Current compaction calls `CommitFromCompactor()`, which swaps a rewritten BTree root without emitting TRL commands.
  Replication needs an opt-in API that captures and applies bounded physical rewrite chunks under the database writer
  gate for out-of-band distribution. It does not need a new TRL command or a replay-time skip rule.
- Before rewriting any values, current `Compactor.RunCore()` calculates file usefulness,
  `MarkTotallyUselessFilesAsUnknown()`, and conditionally `DeleteAllUnknownFiles()`. Only later does it enter
  `CompactOnePureValueFileIteration()` and `ReplaceBTreeValues()`. This is the concrete cut where follower compaction
  stops: local useless-file cleanup is allowed, while PVL creation and pointer replacement remain leader-only.
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
WriteEventEnd(eventIdentity, outcome)
    -> emit metadata deltas and event-cursor Ulong update without committing

CommitFollowerSpeculation(eventRange, outcome)
    -> local TRL segments + disk-indexed comparison metadata; no retained root

RollbackFailedApplicationAttempt(startRoot, startWriterCut, applicationState)
    -> restored private attempt state + discarded uncommitted bytes

CommitSkippedEvent(eventIdentity)
    -> ordinary metadata-only transaction + unchanged application keys

CaptureLeaderCommit()
    -> canonical TRL segments with event ends + event coverage/outcomes + comparison metadata

CompareFollowerRange(localEntries, canonicalFrames)
    -> structurally equal | awaiting coverage | structural mismatch | invalid bytes

ConfirmMatchingRange(canonicalResult)
    -> advance confirmed metadata; leave current BTree unchanged

AppendOptimisticEventsAfter(adoptedBoundary, committedLocalGroups)
    -> deduplicated new-term canonical transactions + reconciled publication result

RestartDivergentFollower(reason)
    -> fence session + bounded host restart + canonical rebuild on startup

ValidateClosedTransactionBoundary(segments, expectedEnd, expectedSequence, expectedFrameHash)
    -> valid closed transaction | incomplete/corrupt

CaptureLeaderCompactionChunk(basePhysicalLayout, relocationData, cancellation)
    -> bounded out-of-band rewrite operation + resulting physical-layout identity

ApplyLeaderCompactionOperation(currentHead, operation, verifiedPvlFiles)
    -> applied physical layout | retained older compatible layout | rejected invalid/stale operation

RunFollowerLocalFileCleanup(retainedRoots, localRecoveryCut, cancellation)
    -> deleted completely-unused local files + optional follower-local KVI; no canonical or remote effect

ReplayStartupTransaction(kviIdentity, trlTransaction)
    -> applied ordinary event-consumption state | failed corruption

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
to append/cache the transaction, commit the root, and retain disk-backed comparison metadata. It must not await the peer
transport, leader progress, object storage, or a background queue with a fixed speculative-window limit. Volatile
shutdown is the explicit no-persistent-append exception described below.

The comparison coordinator is asynchronous, validates leader authority and contiguous ranges, and advances confirmed
metadata without changing the BTree. It uses bounded buffers and a disk-backed index rather than retained roots. There
is no live reconciliation replay or long suffix-repair writer critical section. Structural mismatch invokes restart.

There is no protocol limit on speculative event distance. Pending TRL/index entries grow on disk while leader progress
lags; RAM does not grow through historical root pins. Report lag in count, bytes, event distance, and age. Ordinary disk
exhaustion can fail local I/O explicitly, but the follower does not wait for leader progress before committing. Normal
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
never hard-flushed, uploaded, placed in a manifest, or renamed into the canonical file set. Clean shutdown deletes them
after the volatile root is released. Startup removes every recognized `.temptrl` file and abandoned prior-session scratch
directory **before** constructing the canonical file collection, scanning KVI/TRL metadata, or reporting readiness. It
never parses or replays those files; failure to remove them keeps the node unavailable rather than risking that
scratch data is mistaken for canonical state. Cleanup is restricted to an exact configured failover scratch root and
validated session/file names; it must never follow links or sweep an ambient operating-system temporary directory.

Entering volatile shutdown mode is irreversible for that node session. Its commits advance only the disposable BTree
head and process-local event cursor. Temporary files receive only node-local volatile identities; they do not allocate
canonical transaction sequences, advance confirmed metadata, emit `leaderLocal` frames, enqueue object-store
publication, write KVI/PVL files, or acknowledge upstream events at a durability level that the shutdown canonical cut
did not reach. A successor deterministically re-executes those events from its adopted durable base. If temporary storage
is full or lost, the affected local transaction may fail explicitly; the generation must never fall back to Blob storage.

### Application state and reader generations

Follower structural divergence restarts the process and reopens canonical state, rebuilding ObjectDB allocation counters
and metadata caches naturally. There is no live BTree rebase or migration of old readers across canonical file namespaces.
Failed uncommitted application attempts still need correct rollback of private TRL, counters, and caches before a retry
or metadata-only skip; this narrower transaction integration remains B3.

Within a running process, ordinary readers and the current BTree must retain their original file-ID-to-bytes mapping.
Canonical incoming bytes remain separate from differently batched local files. Local cleanup accounts for user-held
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
canonical progress. Either durability choice affects local restart work only, and object storage is never awaited by a
follower application commit.

The important new core integration point is a consistent checkpoint export:

1. select one committed root and its atomically stored event position;
2. create or select a `.kvi` that describes exactly that root;
3. record an immutable byte cut for every referenced file through the selected root's exact post-commit cursor; when its
   transaction crossed TRL files, include every segment and never substitute a physical file end for transaction closure;
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

Whether mirrored bytes use `DurableTransactions=true` is an RTO/performance choice. With it disabled, a crash can move
a node's local accepted cursor backward, but recovery remains correct if the durable canonical boundary and upstream
event inputs are available.

### Canonical transactions and parent-published leak removal

Decision recorded 2026-09-01: replication introduces no BTDB maintenance transaction. After the initial empty cursor transaction at sequence zero, every canonical TRL transaction
consumes a contiguous range of ordinary upstream application events, or records one skipped handler failure through
ordinary metadata/commit encoding after rollback. Term changes, checkpoint publication,
compaction controls, artifact transfer, KVI creation, and file-retirement decisions are protocol or storage operations
outside TRL and consume no canonical transaction sequence.

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
replica through normal consumption, the application's handler uses the current event/batch transaction, erases every key
that is still present, and ignores already-absent keys. Each event ends with its cursor marker; the batch commits its final event position atomically.
Removed exact keys are never reused, so delay, overlap, or a second removal event cannot make an old candidate erase newly repurposed data.
Even when all keys are already absent, a successful commit consumes the event exactly once as part of its range.
A handler failure follows the ordinary batch rollback and individual retry/skip rules; it is not a maintenance exception.

The current leader authors the resulting ordinary canonical TRL transaction because it consumes the event as usual.
Followers may batch independently and use event-delimited structural comparison, restarting on mismatch. Startup sees only a normal application transaction. Leader change, disconnection, or graceful
shutdown needs no leak-specific recovery: the retained event is replayed in total order by the successor. Detection can
run again and the parent may deduplicate by request/list identity or publish another idempotent event.

The list-size budget bounds one handler's work; the local application-batch budget also bounds combined writer work.
Larger results are split by the parent into several bounded ordered events or detected again later; this batching policy is part of the application-event contract rather
than a replication transaction format.

### Leader full compaction over the follower session

Decision recorded 2026-09-01: the CAS-confirmed leader alone runs full BTDB compaction. Sealed PVL artifacts and bounded
physical pointer-rewrite operations are distributed out of band over the existing authenticated leader-to-follower
session. They never appear in TRL, never advance application event position or canonical sequence, and require no new
BTDB command decoder or startup skip support.

Followers do not initiate pointer rewriting. They may independently run only the initial cleanup part of the compactor
against their own disposable local files. This local cleanup has no leader, canonical-sequence, TRL, or object-store
effect.

Today one full compaction iteration:

1. calculates file usefulness and marks completely unused files as unknown;
2. creates and seals a new `.pvl` containing copied live values;
3. builds an old-position to new-offset map;
4. repeatedly calls `ReplaceBTreeValues()` in short writable transactions;
5. publishes each rewritten root through `CommitFromCompactor()` without any TRL command;
6. creates a `.kvi` and eventually retires obsolete files when old readers no longer reference them.

Replication preserves that no-TRL behavior and adds an explicit capture/apply surface around the bounded rewrite. One
operation has semantics equivalent to:

```text
CompactionOperation
    protocolVersion
    authority: AuthorityIdentity
    operationId / chunkIndex / previousChunkHash
    base: CanonicalPosition
    baseLogicalStateIdentity
    sourcePhysicalLayoutIdentity
    targetPvlFiles[] = { fileId, length, contentHash, artifactIdentity }
    orderedRelocationsOrRewritePlan
    resultingPhysicalLayoutIdentity / chunkHash
```

The leader flow is:

1. While authority is fresh, select a confirmed application boundary and current physical-layout identity, create an
   operation ID, run ordinary useless-file cleanup, create and seal target PVLs, and build relocation data.
2. Advertise `compactionStart` on follower sessions. Followers fetch target PVLs through the existing authenticated
   artifact range endpoint and hash-verify complete files before any root can reference them.
3. Under the database writer gate, execute one bounded `ReplaceBTreeValues()`/`CommitFromCompactor()` chunk and capture
   its exact relocation preconditions and resulting physical-layout identity. Send the corresponding
   `compactionChunk` control after the local chunk commits. This message is ordered in the peer stream but is not a
   canonical frame and does not enter the frame-chain hash.
4. Continue in bounded chunks while application commits interleave at safe writer boundaries. After all chunks finish,
   create the leader completion KVI and send `compactionComplete`. That control reports only operation completion and the
   resulting physical-layout identity; it contains neither the KVI nor a local-file deletion command.
5. Retire leader-local sources only after the completion KVI and reader/history pins permit it. Remote objects become
   collectible as soon as the replacement checkpoint is fully recoverable and selected; old follower restore attempts
   do not retain the superseded closure.
   Follower acknowledgements are diagnostic/optimization state, never the remote-deletion proof.

The base canonical sequence, frame hash, logical identity, and source physical identity are captured separately for
each chunk while holding the writer gate; they are not one stale base reused for the whole compaction. Leader stream
ordering places each `compactionChunk` exactly between the application frames that precede and follow its local physical
commit. A follower that deliberately retains the older layout acknowledges and advances past that control message so a
missed optimization never becomes a permanent gap in application-frame delivery.

A follower applies a compaction operation only if its current head is at the required event position and its physical
layout, source files, hashes, and relocation preconditions match. Apply under normal writer serialization while retaining
ordinary reader pins. Never restore an older root or replay a suffix to make a compaction operation applicable.

A follower that has already advanced past the required base, has another valid physical layout, misses a control range,
or cannot satisfy a relocation precondition reports `retainedOlderLayout` and continues normal application processing.
The leader must not block application progress waiting for compaction acknowledgements. A later selected checkpoint can
converge that follower's layout only on a future restart or explicit rebuild that transfers its KVI as the new bootstrap;
it is never pushed into the running database. Until then, ordinary canonical transactions remain logically replayable on
the older layout, and structural comparison ignores permitted physical-layout differences.

Leader-assigned target PVL file IDs are consumed by canonical allocation even when a follower retains the older layout.
Resumable session/database progress must expose the resulting canonical allocation watermark independently of whether
the follower installs the PVL. Before installing later leader-authored files, a follower either proves that its local
speculative namespace did not collide or abandons/remaps the conflicting disposable generation. Learning this metadata
must not gate local event execution and requires neither synthesizing nor skipping a BTDB transaction.

Follower cleanup-only compaction stops at the source-code boundary before `CompactOnePureValueFileIteration()` and
`ReplaceBTreeValues()`:

1. Enumerate and protect every root and file pinned by that node's currently open read-only transactions, application
   reads, confirmation rollback, speculation, compaction application, history, and local recovery cut.
2. Calculate file usefulness and mark only completely unreferenced local files as unknown.
3. If deleting a useless TRL would invalidate the follower's only restartable local cut, either create a follower-local
   KVI first or keep that file. Such a KVI is cache metadata only, uses a non-canonical identity, and is not distributed
   or uploaded.
4. Delete proven-unused files only from that local file collection, then stop. Do not initiate a PVL, rewrite a pointer,
   publish a root/checkpoint, or invoke object-store deletion.

A follower may therefore reclaim local source files after a live rewrite while the leader separately owns remote
retention. The leader neither knows the follower's open read-only transactions nor sends a deletion list; receiving
`compactionComplete` does not authorize local deletion. If local cleanup intentionally leaves no self-contained older
local recovery cut, a later process restart must reject that local cache and restore a selected checkpoint from object
storage; it must never guess around missing files.

Startup requires no compaction awareness in the TRL reader:

1. Select the newest valid local KVI or obtain exactly one bootstrap KVI and its selected checkpoint closure through the
   leader's artifact endpoint, falling back to the identical hash-verified objects in object storage.
2. Replay only ordinary application transactions after that KVI. There is no compaction command to recognize or skip.
3. A KVI/checkpoint captured before compaction naturally reconstructs the older physical references; a selected
   checkpoint captured after compaction naturally starts with the newer layout. Both produce the same logical contents
   at an equal application boundary.
4. If follower cleanup removed a source file required by its chosen older KVI, that local generation is invalid and is
   rebuilt from a complete selected checkpoint. Recovery never guesses around the missing file.
5. An incomplete or unreferenced target PVL and partially received operation metadata are disposable local debris.

If leadership is lost during full compaction, no further operation chunk, KVI/checkpoint, or remote deletion may be
published by the old leader. Already completed local or follower chunks remain logically valid physical layouts, but
they grant no authority and need not be reproduced after restart. Without a completion KVI, source files remain retained;
a later leader may discard the disposable local generation, restore the selected durable boundary, or start a new
compaction operation.

Graceful shutdown requests cooperative cancellation immediately and prevents another compaction chunk from starting. A
bounded chunk already inside the writer gate may finish or abort at that safe boundary; the persistent-to-volatile switch
waits only for this bounded work, not for the whole compaction. Once volatile mode begins, the node sends no further
compaction control, writes no target PVL or KVI, publishes no compaction artifact, and makes no file-retirement decision.
Application transactions continue against the current in-memory root throughout the remaining shutdown lifetime.

KVI creation and physical deletion are recovery/retention operations rather than logical BTree mutations, but their
file-ID allocation and lifetime still need protocol coordination. Canonical TRL/PVL identities remain leader-assigned.
Leader completion/checkpoint KVIs and follower-local KVIs are not live-mirrored file allocations. A canonical KVI is
transferred only when selected as the bootstrap KVI for a database open or rebuild; no newer KVI replaces it during that
open lifetime. Local-only KVIs need a disjoint namespace or mapping so they cannot collide with a future leader-assigned
canonical data file. Local deletion remains a node-private decision, while remote garbage collection is a
leader-authorized object-store operation regardless of which nodes have already deleted the equivalent local files.

### Existing `BTDB.AzureStorage` is not the failover protocol

The current [`AzureBlobFileCollection`](../BTDB.AzureStorage/AzureBlobFileCollection.cs) is a useful single-node local
cache and asynchronous backup mechanism, but its current contract does not provide the conditional publication and
multi-node fencing required by this design. `BTDB.Replication` may reuse lower-level transfer ideas, but it needs
term-qualified or immutable data objects, a complete manifest, CAS-fenced leader/database-state publication, and
manifest-aware garbage collection. The existing adapter details are recorded in
[ObjectStorages.md](ObjectStorages.md#conditional-tail-append-with-block-blob), outside the failover protocol.

## Object-store dependency

The detailed provider-neutral interface, all available `celld` `Bucket` and `BucketOwnership` operations, CAS retry
and ambiguity rules, the live capability probe, and Azure/S3 limitations are documented in
[ObjectStorages.md](ObjectStorages.md).

The architecture depends on these conclusions:

- one small mutable leader object and one state object per database use conditional create/replace with opaque version
  tokens;
- CAS results preserve `Applied`, clean `Rejected`, and possibly committed `Ambiguous` as distinct outcomes;
- conditional writes are not transparently retried; ambiguity is reconciled by reading an operation identity;
- object storage supplies no multi-key transaction, so immutable data is completed before the corresponding database
  state CAS publishes it;
- the database-state CAS is the only durable BTDB publication point and may select only a boundary after a complete
  transaction; an immutable descriptor makes a transaction spanning several TRL objects one atomic logical update;
- the Azure version-one data plane exposes conditional atomic tail append as a semantic operation; physical layout,
  staging, and commit details remain inside the provider adapter;
- bulk data is immutable or term-qualified, and listings never define the authoritative restore set;
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

The JSON contains the active database names directly, with no per-database progress. Per-database state retains
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

### Per-database state record

Each database instance has one mutable state object, for example `databases/main/stable-instance-id/state.json`:

```text
DatabaseStateRecord
    format
    database: DatabaseIdentity
    authority: AuthorityIdentity
    revision / lastOperationId
    durableBoundary: RecoveryBoundary
    activeTrl = { lineageId, fileId, objectKey, committedLength, lastAppendToken, prefixHash }
    checkpoint: CheckpointReference
    previousManifestKey?
    canonicalAllocationWatermark
```

The record's ETag is the CAS token. `durableBoundary` selects an immutable descriptor ending after a complete transaction;
`activeTrl` caches its final file cut for continuation. Its append token is an opaque provider version, not a content
identity. A newer physical token/length after an unselected append is acceptable only when the selected prefix still
hash-verifies. Recovery never derives accepted length, closure, ancestry, or order from that physical state.

The selected checkpoint is on the durable boundary's ancestry and cannot be ahead of it. Its physical layout may differ
from an earlier checkpoint at the same position. The allocation watermark comes from canonical allocation, never a
follower's local maximum file ID.

A state whose authority differs from the current leader is a predecessor during activation, not current-term progress.
Removed databases are not adopted or updated by the new leader and require no retirement-state mutation.
All mutations use the publisher below. The leader record has its own isolated authority lane.

### Database state publisher

One serialized publisher per database owns its state ETag and validates all state mutations. Checkpoint capture, uploads,
TRL preparation, and transition planning may run asynchronously, but none may bypass this publisher to write state.
Serialization is per node; storage CAS and term adoption fence other sessions. It never runs on a follower commit path.

| Intent | Required preconditions | Selected effect |
| --- | --- | --- |
| `CreateGenesis` | No published instance state; current activating authority; initial empty transaction and its file closure verified | Conditionally publish sequence-zero initialization boundary and starting event cursor |
| `AdoptTerm` | Selected predecessor verified; current activating authority; new empty lineage prepared | Preserve canonical position/checkpoint; change authority and lineage; fence predecessor ETag |
| `PublishTail` | Write-ready authority; batch extends selected ancestry; all whole-transaction segments and descriptor verified | Advance durable boundary and matching active-file cut |
| `SelectCheckpoint` | Write-ready authority; complete manifest verified; checkpoint lies on selected ancestry at or behind durable boundary | Replace checkpoint reference only; preserve durable boundary |

Each intent uses the same sequence:

1. Prepare and hash-verify immutable prerequisites without claiming state publication.
2. Enter the publisher, read/reconcile its current state and token, and validate authority, instance, ancestry, intent,
   and I5/I6/I7. Reject stale capture results. Revalidate authority immediately before mutation dispatch.
3. Construct the complete next state from that observed state, preserving fields the intent does not own. Include a
   unique operation ID and next revision. Dispatch one conditional create or replacement with automatic retries disabled.
4. On `Applied`, record the returned token and expose the selected result. A durability notification follows state CAS,
   never an earlier artifact upload. Release publication pins only when the attempt is selected or safely abandoned.
5. On clean `Rejected`, reread, validate authority again, and derive a new intent from the actual winner. Never resend an
   old body against a newly read ETag. A later term stops this publisher rather than authorizing an old-session retry.
6. On `Ambiguous`, stop further state mutation and reconcile operation ID and selected content. An old value means
   unresolved while the request could still land. Do not treat a timeout, cancellation, or read as a write fence.
   Successor adoption can exclude a pending predecessor ETag. Removed database namespaces require no adoption or retirement fence.

A checkpoint capture ahead of the durable boundary waits in the background for tail publication or is abandoned; it
cannot publish checkpoint-only state ahead of recovery. No synchronous application commit waits for that capture.
A checkpoint behind the already selected checkpoint is rejected. At the same canonical position, the same manifest is
idempotent, while a verified new physical layout may replace it by state revision, allowing compaction of an idle database
to become durable. A conflicting logical identity is a safety fault. This adds no application transaction.

Publication ancestry is verified even when a predecessor checkpoint/position was authored under an older database set or term.
The current authority wrapper must agree with the selected database set; historical immutable objects retain their original
identities. Adoption never relabels history as if the new leader had authored it.

#### Transaction-aligned durable boundary

Decision recorded 2026-08-30: every durable object-store cursor must end at a complete BTDB transaction boundary. This
rule applies to the ordered TRL chain, not to an individual physical file, because one transaction may span many files.

The immutable boundary descriptor needs semantics equivalent to:

```text
TrlBoundaryDescriptor
    format
    authority: AuthorityIdentity
    lineageId
    previousDescriptorKey? / previousDescriptorHash?
    base: CanonicalPosition
    result: CanonicalPosition
    fileCuts[] = {
        fileId
        objectKey
        acceptedLength
        prefixHash
        sealed
    }
    transactions[] = {
        result: CanonicalPosition
        input: EventRange
        outcome = applied | skipped
        startFileId / startOffset
        segments[] = { fileId, startOffset, endOffset, segmentHash }
        endFileId / endOffset
        terminator = commit | commitWithDeltaUlong
        transactionHash
    }
```

The exact encoding remains open, but its invariants do not. Every `transactions` entry is contiguous with its
predecessor, all segments and file headers are present and hash-valid, and `endOffset` is immediately after the complete
commit command and metadata. `EndOfFile` and a subsequent TRL header are continuation mechanics when a transaction spans
files; neither closes the transaction. A batch may contain many transactions, but its last byte cut must close the final
one and the state record may advance only to that final boundary.

All referenced file prefixes and the immutable descriptor are uploaded and verified first. Until the state-record CAS
selects that descriptor, they are unaccepted staging data even if individual Blob operations made them physically
visible. A crash can therefore leave either the previous complete boundary or the new complete boundary selected, never
a subset of the transaction's files. Unselected data is ignored during recovery and reclaimed by publication-aware garbage
collection.

#### Per-database term adoption fence

`AdoptTerm` in the [publisher](#database-state-publisher) is the only continued-database adoption algorithm. Read and
validate the predecessor, prepare an empty term-qualified TRL lineage and boundary descriptor linked to it, then CAS
preserving its position/checkpoint. If a delayed predecessor publication wins first, reread its complete boundary and
rebuild the adoption intent. No database authors canonical work while cluster activation is incomplete.

After adoption, every pending predecessor request with the older ETag is excluded. A correct old session is already
self-fenced and cannot regain publication permission by rereading state. Term-qualified data keys separately ensure that
an old append cannot alter a new term's bytes. An old append outside a selected cut never becomes canonical by length.

An added instance has three explicit recovery cases:

| Observed state | Activation action |
| --- | --- |
| Absent | Reconcile ambiguous writes; `CreateGenesis` from scratch with an empty transaction and starting event cursor |
| Present with valid matching instance and ancestry | Restore published history; adopt if its authority is older, or reconcile as complete if already activated by this authority |
| Present with unrelated instance or invalid ancestry | Safety fault; never overwrite existing history |

A crash after creating some additions preserves their published state. A successor initializes only unpublished
additions; the leader JSON does not bind them to the failed candidate's provisional data or proposed starting cursor.
Removed databases require no activation or publication fence and do not delay cluster readiness.

### Cluster transition engine

Bootstrap, unplanned takeover, same-generation handoff, and application upgrade use one engine. Only preparation inputs
and lease acquisition differ. There is no second activation algorithm for shutdown or upgrades.

| Entry | Preparation | Lease path |
| --- | --- | --- |
| Empty cluster | Empty predecessor database set; initialization cursor and inputs available for every instance; conditional leader placeholder | Acquire finite lease |
| Unplanned takeover | Reconstruct selected durable bases; recover any selected transition obligations | Randomized contention after leader-session loss or authority mismatch |
| Same-generation handoff | Exact selected database set; prepared target and final durable-base vector | Old leader's `Change Lease`, then target proves ownership |
| Higher-generation transition | Exact declared predecessor; continued bases, initialization inputs, explicit retirements | Prepared handoff preferred; eligible unplanned contention uses the same activation obligations |

| Phase | Permitted work | Completion / failure |
| --- | --- | --- |
| `Following` | Validate discovery/database set; local speculation; serve reads under the read contract | A qualifying candidate starts preparation. Session failure alone confers no authority. |
| `Prepared` | Verify bases and initialization inputs, protocol and generation eligibility, endpoint, and transition inputs | Acquire/prove lease. Losing preparation changes no selected history. |
| `Selecting` | Reread leader record under owned lease; CAS higher term, session, nondecreasing generation, database names, endpoint, and new API key | Reconcile operation identity and ownership before proceeding. Ambiguity leaves candidate inactive. |
| `Activating` | Run per-database publisher intents; restore each selected base; finish fixed database set obligations | All active instances must match current authority. A partial result survives for idempotent successor recovery. |
| `Leading` | Author ordinary event transactions, stream frames, publish, compact, and perform proven-safe remote GC | Lost/unsafe authority fences all databases. Graceful departure enters irreversible draining. |
| `Draining` | Switch application writes to `.temptrl`; serve existing artifacts; retain only permitted authority/control operations | Transfer/release/expire authority. No path back to canonical writing in that session. |
| `Fenced` | Existing accepted reads subject to policy; non-authoritative local work only when its mode is safe | Rejoin through discovery and validated recovery; never resume old canonical authority. |

The phase labels describe node-local permission, not additional mutable storage records. The leader-record CAS selects
the generation and database names once. `Activating` uses separate database CAS operations and is not cluster write readiness.
After every active base is restored and activated, publish term-start controls bound to those bases and start canonical
event execution in `Leading`. Catch-up/read readiness can remain per database; do not require reaching a moving stream
end before entering `Leading`.

Same-generation eligibility requires the selected database set and every published active durable base; unpublished
additions require the ability to initialize from scratch. Higher generations require a valid transition and available
initialization inputs; lower generations never lead. After acquiring authority, reread all bases because a pending
predecessor publication may have won in the meantime. Initial bootstrap applies the same rules to an empty database set.

The engine's recovery rule is always: inspect published per-instance results, restore existing history, and initialize
missing databases. Failure of one active instance prevents canonical writing for the entire cluster. Selection/activation
deadlines remain explicit blockers, not assumptions that every retry eventually succeeds.

### Election, renewal, and fencing

Election triggering remains [leader-centered](#leader-centered-follower-sessions-and-election-triggering). A candidate
conditionally creates the initial placeholder if needed, then uses the transition engine. Lease ownership plus the
matching leader-record CAS is necessary; neither an authenticated peer message nor an available endpoint is authority.

The leader renews through an isolated authority client/pool, independently of database publication and transaction rate.
Failed or ambiguous renewal that cannot be reconciled before self-fencing stops canonical work for all databases.
Followers likewise stop new acceptance when authority cannot be proven, while existing reads and local speculation have
separate availability semantics. An ordinary leader-record read is not evidence of a fresh full lease lifetime.
The precise safe observation/deadline protocol, including immediate transfer, is blocker B1.

A takeover first selects each database's durable predecessor, which may differ from another database's position.
Previously dispatched publication must be reconciled before deciding its end. The new authority may then copy validated
optimistic event groups after that exact end through the procedure below. Old cached physical file length is never the
cutoff. Removed databases remain outside activation and receive no tail adoption.

### Optimistic tail adoption on takeover

Optimistic TRL is reusable event execution, not an independent authority source. After acquiring the lease, selecting
the new term, and adopting/restoring all active durable bases, pin the candidate's committed optimistic groups at a
complete local transaction boundary. Pause fresh execution while preparing each database's continuation. An unfinished
local batch is rolled back or completed before capture; its event-end markers alone cannot authorize copying.

1. Determine `C`, the last consumed event in the currently selected canonical database boundary, after reconciling
   pending publication. Load `eventsToSkip`. Verify the candidate's database/stream identity, canonical base lineage,
   contiguous event coverage, checksums, committed transaction status, and structural agreement with every overlapping
   canonical event. Cached evidence must bind the actual adopted history; any mismatch rejects the candidate's tail.
2. Exclude all optimistic events at or before `C`. Select the contiguous complete event groups strictly after `C`, up
   to the captured committed head. If a pending skip entry conflicts with a locally applied event, use input recovery
   from that event rather than copying it or dependent later effects. Missing/invalid groups likewise require recovery.
3. Append selected event commands and end markers after the adopted end in the new term's canonical TRL lineage.
   Recreate valid transaction framing where necessary. For example, canonical history through 102 plus a committed
   optimistic batch 101-105 produces only events 103-105 in a newly framed transaction. No command from 101 or 102 is
   copied. A new frame-chain identity and canonical transaction sequences bind the appended result to the adopted base.
4. Copy command payloads without calling event handlers. Decode/re-encode metadata deltas against the boundary baseline
   when cutting a local batch, and preserve compression/file-format validity. Do not concatenate whole files or blindly
   remove a transaction prefix: first verify that the extracted groups replay correctly from `C`. New physical offsets
   are canonical; do not relabel the optimistic BTree's old value pointers as if they had moved with the bytes.
5. Publish only complete canonical transactions through the existing serialized publisher. Use conditional append/CAS
   and operation identities; reconcile a lost response before retrying. A restart rereads the selected end and copies
   only events beyond that end, so a partially published attempt cannot duplicate already selected events. Unselected
   append bytes remain staging data and are handled by the existing append reconciliation rules.
6. Reopen/replay the resulting canonical file generation before admitting fresh handlers, so BTree value pointers,
   ObjectDB counters, caches and the next input cursor correspond to the copied canonical bytes. No application handler
   runs again for the copied events. No historical BTree root is needed. After cursor `E`, consume only inputs after `E`.

This copying path is permitted only for validated optimistic TRL from the same stream and adopted logical history.
Provisional additions and shutdown/retired `.temptrl` remain disposable and cannot be promoted. If no eligible optimistic
cache survives, restore the durable base and consume missing upstream events normally. All-active-base activation and
normal authority checks still precede canonical publication; tail adoption runs before fresh application execution.
Exact extraction, delta rebasing, retry and physical reopen mechanics are part of B3, not a second publication protocol.

### Fast graceful handoff

A planned process shutdown should avoid both waiting for the finite Blob lease to expire and pausing application
transactions. It uses an irreversible persistent-to-volatile transition:

1. The leader sends `steppingDown` over its active follower sessions and requests cooperative cancellation of compaction
   and every background database writer. It prevents new compaction, checkpoint, remote-publication, and
   garbage-collection work from starting; incomplete unselected artifacts remain harmless debris for later cleanup.
2. For each database, it closes admission to the persistent canonical writer. A transaction already inside that writer
   finishes or aborts at its ordinary safe boundary; no later transaction starts in the persistent generation. This
   produces the database's shutdown canonical cut without ever cutting through a transaction.
3. At that boundary the database irreversibly switches new allocation and transaction output to its volatile `.temptrl`
   generation. New application transactions continue immediately with ordinary local single-writer synchronization;
   their BTree changes and newly written values remain readable from local scratch files. They assign no canonical
   sequence, emit no canonical frame, enqueue no publication, and perform no hard flush. Different databases may reach
   this switch independently; the handoff record therefore carries an exact per-database cut vector.
4. The old leader dispatches no new data-plane storage request after the switch. A request dispatched earlier may already
   have reached Azure, so the old leader reconciles any ambiguous in-flight database-state CAS using reads only. A state
   CAS that landed still selects a complete transaction by construction; appended but unselected bytes remain ignored.
   Shutdown does not force the canonical cut to become Blob-durable. The resulting durable-base vector may be older.
5. In parallel with those bounded switches, the leader builds the eligible set from its connected-follower registry. A
   same-generation candidate must match the selected database set, run a compatible protocol/BTDB format, expose a usable
   candidate endpoint, be within the handoff lag limit for every active database, and retain the events needed after
   every final durable base. It chooses randomly and sends the handoff offer over that follower's existing authenticated
   session. A prepared higher-generation target instead follows the database-set transition readiness and initialization rules above
   and has priority over same-generation shutdown targets.
6. The target obtains and verifies frames and prerequisites through each relevant shutdown canonical cut when available,
   and independently verifies every object-store-selected durable base. A higher-generation target additionally
   verifies starting cursors and input availability for its added databases and explicitly lists retirements. The old leader
   may serve already existing bytes but cannot create or persist new canonical ones. The target responds `prepared` with
   progress vectors, proposed initialization cursors, and a freshly generated proposed Azure lease ID. An unpublished old-leader
   tail remains an optimization only; correctness permits dropping it and replaying its ordered application events.
7. The old leader keeps only the authority/control lane alive: leader-record reads, lease renewal, reconciliation reads,
   `Change Lease`, and eventual lease release. These operations preserve or transfer fencing authority and are the sole
   Blob-plane exception to the no-persistence rule; no TRL append, descriptor, database-state/checkpoint update, PVL/KVI
   write, or object deletion is dispatched. It calls `Change Lease` with the target's proposed ID.
8. The target proves ownership by renewing with that ID and enters `Selecting` in the
   [cluster transition engine](#cluster-transition-engine). The common `Activating` phase restores/adopts every required
   durable base and completes the selected database set obligations before canonical work. Handoff bytes are an optimization,
   never a substitute for a published durable base.
9. The old process may continue executing application events against its volatile generation until exit. It closes its
   sessions after observing the new leader record or reaching its local shutdown deadline, releases the volatile root,
   and best-effort deletes its `.temptrl` scratch directory. None of its post-cut work needs to be drained or transferred;
   restart cleanup is authoritative if clean deletion was interrupted.

There is no cluster-wide application quiesce: each database pays only the bounded writer synchronization needed to end
its persistent generation on a transaction boundary. Once a node session enters volatile shutdown mode it never resumes
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

The leader executes the next ordered event range under the batch/retry/skip rules, commits its final cursor, and captures every
TRL segment through the final commit marker. Only a complete transaction gets a canonical position/frame. Local accepted
progress can be streamed immediately under valid authority and queued for background durability.

The TRL preparer batches contiguous whole transactions, conditionally appends the touched file suffixes, and completes
newly rotated objects. It verifies all accepted cuts and creates the immutable linked boundary descriptor. Per-file
success is preparation only. The [database state publisher](#database-state-publisher) then runs `PublishTail`; its CAS
is the sole durable selection point and supplies the position used in `objectStorePublished` notifications.

A rejected/ambiguous append is reconciled against expected token, length, prefix/suffix hashes, and operation identity.
An unproven suffix prevents descriptor selection; abandon the lineage if it cannot be proven to be the exact prepared
batch. A new lineage still links the last selected boundary and uses the same publisher; it must not re-author a
conflicting already-issued frame within the term. State-CAS outcomes use the publisher's common reconciliation rules.

TRL file rotation, including rotation inside one transaction, never publishes progress itself. `EndOfFile` and headers
are continuations, not commits. Batch byte/time limits affect request cost and recovery lag; they never cut a transaction
or add external I/O to follower commits. Graceful drain dispatches no new data request and only reconciles prior work.

### Canonical frame and comparison protocol

The direct protocol needs its own versioned envelope around raw BTDB bytes. An illustrative frame is:

```text
CanonicalFrame
    protocolVersion
    authority: AuthorityIdentity
    base: CanonicalPosition
    result: CanonicalPosition
    lineageId
    input: EventRange
    outcome = applied | skipped
    trlSegments[] = {
        fileId
        startOffset
        endOffset
        segmentHash
    }
    transactionEndFileId / transactionEndOffset / transactionTerminator
    logicalChangeDigest?
    payloadLength
    payloadHash
    payload
```

`result.canonicalSequence = base.canonicalSequence + 1` for each canonical frame, while the event cursor advances through
its nonempty input range. `skipped` requires exactly one event and no application mutations; `applied` covers a successful
range (which may itself have no application mutations). `termStart` is a separate
`LeaderMessage` control, not a BTDB transaction, and consumes neither. Compaction controls and PVL artifacts use their
separate out-of-band message family and never appear in this envelope. `trlSegments` is ordered and can cross any number
of file boundaries. A receiver may stream segments to temporary local files, but the canonical transaction frame is
indivisible: it is accepted only when the final segment ends exactly at the declared commit terminator and every segment
hash is valid.

### One follower acceptance path

Startup correctness comes from canonical checkpoint/TRL replay; a running follower confirms by structural comparison. The same validated transaction representation
is reconstructed from a live frame or from a selected durable descriptor and its TRL segments. Transport/storage adapters
supply bytes and evidence; they do not implement separate acceptance rules.

| Source | Required evidence before transaction validation |
| --- | --- |
| Live leader frame | Authenticated session, selected database set/instance, current authority under B1, and valid resume lineage |
| Durable replay | State-selected or explicitly retained recovery closure, descriptor ancestry, and exact hash-verified cuts; historical author authority need not still be live |
| Local speculative bytes | Comparison evidence only over the same event coverage; structural equality does not make these canonical physical bytes |

For one candidate transaction:

1. Validate the source evidence, complete shared identities, segment bounds/hashes/continuity, payload and transaction
   terminator. No partial transaction can advance a root. Fetch and verify prerequisites before entering writer
   serialization; file installation must preserve retained-reader generations (I9/B3).
2. Classify its relation to the accepted position. An exact previously accepted duplicate is a no-op. Conflicting frames
   for the same authority/database/base/sequence are a safety fault: freeze acceptance and retain evidence. A gap cannot
   advance; resume or rebuild. A new transaction must extend the exact confirmed position and validated lineage.
3. Select the application action from the table below. A successful comparison records the leader result identity while keeping local physical bytes separate. The comparison optimization grants no additional authority.
4. Advance confirmed progress only after the chosen action completes. Publish readiness according to the read contract,
   separately from speculative and durable progress. Report bounded/redacted logical diagnostics on divergence;
   permitted physical and batch-boundary differences are normalized away. Applied/skipped disagreements are outcome
   mismatches. Different-range digests alone cannot establish logical divergence.

| Local condition | Action |
| --- | --- |
| Complete matching event coverage and normalized structure | Advance confirmed metadata; current BTree and local files remain unchanged. |
| Follower has not consumed the complete leader range | Spool frames and await local coverage; do not restart for lag. |
| Structural mutation or outcome mismatch | Fence and restart follower; rebuild canonical state in a fresh process. |
| Unrecoverable missing/corrupt comparison data | Restart/rebuild; do not accept unverified progress. |

The [structural comparison rules](#per-event-trl-boundaries-and-independent-batching) define normalization and restart.
Equivalent transaction grouping and physical layout do not constitute divergence. A matching result does not grant the
local bytes canonical physical identity or provide a readable historical root. Comparison encoding is B3.

Compaction is optional: apply only to an eligible current head or retain the older layout. It needs no live rebase.

### Peer transport abstraction and Kestrel adapter

The core depends only on the peer transport and peer endpoint contracts described in the testability section. It does
not reference HTTP, ASP.NET Core, Kestrel, URI routing, headers, or sockets. Reconnect and resume are expressed in terms
of the shared `ResumeToken` and typed transport outcomes.

The preferred version-one production adapter uses resumable server-streaming HTTP plus small follower-to-leader HTTP
requests rather than WebSockets. Canonical data and control messages flow from leader to follower on one long-lived
response; status, compaction-operation results, and handoff responses flow back as separate authenticated requests. Together
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
POST /_btdb/replication/v1/sessions/{connectionId}/compaction-results
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

The compaction-result request uses the shared binary DTO and accepts only the current operation/chunk identity for that
authenticated session. Its typed result distinguishes applied, retained older layout, missing prerequisite, stale base,
and invalid operation. It is ephemeral optimization feedback and does not acknowledge a canonical transaction. Leak
removal does not use a replication HTTP route; it is requested through the separately injected parent event publisher.

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

### Immutable manifest

A manifest should describe a complete restore point:

```text
CheckpointManifest
    format
    position: CanonicalPosition
    physicalLayoutIdentity
    authoredUnder: AuthorityIdentity
    cursor: TrlCursor
    lastAppliedEventHash?
    logicalStateHash?                // optional independent audit, not the frame-chain identity
    files[] = { fileId, type, length, sha256, objectKey }
```

The manifest itself is immutable and uploaded with create-if-absent semantics after every referenced object is
present and verified. Its key/hash live in the external `CheckpointReference`, never in its own hashed content.

Initialization uses ordinary TRL containing the initial empty transaction and its event cursor, published through
`CreateGenesis`. There is no separate `SeedManifest`. Checkpoints subsequently use `CheckpointManifest` and the common
closure validator and publisher.

Checkpoint capture pins one closed root and its complete file graph while hashing/uploading the immutable objects and
manifest. The resulting `CheckpointReference` is submitted to `SelectCheckpoint` in the
[database state publisher](#database-state-publisher). That is the only selection algorithm, including equal-position
physical replacement after compaction. Losing authority abandons publication; unselected uploads remain staging data
subject to the remote-GC proof. Local pinning and durable remote retention are distinct lifetimes.

## Candidate object layout

An illustrative, deliberately non-final namespace is:

```text
cluster/
    format.json
    leader.json
databases/<database-name>/<database-instance-id>/
    state.json
    trl/terms/<term>/<file-id>-<lineage-id>.trl
    trl/indexes/<index-hash>.json
    manifests/<canonical-sequence>-<event-position>-<manifest-hash>.json
    objects/sha256/<content-hash>
    attempts/<term>/<leader-session>/<attempt-id>/...
    gc/...
```

Important properties are more significant than the exact names:

- Checkpoint objects and manifests are immutable; term-qualified active TRLs preserve every previously selected prefix.
- The leader record selects the generation and database names; per-database state retains published initialization metadata. The leader record contains no mutable database progress. A database state record never references an object that was
  not uploaded first.
- Leadership term and lineage are explicit in mutable-TRL or range keys, so an old leader cannot extend a new term's
  accepted lineage.
- Leader-authored `.trl` bytes and every referenced file must be available on each replica that uses them. Live
  compaction may give replicas the same `.pvl` layout, while a missed control or recovery from an older KVI naturally
  retains older references because TRL contains no rewrite. Such a replica may have a different physical layout, but
  every reference must resolve to the same logical value and remain within files protected by retention rules.
- Use attempt-qualified immutable object keys. Once retired for deletion, a key is never selected or reused again,
  even for identical bytes; a delayed old delete must not remove a future selected object.
- A manifest contains every `.trl`, `.pvl`, and `.kvi` file needed to open its checkpoint. It must not depend on an
  unlisted local cache file.
- The current leader may delete superseded remote files immediately after selecting a complete replacement recovery
  closure. There is no follower acknowledgement, restore lease, grace period, or fallback-manifest retention delay.
  Local cleanup remains independent and protects ordinary local readers and current-tree references.

### Immediate remote cleanup and interrupted restore

Publish and verify the new KVI, manifest, referenced values, and required canonical TRL before selecting that checkpoint
in the database state record. Once selected, remote files absent from the current recovery closure may be deleted
immediately. A newer KVI file merely existing is insufficient: it must be published and restore the database through
the selected durable boundary without the removed files. Files shared with the new checkpoint or its required tail
remain necessary and cannot be deleted.

A follower restore does not pin remote objects. If a file disappears while it is opening/downloading, the follower
abandons that restore attempt and restarts through the host port. Startup rereads current database state and opens the
newest published KVI and its selected closure; it does not retry the old manifest indefinitely or ask GC to wait.
Partially downloaded files are untrusted cache and may be reused only after validation against the new closure.
An already running follower keeps using its local files according to its own reader/current-tree pins; remote deletion
is not a command to delete those local files.

No automatic old-checkpoint fallback retention is required. `previousManifestKey`, if kept for diagnostics, does not pin
its files or promise recoverability. Backups remain a separate administrative facility. A file missing from the still
current published closure is a publication/storage fault, not an expected GC race; keep the node unavailable and report
it instead of serving partial state or asserting a newer checkpoint exists. Repeated restore restarts are observable,
but must not add a remote deletion grace period.

B5 is now only the implementation proof for publish-before-delete ordering, staging protection and permanent key
non-reuse. Old-term in-flight deletes must remain harmless to the current closure; follower restore tracking is absent.

## Event application, checkpoint, and recovery sequences

### Normal event application

The [event-log contract](#event-log-contract-and-deterministic-replay) owns synchronous event execution.
[Canonical TRL publication](#canonical-trl-publication) owns the leader's capture/preparation path;
[canonical frame comparison](#canonical-frame-and-comparison-protocol) owns follower confirmation/repair. These are the
same paths during ordinary operation and catch-up. A draining or retired instance instead follows the
[volatile core path](#minimum-opt-in-btdb-core-extensions), returning explicitly local results with no canonical position.

### Per-database replay hierarchy

Replay is independent for every active database instance; the shared leadership term does not create one cross-database
replay cursor. A replica reconstructs one database in this order:

1. Validate and use a local KVI/file set when it exactly matches a retained manifest or accepted state identity.
2. Otherwise accept one `databaseBootstrap` naming the newest valid immutable manifest and KVI selected by that
   database's state record, and fetch its hash-verified closure through the leader artifact endpoint or object storage.
3. Follow the state-selected linked boundary descriptors and exact cuts from the KVI's TRL cursor. Use the
   [common acceptance path](#one-follower-acceptance-path) in durable-replay mode for ordinary TRL transactions and
   cross-file continuations. Unselected bytes/objects are ignored even when physically present.
4. Stop exactly at the state record's final `(fileId, offset)`. Require the decoder to have no open transaction and the
   resulting canonical sequence, frame-chain hash, event position, and root identity to match the selected boundary.
5. After reaching the Azure durable boundary, connect to the endpoint from `cluster/leader.json` and resume direct
   frames from the exact `ResumeToken`.
6. Consume the corresponding upstream application events to build the follower's speculative suffix and divergence
   evidence. A new leader first reuses validated optimistic TRL beyond the adopted end; unavailable events are regenerated
   from the ordered input source.

A planned handoff verifies the final Azure durable cursor for every continued active database before transferring the
leader lease and also reports how much of each newer shutdown canonical cut the target already has. A database set transition
additionally verifies each added database can initialize and excludes removed databases from activation. Exact direct bytes may
reduce replay, but transfer does not wait for the canonical cut to become durable and never treats an unselected tail as
recovery truth. Both planned and unplanned takeover adopt each continued state-record boundary, copy validated optimistic
events beyond that end, and consume missing inputs only where cache reuse is unavailable; unpublished additions initialize from scratch, and the planned path is faster because the target and its caches
are already prepared.

A node may serve an already accepted local read while another database is replaying, subject to per-database readiness.
It cannot become cluster leader until every database active in the database set it would select satisfies election or initialization
eligibility and its application generation is not below the current floor.

### Checkpoint publication

An interval or compaction completion can trigger [checkpoint capture](#immutable-manifest). Every result uses the common
publisher's `SelectCheckpoint` intent. Follower delivery remains bootstrap-only: selecting a newer checkpoint never
pushes its KVI into an already running follower.

### Leader transition

Use the [cluster transition engine](#cluster-transition-engine) for all entry paths. Lease transfer changes only how the
target proves ownership. It does not bypass per-database activation, initialization, durable-base restoration, or the
all-active-databases readiness gate. Later checkpoints and full compaction use ordinary leader paths after activation.

### Restart or new replica

1. Read `cluster/leader.json`, validate its selected generation and database names, compare them with the
   node's own application generation/database set, and read state only for relevant database instance IDs.
   Compare application generations and name sets: an older node ignores unknown additions and treats its names omitted
   by the newer generation as removed. A valid newer configuration opens its additions provisionally.
   Equal-generation name-set differences are configuration faults. No durable retirement history is needed.
2. For an active database, a state naming another term/session means the current lease holder is activating or has
   failed. The node may retain or prefetch the previous accepted state, but it does not accept current-term frames or report
   current-term readiness until an adopted state is observed.
3. Validate any existing local directory as one complete recovery generation. Every referenced object's identity,
   length and hash, the KVI, descriptor chain, TRL links, and closed-transaction cursor must match durable state. Never
   combine plausible-looking remnants from different local generations.
4. If validation fails for any reason, quarantine or discard that local generation. Read the selected checkpoint
   manifest and transaction-boundary descriptors, then download all referenced files into a fresh temporary directory.
   Existing local files may be reused only after their exact identity and hash are proven.
5. Verify object hashes, sizes, database identity, BTDB format, descriptor linkage, segment continuity, and transaction
   closure before opening the restored database.
6. Use the common canonical validation/replay path in the temporary generation to the exact selected durable boundary.
   Require no open transaction at end
   and verify canonical sequence, frame-chain hash, TRL cursor, root identity, and event position.
7. Atomically install the fully verified local generation. A crash before installation leaves it unselected; a later
   restart validates whichever generation is present rather than trusting the rename or directory contents.
8. Verify that every file referenced by the installed root remains protected, and initialize ObjectDB and
   application state before new event execution (B3). Step 6 already replayed post-KVI transactions; do not apply them
   again. Later follower-local KVI metadata and useless-file cleanup have no canonical effect.
9. Read the current endpoint and API key from the leader record, connect the resumable leader session, and start
   proposing the corresponding upstream events from each accepted event position. A provisional addition instead keeps
   processing locally; a removed instance is outside cluster recovery and any optional local reopen uses `.temptrl`.
10. Become election-eligible only after every database active in the candidate database set satisfies the canonical lag or
   initialization policy and the node's application generation is at least the selected floor.

If a restore file disappears after a newer checkpoint was selected, restart and discover the newest published KVI.
There is no guaranteed old-manifest fallback. If the current selected closure itself is incomplete or corrupt, report
unavailability; never initialize over published history. Only an unpublished new database follows the empty-initialization
rule. Followers never independently declare their replay canonical.

A missing or corrupt local file discovered while running invalidates the affected local generation. The node stops
reporting that database ready and rebuilds from the selected durable boundary. If the current leader loses any part of
an accepted but not yet object-store-published canonical tail and cannot prove the exact bytes and roots from another
verified source, it self-fences instead of inventing a replacement in the same term; a higher term rewinds to the last
durable boundary and replays retained events. Losing every node's local disk is therefore recoverable under the stated
object-store and event-retention assumptions, while insufficient durable inputs cause explicit unavailability rather
than a corrupted database.

## Read-serving and command consistency

A follower has one current BTree plus a confirmed-position watermark. Replication retains no old confirmed snapshot.
Write transactions start from the current local head; leader distribution does not throttle application execution.

- **Local confirmed read** is available only when a newly opened ordinary read transaction's event position is already
  structurally confirmed. If the head is ahead, route a strict read to the leader or wait for that specific read snapshot
  to become confirmed. Such a request owns an ordinary bounded-lifetime reader; replication does not retain every root.
- **Optimistic local read** reads the current local state and reports its event position and the confirmed watermark.
  If ahead, its contents remain speculative. Structural mismatch closes the session and restarts; returned results may
  be invalidated. No live corrected suffix with accepted drift remains in this design.
- **Provisional/retired local read** is explicitly labeled with `provisionalLocal` or `retiredVolatile`, application
  generation, database instance ID, and local event position. It lets the application use a newly added database while
  an old leader remains active, or a removed database on an old follower, but provides no canonical identity or
  durability claim. It may disappear when the database is closed and reopened from the selected leader.
- **Bounded-lag read** is admitted only while the replica is within a configured canonical-sequence/event/time distance
  from leader progress; otherwise readiness fails or the request waits.
- **Minimum-position read** carries a required event position, often returned after accepting a command, and waits or
  redirects until a read snapshot containing it is structurally confirmed, or redirect to the leader. This can provide read-your-writes after the event is replayed even
  if an unpublished old-term tail was rolled back.
- **Barrier/latest read** requires an event-log barrier or another authoritative high-watermark protocol. Merely
  checking an eventually changing end offset does not by itself make arbitrary reads linearizable with concurrent
  command producers.

All state-changing commands must enter through the ordered event path. A request handler must not update only its
local BTDB and later emit an event, because that mutation is not leader-authored and another replica could observe a
different order. The command API still needs to define whether success means "accepted into the event log,"
"canonicalized by the current leader," "durably published to object storage," or "accepted through position P by a
particular replica." It cannot mean synchronously accepted by every live replica without a separate acknowledgement
protocol.

Once a leader has crossed its shutdown canonical cut, any transaction result it returns is explicitly local and
volatile. It may report the event identity and disposable head for diagnostics, but it cannot claim leader-canonicalized,
object-store-published, or externally acknowledged durability. A client that submitted the underlying event uses the
event-log acknowledgement and later minimum-position read to observe it on the successor.

The same restriction applies to provisional additions and retired-database `.temptrl` execution. Their results are
useful process-local outcomes only and do not establish published canonical history.

Because a small direct-stream tail may be rolled back on leader loss, "canonicalized by the current leader" is weaker
than "durably published." The application-level event token remains replayable, but a client that requires no visible
rollback must wait for the durable boundary or use a stronger external acknowledgement contract.

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
| Partial multi-file upload, descriptor write, or lost state response | Previous or new whole boundary only | Reconcile publisher operation; prepared bytes alone are not accepted. |
| Checkpoint is ahead, stale, or at the same position | Ahead waits; stale is rejected; equal-position verified physical replacement is allowed | Application processing continues; only `SelectCheckpoint` selects the result. |
| Handler throws in a batch / individual retry | Whole failed attempt rolls back; retry its events individually; failed singleton commits cursor-only skip | Discard failed TRL/application effects; structural/outcome disagreement restarts the follower. |
| Missing/duplicate/conflicting canonical data | Gap cannot advance; exact duplicate is idempotent; conflict is a safety fault | Resume from confirmed metadata or restart for canonical rebuild using the common acceptance path. |
| Event-structure match / mismatch | Ignore transaction framing; actual structural or outcome mismatch restarts follower | Matches advance metadata only; no live suffix repair. |
| Long speculative lag or structural mismatch | No speculative state becomes canonical by age or volume | Disk-backed comparison; no historical replication roots. Mismatch restarts and rebuilds. |
| Lost, stale, or inapplicable compaction controls | Application frame processing continues | Keep older valid layout and its local pins. Future open may bootstrap a newer checkpoint. |
| Shutdown at any transaction/compaction boundary | No new canonical work after the per-database cut | Continue `.temptrl`; cancel compaction at a bounded safe point; use authority lane only. |
| Failed target or ambiguous lease transfer | Draining never resumes canonical work | Reconcile proposed lease ownership, try another target only when authority permits, or await expiry. |
| Scratch cleanup failure / scratch I/O failure | No promotion or remote fallback | Startup stays unavailable / affected local transaction fails explicitly. |
| Duplicate/delayed leak-removal request or event | Only the normal ordered event can mutate state | Parent reconciles publication identity; handler ignores absent non-reused exact keys. |
| Event source gap, unavailable source, or expired retention | Never skip required events | Accepted reads and available canonical replay may continue; expose degraded readiness or unrecoverable gap. |
| Missing, corrupt, or mixed local files, including total cache loss | No inferred local truth | Validate one closure or rebuild. Leader losing an unprovable unpublished tail self-fences. |
| Remote object disappears during restore | No incomplete database is served | Restart; reread latest state and use its published KVI. If the current closure itself is broken, report unavailability. |
| Restore/publication races GC | Publish replacement closure before deleting superseded files; no restore pin | Follower restarts on missing old files. B5 verifies staging protection and key non-reuse, not delayed deletion. |

### Availability and progress budgets

Read availability, speculative throughput, canonical progress, and durable progress are separate signals. A lease renewal
alone is not application health, and an event stream with no new events is not a stalled leader. Define activation and
required-work deadlines before relying on automatic recovery (B6).

A conservative failover budget includes detection, remaining lease lifetime, backoff, authority/adoption, and required
restore/replay. Measure warm and cold recovery separately; overlapping work may reduce the observed time. One failed
active database gates cluster leadership. Per-database reads can remain available under their own read policy.
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

Proposed replication policy: reuse timeout, durable skip decision, and process restart, with exact event IDs in the
leader JSON instead of separate marker blobs. Five minutes is the initial configurable handler-timeout proposal.
A watchdog runs independently of the handler and observes a coherent per-database execution token: attempt identity,
current event identity, start time, and batch range. It measures each handler with an injected monotonic clock; idle
input, storage waits, and transaction commit stalls must not be mistaken for one poisonous application event.

On a leader handler timeout:

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
   retries outstanding events individually through the highest pending skip position before resuming normal batching.
   For the named event it does not call the handler: it publishes the ordinary singleton metadata-only cursor commit
   with `outcome=skipped`. The abandoned transaction/batch has no partial canonical effect. Followers replay that TRL.

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

This settles the proposed response to an identified stuck handler, not every availability failure. Exact integration
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
| Durable boundaries | One transaction spans two, three, or many TRLs; crash after every file/descriptor/state operation; incomplete headers/segments/commit; longer unselected physical suffix | I2, I5, I8 |
| Checkpoints | Ahead-of-durable capture; stale capture; equal-position compaction checkpoint; conflicting logical identity; retained predecessor checkpoint after adoption; source pinned throughout upload | I5, I8, I9 |
| Transition recovery | Bootstrap and handoff use identical activation checks; every partial create/adopt combination; matching preexisting genesis versus unrelated state; one instance cannot activate | I1, I6, I7 |
| Upgrade | Independent disposable provisional copies; highest prepared generation priority; crash before/after selection; every provisional copy reopens from canonical initialization; removed names are never reused; floor blocks old nodes | I7, I11 |
| Retirement | Delayed predecessor CAS may land in abandoned namespace; no freeze or retirement CAS; old nodes continue independently from local views; removed names never reused; active-database readiness unaffected | I7, I11 |
| Graceful drain | Shutdown inside application transaction, compaction chunk, upload, and ambiguous state CAS; per-database cuts differ; no target; target failure before/after transfer; ambiguous Change Lease | I1, I5, I11 |
| Scratch | Values above inline limit; later reads of old/new values; disjoint IDs; disk full/lost; interrupted deletion; startup cleanup failure; malformed names and links cannot escape scratch root | I8, I9, I11 |
| Application batches | Independent leader/follower batches; per-event markers agree; no comparison of uncommitted markers; cut inside committed local batch; rollback removes markers; no historical roots | I2, I3, I4 |
| Structural comparison/restart | Leader 1-3 versus independently batched local 1-5; batching/framing/compression/physical differences match; real mutations and skip outcomes mismatch; no pinned historical roots; restart discards divergent cache | I3, I4, I9 |
| Optimistic tail adoption | Canonical 102 versus local batch 101-105 copies only 103-105; overlap mismatch rejects; end advances during reconciliation; metadata delta baseline changes; copy/publish crash at every point; no duplicate events or handler reruns | I2, I3, I5, I8 |
| Invalid comparison cache | Missing/corrupt bytes reject confirmation and trigger restart/rebuild when unrecoverable | I3, I8, I9 |
| Failed consumption | Throw first/middle/last after writes and file rotation; rollback all batch effects; retry all events individually; multiple failed singletons; metadata-only skip; reset ObjectDB counters; operational failure is not skipped | I2, I3, I5, I8 |
| Skip replay | Leader skip versus follower success and reverse; same-outcome exact match; crash before/after skip commit and publication; durable skip never invokes handler; unpublished skip may be re-decided after takeover | I2, I3, I5, I8 |
| Follower acceptance | Exact match does no BTree work/copy; input-lag direct replay; duplicate/gap/conflict; byte-only mismatch; logical mismatch; equivalent transactions on different valid physical layouts | I2, I3 |
| Restart recovery | Structural mismatch first/middle/last; fence racing a commit; host termination; canonical rebuild ignores divergent cache; fresh ObjectDB counters/caches; restart-loop diagnostics | I3, I4, I9 |
| Speculation resources | Extended disconnection and slow delivery; bounded comparison RAM and pending TRL disk growth; bounded streaming comparison; explicit storage failure without remote admission dependencies | I4, I9 |
| Leak events | Every replica detects only accepted roots; malformed/hash-invalid/duplicate/over-budget candidate; bounded batching; delayed, overlapping and ambiguous parent publication; normal event replay including cross-file transaction | I2, I3, I10 |
| Physical compaction | PVL prerequisite missing/corrupt; duplicate/stale/out-of-order chunks; per-chunk base with interleaved events; partial/full completion; follower retains older layout without application gap | I3, I9, I10 |
| KVI and local cleanup | Exactly one bootstrap KVI per open; no later live KVI; older/newer local startup layouts; cleanup-only stop before PVL/rewrite; follower KVI disjoint from canonical allocation | I8, I9, I10 |
| Reader/file lifetime | Long-lived accepted and optimistic readers; ordinary user-reader/current-tree/comparison-file/recovery lifetimes; allocation after missed compaction; no distributed deletion decisions | I9, I10 |
| Cache loss | Delete/truncate/replace/mix every local file type before and during open and while running; crash around generation installation; all local disks lost; unprovable leader tail loss | I1, I8, I9 |
| Remote GC | Delayed old-term delete; attempted reuse of retired key; unselected initialization upload later published; current published recovery closure; immediate delete during follower download causes restart onto newer KVI; no grace period | I5, I7, I9 |
| Transport | Independent leader links fail while others work; reconnect storms; old status/connection IDs; rotated credentials; unauthorized request; secret logging; bounded queues and unavailable retained ranges | I1, I3, I12 |
| Input/read contract | Duplicate/gap/expired input; source unavailable with canonical bytes available; read token across term rollback; speculative/provisional/volatile reads never labeled durable | I2, I3, I8, I11 |
| Liveness | Activation stall; publisher/event worker stall with healthy renewal; idle input; failed leader endpoint; warm/cold takeover and generation-floor loss | I1, I4, I7 |

Canonical logical equality must be checked against an independent logical-state oracle, not only a matching frame-chain
hash. Compare restored values as well as positions. Assert physical file resolution for every retained reader. Intercept
ports to fail tests on follower remote deletion, speculative publication, post-cut data persistence, or any live KVI/delete
message prohibited by I10/I11.

Every port has reusable semantic conformance tests. Run transport cases against both the in-process adapter and loopback
Kestrel; add HTTP fragmentation, proxy buffering, flush/idle timeouts, range reads, authentication, and slow consumers.
Run storage cases against real Azure in addition to Azurite: conditional append, unchanged selected prefixes, multi-file
preparation, CAS ambiguity, and acquire/renew/change/release. The four-step CAS probe is a prerequisite, not proof of
multi-node correctness. S3 qualification belongs to its later adapter.

Measure standalone BTDB before and after core changes: reads/writes, inline and large values, range erases, commits,
allocation, throughput, and startup replay. No measurable regression beyond benchmark noise is acceptable. Separately
measure replication overhead, comparison memory/disk growth and restart recovery work, cold restore, and event catch-up.
Follower transport delays must not introduce commit waits for external progress. Performance results cannot excuse a
safety failure or make an unspecified normalization or disk-buffering mechanism implicit.

## Open decisions

### Implementation blockers

These are unresolved mechanisms, not optional optimizations. The corresponding invariants and intended capabilities
remain required. Consolidation does not claim to have solved them.

| ID | Decision needed before the affected mechanism is implemented | Required evidence |
| --- | --- | --- |
| B1 Authority freshness | Specify leader/follower authority observations, clock and request-delay assumptions, renewal margins, in-flight commit fencing, and invalidation during immediate lease transfer. A GET does not establish a fresh lease lifetime. | Old-term acceptance excluded under pauses, delayed reads/frames, and handoff; real-provider conformance. |
| B3 Structural comparison and transaction rollback | Define immediate per-event Ulong emission and metadata baselines, event-delimited comparison, committed-tail extraction across local batches, idempotent canonical append/reopen, and failed private-attempt rollback. No live follower rebase. | Different supported batching/physical layouts compare equal; real changes do not. Comparison uses bounded RAM with no historical roots. Failed handler attempts reset counters/metadata before retry or skip; restart reopens coherent ObjectDB state. |
| B5 Publication/deletion ordering | Implement publish-before-delete, staging protection, and permanent non-reuse of retired object keys. Superseded files are deleted immediately; follower restores do not pin them. | Selected KVI and tail remain complete; delayed old deletes cannot remove newly selected data; a follower losing old restore files restarts onto the latest KVI. |
| B6 Availability envelope | Handler timeout uses the proposed persisted exact-event skip and host restart policy above. Define other activation/stalled-work deadlines, bounded host termination, safe abdication with a live renewal loop, election readiness, and regional storage recovery boundaries. | Warm/cold failover budgets; idle streams stay healthy; failed workers cannot hold authority indefinitely; regional recovery does not assume compute-election guarantees. |

The selected checkpoint rules (durable ancestry and equal-position physical replacement) and idempotent activation cases
are now normative. They require tests but are no longer alternative algorithms to choose between.

### Integration and policy choices

| ID | Remaining choices within the selected design |
| --- | --- |
| Q1 Input integration | Application exception classification and skip diagnostics; exact-event timeout reporting and skip-list integration; batch partition independence; optional cross-framing mutation diagnostics; event-range identity/hash encoding; event cursor/stream-generation encoding; one cluster-wide ordered stream; independent full-stream delivery and explicit seek; library-owned consumption versus application callback. |
| Q2 Core API and codecs | Opt-in capture/pin/replay/export APIs; canonical position persistence separate from reused BTDB TransactionId; exact transaction and logical-change encoding; hashes and redacted diagnostics; independent protocol/leader-record/database-state/checkpoint format versions. |
| Q3 Application/read semantics | Default command acknowledgement; side-effect contract for retry and abandoned speculation; minimum-position/barrier reads; divergence readiness policy; compatibility of event execution separately from storage readability. |
| Q4 Operational budgets | Independent per-replica event-batch count/bytes/time and independent remote publication-batch bytes/time; checkpoint cadence; local hard-flush policies; root/history and event-retention budgets; warm/cold RTO; handoff lag/grace and long-transaction limits; watchdog thresholds and metric alert levels. |
| Q5 Transport | Shared binary codec; frame/artifact bounds; heartbeat/status/flush cadence; connection replacement, retry/resume/backpressure and range retention; authorization beyond the shared key. |
| Q6 Maintenance | Bounded compaction codec/chunk preconditions and canonical allocation watermark; local cleanup inventory and cache-KVI cut; checkpoint scheduling after compaction; leak detector compatibility, trust/reachability validation, exact-key budgets, batching, and deduplication. |
| Q7 Recovery and storage | Descriptor indexing/truncation without losing ancestry; latest-checkpoint restore and complete event coverage; offline backup restore with new stream/cursor binding and leader-record reset; runtime local-integrity cadence; restart on removed restore files without remote pins; reuse of transfer code without legacy unconditional writes. |
| Q8 Deployment and lifecycle | Database-name/instance syntax; generation allocation/conflict checks; eligible rollout redundancy; starting event-cursor policy and required input retention; abandoned namespace retention/administration; exact scratch namespace/quota/cleanup and explicit local commit result. |

Provider mechanics and future S3 alternatives stay in ObjectStorages.md. Version-one append selection, validated optimistic tail reuse with input-recovery fallback, event-end Ulong markers, bootstrap-only KVI transfer, and node-local deletion decisions
are settled constraints, not recurring open questions.

## Design history and alternatives

Decisions incorporated into the normative sections:

| Date | Adopted direction |
| --- | --- |
| 2026-08-30 | Azure-first cluster leadership, transaction-aligned publication, injected adapters, and leader-centered sessions |
| 2026-08-31 | Initial generation-fenced transition proposal; selected seeds and frozen retirement requirements superseded below |
| 2026-09-07 | Inline `databaseNames` in the leader JSON; initialization metadata stays per database |
| 2026-09-07 | Activate with an empty transaction and starting event cursor; unpublished databases may restart from scratch without preserving provisional seeds |
| 2026-09-07 | Removed names are never reused; old nodes continue independently until shutdown. Exact retirement freeze and blocker B2 removed |
| 2026-09-01 | Ordinary event-only TRL, parent-published leak removal, and out-of-band physical compaction |
| 2026-09-06 | Leader-only batching and metadata-only skips; the initial retained-root/suffix-repair proposal is superseded below. |
| 2026-09-07 | Batching-independent structural TRL comparison; follower mismatch restarts/rebuilds. No replication-owned old BTree roots or live suffix repair; B4 removed and B3 narrowed. |
| 2026-09-07 | Emit event-end Ulong updates inside TRL; all replicas batch independently. Takeover appends only validated optimistic events beyond the adopted canonical end, without duplicate consumption. |
| 2026-09-07 | Delete superseded remote files immediately after publishing the replacement KVI; interrupted follower restores restart onto the newest checkpoint. No restore leases or grace period. |
| 2026-09-06 | Shared identities, one database publisher, one transition engine, and one follower acceptance path; checkpoint selection constrained to durable ancestry and allowed to replace physical layout at equal position |

The following alternatives explain the storage decision. Version one remains conditional atomic TRL tail append with
sealed/pinned immutable checkpoint closures. Azure mechanics and later S3 research remain in ObjectStorages.md.

## Checkpoint-file storage alternatives

The event log and canonical TRL contain the same ordered application transitions, including a parent-published
leak-removal event. Out-of-band compaction controls are deliberately absent from both. A valid KVI seals one physical
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

This remains the leading checkpoint file-set format and can be used alongside the conditionally appended active TRL.

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
This per-file operation is not the BTDB durable commit point. The database state record separately selects an immutable
transaction-boundary descriptor, so physically present but unpublished suffixes or newly rotated files are not durable
canonical progress. When one transaction spans files, every file is prepared first and one state CAS selects all of its
segments together.

The failover state machine depends only on those semantics. Provider object layout, staging calls, identifiers, and SDK
choices are intentionally outside this architecture and are recorded only in [ObjectStorages.md](ObjectStorages.md).

Advantages:

- the protocol expresses the required atomic append without coupling recovery to an Azure blob type;
- the previous token and length make a lost race detectable;
- an older accepted boundary remains valid even if a later append became visible before its state-record CAS;
- range reads can replay only file cuts selected by the transaction-aligned database state.

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

### Candidate revision: identical event TRL and independent local compaction

Discussion on 2026-09-08; this is a candidate replacement for the preceding batching and distributed-compaction design,
not a claim that the existing APIs already implement it. Separate event-log transaction boundaries from publication of
an in-memory BTree batch. Each event produces the same complete log transaction on every replica given identical
application execution, while each node independently decides when to publish its working root to local readers.
Deterministic TRL is a property to validate; nondeterministic handler results still cause divergence and restart.

Current code ties the operations together: `CommitWritingTransaction()` writes metadata/commit bytes, flushes, and swaps
`_lastCommitted`. Its metadata deltas are relative to that published root. A split implementation needs a separate
last-logged-event metadata baseline. `Compactor.RunCore()` currently creates a flushing transaction and requests a
`TemporaryEndOfFile`; the temporary close appends bytes and hard-flushes but does not itself rotate the log. Replace this compactor
operation with a byte-preserving hard flush; canonical log rotation must remain independent of local compaction.
TRL headers, rotation thresholds, codec configuration and logical file identities must likewise be independent of local
PVL/KVI allocation. Identical event-record bytes do not imply identical directory contents after local compaction.

There is a failure-semantics decision: once an event's complete log transaction is accepted as committed, a later
failure in the same memory batch cannot erase that earlier event. If the working batch root must be discarded, rebuild
its completed events from their log without rerunning handlers, then handle the failed event. Either this replaces
whole-application-batch rollback, or log publication must remain provisional until the entire memory batch succeeds.
The latter needs an explicit acceptance boundary; a commit-shaped provisional byte sequence is not durable history.
Do not silently claim both independent per-event log commits and atomic rollback of already accepted earlier events.

Independent local compaction produces private PVLs and KVIs, preserving logical state and the event log. Running nodes
need no pointer-rewrite/PVL distribution. Local file cleanup continues to respect current roots, user readers, and any
unpublished log ranges needed for comparison or takeover.

Remote compaction can use one of two producers with the same checkpoint-publication contract:

- Export an existing node's locally compacted checkpoint, preferably the current leader at a canonical event boundary.
  Serialize a pinned current root into KVI, record its full value-file closure and log cursor, pin those files, then
  release the root after serialization. Upload every referenced file or exact immutable prefix not already available.
  This does not need an independent full in-memory BTree. Temporary copy-on-write retention during KVI serialization
  still consumes memory; pinning files for upload must not unnecessarily retain the root for network duration.
- A newly starting replica, or a dedicated job, opens the currently published canonical checkpoint and log through a
  fixed durable boundary C, compacts its own copy, and emits a candidate KVI/PVL closure. It does not execute application
  events or become event leader. A starting replica can use its one BTree for this work and subsequently catch up/join;
  a dedicated process moves the extra BTree's memory out of an application node, but still needs that memory somewhere.

A locally compacted BTree cannot describe the original remote physical reachability graph, but it can describe a
complete replacement snapshot. Existing `CreateKeyIndexFile()` serializes actual file IDs/value offsets and enumerates
used files; `CreateIndexFile()` temporarily pins one existing root. An export API would reuse those capabilities while
keeping the captured file closure alive. This is not permission to upload just a KVI whose referenced local PVLs are
missing remotely. Snapshot-scoped file-ID mapping and the connection to canonical future TRL must be explicit so local
compaction IDs cannot collide with later log/PVL IDs. Copying KVI bytes alone does not solve that integration.

Whichever producer is used, only the current leader publishes the replacement through its serialized database-state
publisher. The candidate supplies its canonical event boundary, ancestry identity, KVI, full file closure and canonical
log cursor. Boundary C must be on the currently selected durable history and no later than its durable end D. Keep the
canonical log from C to D and onward; selecting a new physical checkpoint must never move durable progress backward.
Reject a stale or unrelated candidate and preserve current publisher fields on CAS retry. Upload the complete closure
before selection; delete superseded objects immediately afterward using the agreed no-restore-grace policy. A job or
starting replica whose input files disappear restarts from the newest published KVI, just like another restoring node.

The architectural simplification to evaluate first is exporting a complete locally compacted leader snapshot. A job is
useful when isolating compaction CPU/memory is worth another deployment path, or when no existing node can supply a
suitable canonical snapshot. Neither producer requires maintaining a permanent second BTree in every application node.

### Verified compactor flush simplification

Source and isolated experiment on 2026-09-08: the compactor's call to
`NextCommitTemporaryCloseTransactionLog()` currently forces a writable transaction, emits an empty commit plus
`TemporaryEndOfFile`, and hard-flushes the active file. It does not directly rotate or permanently close the TRL;
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
compactor operation; the production code was not modified by the experiment. These tests support the replacement,
not an exhaustive crash-consistency proof for every concurrent checkpoint/adapter scenario.

Retain the public temporary-close API and `TemporaryEndOfFile` decoder for compatibility. The loader uses this marker
to recognize a safely appendable tail when scanning a file, and `Dispose()` also emits it. Removing one compactor call
is different from removing the format command globally. A KVI whose saved cursor is exactly at a file's EOF is already
accepted for continuation by `LoadTransactionLog()` without requiring that marker.

## Provider-specific object-storage behavior

Azure Blob and Amazon S3 conditional writes, native Azure leases, throughput concerns, and the concrete Azure
realization of conditional tail append are maintained in [ObjectStorages.md](ObjectStorages.md). The failover and replay
protocols depend on the semantic append contract, not its physical block layout.
