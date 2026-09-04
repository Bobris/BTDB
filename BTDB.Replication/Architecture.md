# BTDB.Replication Architecture

Status: Brainstorming; no implementation has started.

Provider research snapshot: 2026-08-29. Architecture decisions are current through 2026-09-01. The provider contract,
`denoland/celld` study, and Azure/S3 details are maintained in [ObjectStorages.md](ObjectStorages.md).

## Problem statement

`BTDB.Replication` should allow one or more short-named logical `BTreeKeyValueDB` instances to run on one to many compute
nodes while preserving one canonical transition sequence per database under one cluster-wide leader.

There are two related but distinct authorities:

- The durable upstream event log is the authority for ordered **application inputs** and is the recovery source for
  recent application transactions.
- The current leader's transaction log for each database is the authority for that database's exact **logical,
  event-driven BTDB transition sequence**. Every canonical TRL transaction is the ordinary result of consuming one
  upstream application event; replication adds no maintenance transaction kinds to BTDB. Full compaction changes only
  physical layout through a separate live control protocol.

Only the lease- and CAS-confirmed cluster leader authors and appends each real, canonical `.trl`. A follower nevertheless
executes and synchronously commits the same ordered application transactions at local speed. It appends their exact BTDB
transaction bytes to a non-authoritative local speculative cache and advances a speculative BTree head without waiting
for leader frames, object storage, or any network acknowledgement.

The follower retains a pinned read-only root at the last transaction boundary confirmed against the leader. It also
retains an ordered speculative index containing each later event identity, local TRL byte range, change digest, and
resulting root boundary. When leader frames arrive:

1. compare each application transaction with the oldest unconfirmed local transaction at the same event boundary;
2. if the complete transaction bytes and required physical coordinates are identical, advance the confirmed anchor to
   that retained root; the BTree and local bytes already match, so no replay or copy occurs;
3. on the first difference, briefly serialize with local writing, restore the pinned confirmed root, abandon the
   speculative suffix, and replay the leader-authored transaction bytes;
4. re-execute the later ordered application events from that corrected head to rebuild the speculative suffix.

A follower may therefore contain follower-produced bytes beyond its confirmed anchor. They are only a local cache and
must never be published as canonical based on their origin. A byte-identical transaction becomes canonical locally when
matched to the leader frame; otherwise leader bytes replace it through rollback and replay.

At the boundary after canonical transaction sequence `S` for database `D`, every ready node that reports `(D, S)` must
expose the same logical BTree state. Application transactions normally also produce identical canonical file
references. Full compaction is an out-of-band physical optimization distributed over the leader session, so a follower
that applied it and one that retained the older layout may have different physical references at the same canonical
sequence while resolving every logical value to identical bytes. There is no compaction transaction to replay or skip.
Nodes can temporarily be at different sequence numbers, but two nodes must not claim the same accepted sequence with
different logical contents.

One node is elected as the cluster-wide leader through one shared Azure leader blob. The same node leads every BTDB
database active in the leader-record-selected catalog. It:

- authors every database's canonical TRL and assigns its canonical transaction sequence numbers;
- directly streams committed TRL frames to followers;
- publishes recoverable transaction-aligned remote TRL boundaries and complete checkpoints;
- is the only node allowed to initiate and distribute full pointer-rewriting compaction or delete a file from object
  storage. Leak removal reaches canonical history only as an ordinary event published by the parent system.

The local disk on each node is a working copy and cache. Losing a small unpublished leader tail is acceptable: a new
term may return to the last object-store-published canonical boundary and deterministically replay the missing
application events. This explicitly permits a small rollback across leader failure; it must never permit two live
canonical histories.

A leader entering graceful shutdown does not stop application transaction execution. At the next safe transaction
boundary it irreversibly closes persistent canonical writing and switches subsequent application transactions to an
ephemeral local generation. They continue to update the disposable process-local BTree head, while newly written values
and transaction bytes go to one or more local `.temptrl` scratch files. Those files are never canonical, hard-flushed,
uploaded, or included in a checkpoint; they are deleted on clean exit and unconditionally removed before database recovery
after restart. The successor starts from the final object-store-selected boundary and replays later ordered events.

Local storage is never a source of durable truth. A restart may find any subset of local files missing, truncated, or
left at different moments. The node must either validate a complete local cut against an object-store-selected recovery
identity or discard/quarantine it and rebuild from object storage plus the retained event log. If no complete verified
recovery closure exists, the node stays unavailable; it never repairs by guessing from filenames, lengths, or the newest
locally visible bytes.

The baseline is therefore:

- every application event has one total order and is executed against the preceding local event root, which may be
  confirmed or speculative but is always ultimately anchored to a leader-confirmed prefix;
- only a validated leader frame can advance accepted state; follower-produced bytes count only after exact
  transaction-level equality with that frame has been proven;
- a separate monotonic canonical transaction sequence per database orders only ordinary event-consumption transactions;
- replication introduces no BTDB transaction kind: full-compaction controls stay outside TRL, while leak removal is an
  application event;
- the event mutation and its event position are encoded in the same canonical application transaction;
- every object-store-published TRL cursor is immediately after a complete BTDB transaction; one publication can describe
  multiple physical TRL segments when that transaction crosses file boundaries;
- follower write commits perform only bounded local synchronous work and never wait for leader distribution, object
  storage, or follower acknowledgements;
- graceful leader shutdown does not gate application transactions: after one bounded transaction-boundary switch, the
  old leader applies them only to disposable local `.temptrl` scratch files and performs no canonical local or remote
  persistence;
- every follower distinguishes its rollbackable speculative head from its leader-confirmed anchor;
- a monotonic leadership term and conservative lease validation fence delayed leaders;
- every follower maintains an authenticated transport session to the leader; losing that session triggers leader-record
  refresh and possible election contention, but never grants authority;
- one leased and CAS-protected Azure leader record selects the cluster-wide leader and publishes its opaque peer
  endpoint and current API key;
- each short-named database keeps its own canonical TRL/checkpoint progress under that shared leadership term;
- all local database files are disposable validated caches; missing or inconsistent local content causes rebuild or a
  fail-closed state, never partial recovery;
- restart restores a canonical checkpoint/TRL boundary and replays retained application events as necessary.

The library should support both of these Kubernetes shapes:

- A `Deployment` with `1..N` disposable replicas. Every process has a unique session identity, and local storage may
  be ephemeral.
- A `StatefulSet` with stable volumes as a warm-cache optimization. Pod ordinals, stable hostnames, and PVCs must not
  be treated as ownership or fencing tokens.

A single replica must use the same checkpoint protocol as a larger fleet. Increasing the replica count should add
failover and read capacity without changing the stored database format.

## Initial cluster and database model

Decision recorded 2026-08-30:

- A **failover cluster** is one group of nodes sharing one Azure container, leader record, leadership term, and
  leader-centered follower sessions.
- One process may host multiple `BTreeKeyValueDB` instances. Each database has a short, path-safe name unique inside
  the cluster, such as `main`, `users`, or `jobs`; the exact length and character validation remains to be fixed.
- Leader election is cluster-wide, not per database. The elected node is the only canonical TRL author and the only node
  that runs full pointer-rewriting compaction for every database active in its selected catalog. Followers may run local
  cleanup-only compaction that cannot change a BTree root or remote state.
- Canonical sequence, event position, TRL lineage/offset, KVI, checkpoint, and replay state remain independent for each
  database. A slow database does not share transaction ordering with another database.
- Every application build declares a monotonic application generation and an immutable database catalog. Nodes may
  temporarily host different database sets during a rolling upgrade. The catalog selected by the leader record defines
  which database instances are canonical; a newer prepared follower has handoff priority over same-generation nodes.
- Once a newer application generation has been selected by a successful leader-record CAS, that generation is a
  persistent election floor. Older nodes may keep following compatible database instances, but they can never become
  cluster leader again.

The shared leader record contains only cluster authority and connection information. Per-database durable progress is
stored separately so adding databases does not enlarge or create update contention on the election record.

## Rolling application upgrades and database-catalog transitions

Decision recorded 2026-08-31: a rolling application upgrade may add and remove logical databases. Different application
generations are allowed to overlap, but there is still exactly one cluster-wide leader and one leader-selected canonical
database catalog. A database-set difference is an intentional catalog transition, not a reason to reject the newer
follower or to wait for every old replica to disappear.

### Versioned catalog and downgrade fence

Human-readable package versions are not ordered by the protocol. Every deployable application configuration supplies a
strictly increasing `applicationGeneration` and an immutable catalog with semantics equivalent to:

```text
ApplicationDatabaseCatalog
    format
    clusterId
    applicationGeneration
    previousCatalogHash?
    activeDatabases[] = {
        databaseName
        databaseInstanceId
        eventStreamId
        replicationCompatibilityFingerprint
    }
    retiredDatabaseInstances[] = { databaseName, databaseInstanceId }
    catalogHash
```

The catalog is uploaded under an immutable hash-qualified key. The leader record carries only its generation, key, and
hash, so the database list does not grow the mutable authority object. `databaseInstanceId` prevents removal followed by
reuse of the same short name from accidentally reopening the old history. Active names are unique. Re-adding a name
requires a fresh instance ID and is an addition with a new genesis, while the old instance remains retired.

Two catalogs with the same generation but different hashes are a configuration safety fault. A lower generation can
never replace the generation already selected by the leader record. A higher generation may be selected only by a node
that advertises that exact immutable catalog, supports the current wire protocol, and can execute the transition from
the selected predecessor catalog. Increasing the application generation is valid even when the database list is
unchanged and still triggers a planned upgrade handoff.

The leader-record CAS that publishes term `T + 1` also selects the new catalog and an immutable catalog-transition
descriptor. This is the single authority point for the upgrade. The descriptor names the preceding catalog and, for
every added database instance, the exact provisional seed manifest chosen from the handoff target. It also lists
continued and retired instance IDs. Per-database state remains separate, but every activation and recovery can derive
the one fixed transition plan from the leader record instead of guessing from currently connected nodes.

```text
CatalogTransitionDescriptor
    format
    clusterId
    fromApplicationGeneration / fromCatalogHash
    toApplicationGeneration / toCatalogHash
    continuedDatabaseInstanceIds[]
    addedDatabases[] = {
        databaseName / databaseInstanceId / eventStreamId
        seedManifestKey / seedManifestHash
        seedEventPosition / seedLogicalStateIdentity
        genesisFrameChainIdentity
    }
    retiredDatabaseInstances[] = {
        databaseName / databaseInstanceId
        frozenStateRevision / frozenCanonicalSequence / frozenFrameChainHash / frozenEventPosition
    }
    preparedByNodeSessionId
    descriptorHash
```

The exact frozen state revision is captured only after the old leader has stopped persistent writes and reconciled every
previously dispatched state CAS. This prevents the transition from naming a retirement cut that an earlier in-flight
publication can still advance.

After that CAS succeeds, `applicationGeneration` is the cluster's election floor. A node below the floor is permanently
ineligible for leadership, even if every newer node is unavailable. This deliberately trades availability for preventing
an old binary from resurrecting a retired database or leading without a newly added one. The floor never depends on an
ephemeral follower registry.

### Added database while the leader is older

A newer follower whose catalog contains a database instance absent from the leader-selected catalog opens that instance
in **provisional local mode**:

1. It consumes the database's ordered application input and uses an ordinary local BTDB file generation, so the
   application can use the database and the state can be rebuilt from retained events.
2. Its commits are local and non-canonical. They receive no cluster canonical sequence, produce no canonical frame,
   advance no confirmed or object-store-durable position, and cannot be uploaded through a database-state record.
3. The node reports the provisional event position, logical identity, local readiness, and whether it can capture an
   immutable seed checkpoint. Multiple newer followers may therefore hold different provisional copies; there is no
   merge or majority selection between them.
4. To prepare for handoff, the selected newer follower pins one closed local transaction boundary for each added
   database and uploads a complete immutable seed checkpoint and manifest under target/session-qualified staging keys.
   Immutable staging needs no canonical authority, but it has no state effect and is garbage-collectable if the handoff
   does not commit.
5. The target reports the exact seed manifest hash, event-stream identity, event position, and logical-state identity in
   its `prepared` response. The old leader verifies that the immutable closure exists and includes those identities, but
   it never opens or interprets the new database format.

After authority transfer, the target's successful term-and-leader-record CAS selects those exact seed manifests in the
catalog-transition descriptor. That CAS, not the earlier local commit or upload, chooses which provisional version will
become canonical. The selected target conditionally creates each new database state from the named seed, with canonical
sequence zero and a genesis frame-chain identity derived from the catalog, instance ID, and seed manifest. The seed is
a checkpoint base rather than a BTDB transaction; subsequent event-consumption transactions start the ordinary
contiguous canonical sequence.

The target may have executed more provisional events after capturing the seed. Before authoring the first canonical
transaction it closes that provisional head, reopens the exact selected seed as sequence zero, and replays every later
ordered event as ordinary leader-authored application transactions. Thus even the chosen node promotes only its pinned
seed boundary, never an unframed provisional suffix.

If the target crashes before the new leader-record CAS, no catalog transition occurred and a later candidate may prepare
its own provisional seed. If it crashes after that CAS but before all new database-state records are created, the
selected transition descriptor fixes the seed permanently; a successor of at least that application generation must
finish activation from the same manifests and may not substitute its own provisional copy.

Initial cluster bootstrap is the same state machine with an empty predecessor catalog. The first lease winner selects
its catalog and either names closed local seed manifests or empty sequence-zero seeds followed by replay from each event
stream origin; followers never bootstrap competing canonical histories.

### Removed database under a newer leader

A database instance present in the previous catalog but absent from the newly selected active set is **retired**. Its
last database-state-selected boundary remains a frozen canonical recovery point. The new leader does not adopt that
state into its term, author frames for it, compact it, checkpoint it, or delete its retained remote closure. Eventual
retired-data deletion is an explicit retention/administrative policy, not an automatic consequence of rollout.

An older follower may still run application code that expects the retired database. After validating the newer leader
record and catalog, it performs an irreversible per-database switch at the next complete local transaction boundary:

1. stop accepting canonical frames or confirmation progress for that database and pin its last accepted root;
2. redirect every later transaction and newly written value to a session-qualified local `.temptrl` generation;
3. continue local application execution and reads without assigning canonical sequence, publishing, checkpointing,
   hard-flushing, or acknowledging canonical/durable progress;
4. delete the scratch generation on clean close and unconditionally delete leftovers before reconstructing the frozen
   base after restart. A cleanup failure keeps that local database unavailable.

The retired database's `.temptrl` is never a promotion candidate. An old node that restarts can reconstruct the frozen
canonical base from its retained object-store closure and re-execute later retained events only into a new disposable
generation. Because its application generation is below the leader-record floor, it cannot regain leadership and make
those local changes canonical.

### Reopening followers from the selected new leader

The chosen new leader is the only node whose provisional added-database state is used. Every other follower of the new
application generation must close its corresponding provisional database, discard or quarantine its entire local
generation, restore the exact leader-selected seed/checkpoint and canonical tail, and reopen from that state before
reporting ready. Byte equality or a matching logical digest is not a fast path for this first canonicalization: no
transaction from an unselected provisional history may be promoted or merged. Later transactions use the normal exact
canonical-frame comparison protocol.

An older follower that lacks a newly added database simply ignores that database's stream and remains non-ready for the
full new catalog; it may still follow and serve compatible continued databases. For a database it still hosts but the
new catalog retired, it uses the disposable behavior above. For continued databases, a former old leader discards any
temporary handoff suffix, closes and reopens against the new leader, and then behaves as an ordinary follower.

### Upgrade handoff readiness and selection

An old leader treats a connected higher-generation follower as an upgrade target when:

- its protocol range and every continued database compatibility fingerprint are accepted;
- every continued database is within the configured handoff lag and the target retains the required replay inputs;
- every database added by the target catalog is provisionally ready and can supply a closed seed manifest;
- databases removed by the target catalog are explicitly accounted for in the transition, not accidentally missing;
- its candidate endpoint and proposed lease-transfer ID are usable.

The target does not need to host databases its own catalog retires. As soon as such a follower is prepared, the leader
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
- **Database instance ID**: the stable identity of one lifetime of a database name. Removing and later re-adding the
  same name requires a new instance ID and cannot attach the new application to the retired history by accident.
- **Application generation**: an application-supplied strictly increasing rollout generation, independent of a display
  version. The generation selected by the leader record is a persistent lower bound for future leaders.
- **Database catalog**: an immutable, hash-identified declaration of active and retired database instances for one
  application generation. The leader-record-selected catalog defines the canonical database set.
- **Provisional local database**: a database newly introduced by a follower's higher application generation while an
  older catalog still leads. It runs against ordinary local files and ordered inputs but has no canonical sequence,
  frames, confirmed progress, or remote state. Only the prepared handoff target may seed canonical genesis from it.
- **Retired database**: an instance removed from the active leader-selected catalog. Its durable canonical boundary is
  frozen; older applications may continue using it only through disposable local `.temptrl` writes.
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
- **Event position**: a totally ordered cursor identifying the last upstream application event included in an
  accepted application transaction.
- **Canonical transaction sequence**: a contiguous number assigned by the leader to every accepted event-consumption
  BTDB transaction. It is distinct from event position even though both advance together, because an event position may
  have a stream-specific representation rather than one integer.
- **Accepted root / confirmed anchor**: the immutable BTree root at the latest contiguous transaction boundary verified
  against leader frames. A follower keeps this root pinned, conceptually through an open read-only transaction, so it
  remains available for rollback while the speculative head advances.
- **Speculative head**: the follower's latest locally committed BTree root. It includes a contiguous suffix of ordered
  application transactions that may not yet have been received from the leader and can be rolled back.
- **Speculation generation**: a follower-local branch identity incremented whenever rollback abandons a suffix. Local
  entries use `(nodeSession, speculationGeneration, ordinal)` rather than treating a reused BTDB `TransactionId` as a
  globally unique identity.
- **Speculative transaction entry**: one follower-local application transaction after the confirmed anchor, including
  its speculation generation, event identity, exact local TRL bytes and coordinates, logical digest, and a retained
  resulting-root boundary.
- **Local speculative TRL cache**: follower-produced transaction bytes used for zero-copy confirmation, comparison, and
  rollback/re-execution. It is neither canonical nor eligible for object-store publication until matched transaction by
  transaction to leader frames.
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
- **Volatile catalog-transition generation**: a disposable `.temptrl` generation used either by a stepping-down old
  leader while an application upgrade transfers authority or by an old follower for a database retired by the selected
  catalog. It obeys the same no-flush, no-upload, no-promotion rules as a volatile shutdown generation. A continued
  database may leave this mode only by discarding the complete volatile generation and reopening from the new leader;
  a retired database remains volatile for the rest of that local open lifetime.
- **Application transaction**: a canonical transaction corresponding to one upstream event and comparable with the
  follower's locally proposed mutations.
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

The first design should make these claims precise enough to test under injected crashes, pauses, lost responses, and
network partitions:

1. A replica applies an event only after the preceding event position and never silently skips a gap.
2. One canonical transaction sequence per database orders ordinary event-consumption transactions without gaps or
   duplicates. Replication-specific physical controls never consume it.
3. A follower may advance an arbitrarily long local speculative suffix without waiting for leader frames. It reports
   canonical progress only through the separately pinned confirmed anchor.
4. At equal `(databaseName, databaseInstanceId, databaseCatalogHash, leadershipTerm, canonicalSequence,
   frameChainHash)`, ready replicas expose identical logical BTree state and every value reference resolves to identical
   bytes. Physical references may differ because out-of-band compaction was applied to one local layout and not another.
5. An application mismatch restores the last confirmed root, discards the entire dependent speculative suffix, replays
   the leader transaction, and re-executes later ordered events; it never merges two histories.
6. Every canonical TRL transaction corresponds to one ordered application event. A leak-removal event uses the same
   transaction, comparison, replay, and divergence rules as any other event; no maintenance exception exists.
7. Only the session and term selected by the fresh leased leader record can author frames that followers accept.
8. A delayed leader from an older term cannot extend a current canonical lineage or cause followers to publish its
   roots after the old authority window closes.
9. Every leader-record takeover strictly increases the one cluster-wide term.
10. Per-database state never moves backward within a term and points only to closed-transaction TRL boundaries and
    manifests whose referenced objects were uploaded and verified before publication.
11. Leader-session loss, timeout, duplication, reordering, or partitioning cannot itself grant leadership or advance
    accepted state. It may only trigger a storage-authorized election attempt.
12. Every database-state publication advances from one complete transaction boundary to another. If the last
    transaction crosses any number of TRL files, one state-record CAS selects every required segment together; no
    accepted boundary can expose a transaction prefix or leave the replay parser inside an open transaction.
13. Arbitrary loss, truncation, or inconsistent survival of local files can make a node unavailable but cannot create
    accepted state. The node either proves a complete local recovery identity or rebuilds it from durable inputs.
14. Restoring a checkpoint and replaying canonical TRL plus upstream events reconstructs the same accepted logical
    state while the required inputs remain retained. Compaction does not appear in TRL, so recovery naturally keeps the
    checkpoint/KVI-selected physical layout unless it restores a later checkpoint that already contains the compacted
    layout.
15. A lost or ambiguous leader-record or database-state response is reconciled and cannot publish partial authority or
    an older recovery state.
16. Local file reclamation is decided independently by each node and cannot remove data referenced by any of that
    node's open read-only transactions, confirmed anchor, retained speculative boundary, optimistic reader, local
    recovery cut, or repair in progress. The leader never distributes a local-delete decision because it cannot observe
    those pins. Remote reclamation cannot remove data referenced by a retained recovery manifest.
17. Standalone `BTreeKeyValueDB` behavior and its hot path remain unchanged when failover mode is not enabled.
18. Graceful shutdown may stop persistent progress but does not stop application transaction execution. After the
    shutdown canonical cut, the old leader's writes affect only its disposable `.temptrl` generation; they cannot advance
    canonical, confirmed, durable, checkpoint, compaction, or event-acknowledgement state.
19. Leak detection can only create a publication request from a pinned accepted root. It cannot delete keys. The parent
    system publishes the bounded exact-key list as an ordinary ordered event; every replica applies it through normal
    event consumption, and already-absent non-reused keys are idempotent.
20. Only the leader runs full pointer-rewriting compaction and deletes object-store files. Every node decides deletion
    of its own local cache files from its own complete pin set; no local deletion is distributed or acknowledged as part
    of the compaction protocol, and follower cleanup cannot change canonical or logical state.
21. Live compaction distribution transfers sealed PVL prerequisites and bounded physical rewrite controls over the
    leader session, but transfers neither source-file deletion commands nor later leader-created KVIs. It never uses TRL
    or canonical sequence. A follower that misses or rejects the operation remains correct on its older layout while its
    locally pinned source files remain retained, and recovery needs no compaction-aware BTDB transaction parser or skip
    path.
22. The application generation selected by the leader record never decreases. A lower-generation node cannot become
    leader, advance a retired database, or omit an active newly added database after the catalog transition commits.
23. A newly added database becomes canonical only from the exact provisional seed manifest selected together with the
    new leader term. Other provisional copies are discarded and reopened; they are never merged or promoted by equality.
24. A retired database's canonical state is frozen. Any later transaction executed by an older application is confined
    to disposable `.temptrl`, cannot receive canonical or durable acknowledgement, and cannot be recovered as canonical.

Object storage is not required to publish every transaction immediately, but every boundary it does publish is
transaction-aligned. Its lag is replay work, not data loss, as long as the upstream log retains every event after the
checkpoint. The durability dependency is therefore explicit:

```text
recoverability = valid checkpoint + closed canonical TRL boundary + retained ordered events after its eventPosition
```

Event-log retention must exceed the maximum checkpoint age, the longest object-storage outage, and the worst expected
recovery delay. If the required events expire before a newer checkpoint exists, the design has a real data-loss gap.

## Testability and ports/adapters boundary

Decision recorded 2026-08-30: every correctness-relevant part of `BTDB.Replication` must run without a real network,
Kestrel listener, cloud SDK, wall clock, or second process. The protocol is a set of deterministic state machines wired
to explicit injected ports. HTTP, Azure, process hosting, and other technologies are adapters at the edge, never types
or control flow embedded in the core.

The required boundaries include at least:

- object-store coordination, conditional TRL append, range reads, checkpoint transfer, leader-only remote garbage
  collection, and leader leases;
- the application generation/catalog provider, compatibility decisions, provisional database factory, and closed seed
  checkpoint capture;
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
scope, node/session identity, BTDB file collection, accepted roots, timers, and local failure state. The harness supplies
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

For event position `N`, application code on each node should:

1. start from its local application head whose stored event position is `N - 1`; on a follower that predecessor may be
   part of the unconfirmed speculative suffix;
2. apply the event's state changes and store `N` plus an optional event ID or rolling hash;
3. synchronously commit the local BTDB transaction and capture its exact TRL range, deterministic change trace, and
   resulting root boundary;
4. return from the local commit without waiting for the leader or any external adapter;
5. let the leader frame asynchronously confirm the cached transaction or trigger rollback, canonical replay, and
   deterministic re-execution of the dependent suffix.

A duplicate `N` is recognized as already applied, while `N + 1` before `N` is a gap and stops local event execution.
Broker offset commits are only resume hints; the event position in the accepted BTDB root is authoritative. It is safer
for a broker offset to lag and cause a duplicate than to lead BTDB and skip an event.

The application-change comparison must describe logical mutations, not incidental transport framing. It should cover
the ordered keys, values, erases, range erases, key-suffix changes, `ulong` metadata changes, event identity, and commit
metadata. Physical TRL coordinates are validated separately because long BTDB values are represented in the BTree by
file references. A semantic match with incompatible physical coordinates is not application divergence; it merely
requires rollback/replay instead of zero-work confirmation.

Application handlers must be deterministic and retryable. If a differing application frame arrives while a follower has
an unconfirmed suffix, the coordinator discards and rebuilds that suffix from the corrected base. An out-of-band
compaction operation may similarly require a physical rebase of that suffix without adding an application transaction.
External side effects must therefore not be performed inside speculative transaction execution unless they are
independently idempotent and ordered by the upstream event system.

## Leader-centered follower sessions and election triggering

Decision recorded 2026-08-30: version one has no peer-to-peer membership or failure-detection mesh. Every follower
communicates only with the current leader through the injected peer transport. The production mapping is authenticated
HTTP hosted by the parent application's Kestrel pipeline; the deterministic mapping is an in-process adapter with the
same protocol semantics.

A joining or reconnecting follower:

1. reads `cluster/leader.json` and its immutable catalog from Azure and treats their term, session, generation, database
   instance set, endpoint, API key, and lease observation as the only leader-discovery/authority source;
2. validates that active per-database state records agree with the selected leader or expose an activation in progress;
   retired state is validated against the transition's frozen predecessor instead;
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

A database-set difference does not reject a session when the follower presents a valid higher-generation catalog whose
predecessor is the leader-selected catalog. The old leader streams frames only for continued matching instance IDs. It
does not send a retired-by-target database to a follower that no longer hosts it, and it sends no frame for the target's
provisional additions. Such a node is not ready for the old catalog as an ordinary same-generation failover candidate,
but it can become ready for upgrade handoff under its proposed catalog.

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
    databaseCatalogKey / databaseCatalogHash
    protocolVersionRange
    hostedDatabaseInstances[] = {
        databaseName
        databaseInstanceId
        replicationCompatibilityFingerprint
        initialMode = replicated | provisionalLocal | retiredVolatile
    }
    lastObservedLeaderTerm
    lastObservedLeaderRevision
    databaseResumeVector[] = {
        databaseName
        databaseInstanceId
        acceptedCanonicalSequence
        acceptedFrameChainHash
        acceptedEventPosition
        canonicalTrlFileId
        canonicalTrlLength
    }
```

While that logical connection remains current, the follower reports:

```text
FollowerStatus
    nodeSessionId
    connectionId
    statusSequence
    readiness
    applicationGeneration
    databaseCatalogHash
    upgradeHandoffReadiness
    databaseProgress[] = {
        databaseName
        databaseInstanceId
        mode = replicated | provisionalLocal | retiredVolatile | reopeningFromLeader
        acceptedCanonicalSequence
        acceptedFrameChainHash
        acceptedEventPosition
        speculativeHeadEventPosition
        speculativeTransactionCount
        speculativeBytes
        canonicalTrlFileId
        canonicalTrlLength
        provisionalSeedManifestKey? / provisionalSeedManifestHash?
        provisionalSeedEventPosition? / provisionalLogicalStateIdentity?
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
    leadershipTerm
    leaderSessionId
    leaderRevision
    messageSequence
    kind = heartbeat | termStart | databaseCatalog | catalogTransition | databaseGenesis | databaseBootstrap |
           canonicalFrame | trlProgress | compactionStart | compactionChunk | compactionComplete | steppingDown |
           handoffOffer | handoffFinal
    payload

TrlProgress
    databaseName
    lineageId
    firstCanonicalSequence
    lastCanonicalSequence
    segments[] = { fileId, startOffset, endOffset, segmentHash }
    transactionEndFileId / transactionEndOffset
    firstEventPosition
    lastEventPosition
    rangeHash
    durability = leaderLocal | objectStorePublished
    databaseStateRevision
```

`databaseCatalog` is the first state-bearing message on a new session and must match the catalog reference in the
leader record. `catalogTransition` identifies continued, added, and retired database instance IDs. `databaseGenesis`
names the one selected seed from which matching provisional followers must close, restore, and reopen; it never asks a
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

Exact transaction-byte comparison is the primary zero-work confirmation path, but it is valid only for the same event,
local TRL allocation plan, BTDB format/configuration fingerprint, and physical checkpoint base. A logical change trace
diagnoses why a byte comparison failed; it does not authorize a different encoding to bypass rollback/replay. After
confirmation, the event, logical mutations, sequence, and frame-chain identity agree with the leader. BTree value
references need not agree when the follower legitimately retained an older physical layout; that difference is tracked
by the separate physical-layout identity and cannot by itself be reported as application divergence. Followers never
initiate pointer rewriting independently.

## What the current BTDB code implies

The existing code provides useful integration points, but it is not currently a multi-node protocol.

### BTDB file behavior

- `BTreeKeyValueDB` serializes writing transactions inside one process. That lock says nothing about another process.
- `BTreeKeyValueDBTransaction.MakeWritable()` immediately writes `MagicStartOfTransaction` to the one active TRL.
  Cursor mutations then write their commands directly to that writer before changing the writable BTree root.
- `Commit()` is synchronous. It writes metadata deltas and the commit marker, flushes or hard-flushes according to
  `DurableTransactions`, updates the root's TRL cursor, and only then replaces `_lastCommitted`.
- The copy-on-write root and reference counting provide most of the rollback primitive. `StartReadOnlyTransaction()`
  references the current `_lastCommitted` root, keeping that historical root and its file references alive while later
  commits advance `_lastCommitted`. What is missing is an opt-in operation that, with no writing transaction active,
  atomically restores such a pinned historical root as the working head and starts a safe new local TRL continuation.
- Values longer than seven bytes are not stored in BTree memory. Their 12-byte `trueValue` contains
  `(fileId, valueOffset, valueLength)` pointing into a `.trl` or `.pvl`. A speculative transaction is a zero-work match
  only when its complete encoded transaction and physical references equal the leader transaction; logical equality
  alone falls back to rollback and replay.
- The active `.trl` grows by appending commands and commit markers. Existing rollback handling aborts only the current
  uncommitted writing transaction and can append a rollback marker; it cannot restore `_lastCommitted` to an earlier
  committed root. Failover therefore needs a distinct committed-suffix rollback operation.
- A large transaction can cross physical TRL files. Several command writers call `WriteStartOfNewTransactionLogFile()`
  when the current file reaches `MaxTrLogFileSize`, even though the transaction is still open. Startup replay preserves
  `_nextRoot` across that file boundary and publishes it only after a later `Commit` or `CommitWithDeltaUlong`. Therefore
  `EndOfFile`, a new TRL header, or a file-length cut is not by itself a transaction boundary.
- `.kvi` and `.pvl` files are written as new files and become immutable after finalization.
- File IDs and generations are allocated from local collection state. In failover mode, canonical `.trl` and `.pvl`
  allocation must instead be leader-assigned and reproduced on followers because those IDs are embedded in roots. KVI
  files are not part of live compaction distribution. Any follower-local KVI must use a disjoint cache identity, and a
  future leader must allocate canonical IDs from canonical cluster state rather than its local maximum file ID.
- Compaction creates replacement files and removes obsolete files. Remote deletion must become delayed garbage
  collection, not an immediate destructive operation.
- `LoadTransactionLog()` already decodes TRL commands into a writable root, but it is a startup-oriented routine that
  mutates loader fields and publishes commits as it scans files. There is no live API to validate one framed
  transaction, append leader-authored bytes to a mirror, apply them from a specified accepted root, and publish the
  result atomically.
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
CommitFollowerSpeculation(eventIdentity)
    -> ordered local TRL segments + logical change digest + pinned resulting-root boundary

CaptureLeaderCommit()
    -> ordered canonical TRL segments + exact committed bytes + logical change digest + resulting root identity

ConfirmMatchingSpeculativeTransaction(speculativeEntry, canonicalFrame)
    -> advance confirmed anchor with no BTree replay

RestorePinnedBoundary(confirmedAnchor)
    -> atomically replace speculative working head + abandon suffix + open new local TRL/speculation generation

ReplayCanonicalTransaction(workingRoot, canonicalFrame)
    -> validate/install bytes + decode commands + commit corrected root

ValidateClosedTransactionBoundary(segments, expectedEnd, expectedSequence, expectedFrameHash)
    -> valid closed transaction | incomplete/corrupt

ReexecuteSpeculativeEvents(orderedEvents)
    -> rebuilt speculative entries rooted at the corrected head

CaptureLeaderCompactionChunk(basePhysicalLayout, relocationData, cancellation)
    -> bounded out-of-band rewrite operation + resulting physical-layout identity

ApplyLeaderCompactionOperation(confirmedAnchor, operation, verifiedPvlFiles)
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
    -> retained root or explicit restore/replay requirement

EnterVolatileShutdownMode(closedCanonicalBoundary)
    -> irreversible redirection of later transaction/value bytes to disposable local temptrl files

CommitVolatileShutdownTransaction(eventIdentity)
    -> disposable process-local BTree root + readable temptrl ranges
```

These names are descriptive, not proposed public API names. The existing `IKeyValueDBTransaction.Commit()` remains a
synchronous local operation. During ordinary failover leader/follower operation it may perform only the local work needed
to append/cache the transaction, commit the root, and retain comparison/rollback metadata. It must not await the peer
transport, leader progress, object storage, or a background queue with a fixed speculative-window limit. Volatile
shutdown is the explicit no-persistent-append exception described below.

The reconciliation coordinator is asynchronous and briefly enters the same local writer serialization only to advance
root-retention metadata or perform an exceptional rollback/replay. All remote bytes and prerequisites should be fetched
and verified before that critical section. Exact transaction equality is the primary fast path: the existing local root
and bytes remain untouched while the confirmed anchor advances. A difference always uses the slower restore/replay path;
logical digests then distinguish an application divergence from a harmless encoding or physical-layout mismatch.

There is no configured maximum number of speculative transactions imposed by the protocol. The ordered index, root
pins, and transaction bytes must spill to local storage and remain retained until confirmed or abandoned; leader lag is
reported as count, bytes, event distance, and age rather than converted into admission backpressure. Infinite storage
cannot be guaranteed: if the local file collection cannot complete the ordinary synchronous commit because the device
is full or failed, that local transaction fails as an I/O failure. Network slowness alone must never cause that failure.
Starting a follower write may wait only for BTDB's ordinary local single-writer serialization or an in-progress local
rollback/replay critical section; it never waits merely because the speculative suffix is far ahead of the leader.

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
canonical transaction sequences, advance the confirmed anchor, emit `leaderLocal` frames, enqueue object-store
publication, write KVI/PVL files, or acknowledge upstream events at a durability level that the shutdown canonical cut
did not reach. A successor deterministically re-executes those events from its adopted durable base. If temporary storage
is full or lost, the affected local transaction may fail explicitly; the generation must never fall back to Blob storage.

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

Decision recorded 2026-09-01: replication introduces no BTDB maintenance transaction. Every canonical TRL transaction
is produced by consuming exactly one ordinary upstream application event. Term changes, checkpoint publication,
compaction controls, artifact transfer, KVI creation, and file-retirement decisions are protocol or storage operations
outside TRL and consume no canonical transaction sequence.

Leak reachability is an ObjectDB-level question, while the logical deletion must enter the same ordered input path as
every other state change. Any ready node may detect leaks against a pinned accepted root, never its speculative head,
but it cannot erase accepted state or send a special command to the leader. It creates a bounded candidate with semantics
equivalent to:

```text
LeakRemovalCandidate
    publicationRequestId
    databaseName / databaseInstanceId
    detectorVersion
    observedLeadershipTerm
    observedCanonicalSequence / observedFrameChainHash / observedLogicalStateIdentity
    keyEncodingVersion
    keyCount / encodedKeyBytes / keyListHash
    encodedKeys = repeated(keyLength, exactKeyBytes)
```

The node submits that candidate through the injected `ParentEventPublisher`. The parent system validates the database,
detector and key encoding, strict ordering, duplicate absence, key/count/byte budgets, and hash; reconciles ambiguous
publication by `publicationRequestId`; and publishes a normal application event into that database's ordered stream.
The replication peer protocol is not involved. A successful request means the parent accepted or already accepted the
event, not that any BTDB mutation has happened yet.

The event payload carries the validated exact-key list and ordinary parent-assigned event identity. When it reaches a
replica through normal consumption, the application's normal handler opens one writable transaction, erases every key
that is still present, ignores already-absent keys, and stores the event position in the same commit. Removed exact keys
are never reused, so delay, overlap, or a second removal event cannot make an old candidate erase newly repurposed data.
Even when all keys are already absent, consuming the event still advances the ordinary event cursor exactly once.

The current leader authors the resulting ordinary canonical TRL transaction because it consumes the event as usual.
Followers produce their ordinary speculative transaction for the same event and use the existing exact-match or
rollback/replay path. Startup sees only a normal application transaction. Leader change, disconnection, or graceful
shutdown needs no leak-specific recovery: the retained event is replayed in total order by the successor. Detection can
run again and the parent may deduplicate by request/list identity or publish another idempotent event.

The list-size budget bounds the normal application writer pause. Larger results are split by the parent into several
bounded ordered events or detected again later; this batching policy is part of the application-event contract rather
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
    clusterId / databaseName / databaseInstanceId
    leadershipTerm / leaderSessionId / leaderRecordRevision
    operationId / chunkIndex / previousChunkHash
    baseCanonicalSequence / baseFrameChainHash / baseLogicalStateIdentity
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
   collectible only after a selected compacted checkpoint is recoverable and every retained manifest releases them.
   Follower acknowledgements are diagnostic/optimization state, never the remote-deletion proof.

The base canonical sequence, frame hash, logical identity, and source physical identity are captured separately for
each chunk while holding the writer gate; they are not one stale base reused for the whole compaction. Leader stream
ordering places each `compactionChunk` exactly between the application frames that precede and follow its local physical
commit. A follower that deliberately retains the older layout acknowledges and advances past that control message so a
missed optimization never becomes a permanent gap in application-frame delivery.

A follower applies an operation only while the term/session remains authoritative and the declared application base,
logical identity, source physical layout, chunk order, hashes, and PVLs all match. If its confirmed anchor is exactly the
operation base but its speculative head is later, it fetches prerequisites first, briefly enters the writer gate,
restores that confirmed root, applies the physical chunk, and re-executes the dependent application suffix. This changes
physical references only and is not application divergence.

A follower that has already advanced past the required base, has another valid physical layout, misses a control range,
or cannot satisfy a relocation precondition reports `retainedOlderLayout` and continues normal application processing.
The leader must not block application progress waiting for compaction acknowledgements. A later selected checkpoint can
converge that follower's layout only on a future restart or explicit rebuild that transfers its KVI as the new bootstrap;
it is never pushed into the running database. Until then, ordinary canonical transactions remain logically replayable on
the older layout, although the zero-copy physical fast path may miss more often.

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
for databases active in the leader-record-selected catalog.

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
  "databaseCatalogKey": "cluster/catalogs/sha256-....json",
  "databaseCatalogHash": "...",
  "catalogTransitionKey": "cluster/catalog-transitions/sha256-....json",
  "catalogTransitionHash": "...",
  "peerEndpoint": "http://10.42.1.17:8080/_btdb/replication",
  "apiKey": "base64url-random-256-bit-value"
}
```

The protocol core treats `peerEndpoint` as opaque. The version-one HTTP adapter publishes the routable IP address and
port of the leader's Kestrel listener plus the mapped failover base path; it may also permit a routable DNS name. The
in-process transport uses a test-local endpoint resolved by its injected registry.

The JSON intentionally contains no per-database progress or inline database list. Its immutable catalog reference
selects the canonical database set, and its optional transition reference fixes added-database seed manifests plus
continued/retired instance IDs. `applicationGeneration` is both the selected application version and a persistent
election floor; a later replacement must not lower it. The Azure ETag serializes leader identity and catalog changes,
while a finite native Blob lease on this same blob supplies server-enforced liveness. A session is authoritative only
when it
both holds the current lease ID and the JSON names its term and session. `revision` changes on record replacement;
ordinary lease renewals do not rewrite the JSON or change its ETag.

`apiKey` is a randomly generated per-term secret used to authorize failover peer requests. Every trusted node can read
it from Azure, so it authenticates participation in this storage trust domain rather than giving per-node identity. The
shared protocol authorization component compares it in constant time, excludes it from logs and metrics, and rotates
it on every leader change. The HTTP adapter carries it as a bearer credential and uses TLS unless the deployment
explicitly accepts a trusted private-network boundary.

### Per-database state record

Each database instance has an independent mutable state object, for example
`databases/main/stable-instance-id/state.json`:

```json
{
  "format": 1,
  "databaseName": "main",
  "databaseInstanceId": "stable-instance-id",
  "databaseCatalogHash": "...",
  "leaderTerm": 7,
  "leaderSessionId": "random-node-session-id",
  "revision": 103,
  "lastOperationId": "unique-database-update-id",
  "canonicalTrl": {
    "lineageId": "term-and-file-derived-id",
    "boundary": {
      "descriptorKey": "databases/main/stable-instance-id/trl/boundaries/...json",
      "descriptorHash": "...",
      "endFileId": 23,
      "endOffset": 456789
    },
    "currentFile": {
      "fileId": 23,
      "objectKey": "databases/main/stable-instance-id/trl/terms/7/23-a1b2c3.trl",
      "committedLength": 456789,
      "lastAppendToken": "opaque-provider-version-token",
      "prefixHash": "..."
    },
    "lastCanonicalSequence": 789012,
    "lastFrameChainHash": "...",
    "lastAppliedEventPosition": 123456
  },
  "checkpoint": {
    "canonicalSequence": 788500,
    "frameChainHash": "...",
    "lastAppliedEventPosition": 123000,
    "manifestKey": "databases/main/stable-instance-id/manifests/...json",
    "manifestHash": "...",
    "previousManifestKey": "databases/main/stable-instance-id/manifests/...json"
  }
}
```

The record's ETag is its CAS token. `boundary` selects an immutable, hash-verified descriptor whose final cursor is
immediately after a complete commit command. The descriptor links its predecessor and names every ordered TRL file cut
and transaction segment added by this publication. A transaction crossing file rotation therefore has one descriptor
containing the tail of the old file, any intermediate files, and the committed prefix of the final file. One state CAS
publishes that descriptor, canonical sequence, frame hash, and event position together.

`currentFile.committedLength` and `prefixHash` repeat the final accepted active-file cut needed for efficient continuation;
physical object length may be longer after an ambiguous append or failed state publication. `lastAppendToken` is the
publisher's opaque continuation token, not a content identity: recovery can still accept a hash-verified prefix when the
physical object has a newer token caused only by a later append. Recovery never infers the chain, accepted length,
transaction closure, or order from physical object length or listing.

There is no atomic update spanning the leader record and database state records. Every database state therefore embeds
the leader term and session, and every data key is term-qualified. For live publication, followers accept a state
record only when its term and session agree with the current leader record. A previous-term state remains the proposed
durable predecessor during leader activation, but it is not current-term publication until the new leader adopts it as
described below. A retired state is the explicit exception: the catalog-transition descriptor selects it as a frozen
predecessor, no current-term publication is accepted for it, and old followers can use it only as the base of disposable
local execution.

Updates for each database pass through one serialized in-process publisher so its TRL-boundary and checkpoint updates do
not race on one ETag. The shared leader record has a separate, isolated lease/leadership lane.

#### Transaction-aligned durable boundary

Decision recorded 2026-08-30: every durable object-store cursor must end at a complete BTDB transaction boundary. This
rule applies to the ordered TRL chain, not to an individual physical file, because one transaction may span many files.

The immutable boundary descriptor needs semantics equivalent to:

```text
TrlBoundaryDescriptor
    format
    databaseName
    databaseInstanceId
    databaseCatalogHash
    leadershipTerm
    leaderSessionId
    lineageId
    previousDescriptorKey? / previousDescriptorHash?
    baseCanonicalSequence / baseFrameChainHash
    lastCanonicalSequence / resultingFrameChainHash / lastAppliedEventPosition
    fileCuts[] = {
        fileId
        objectKey
        acceptedLength
        prefixHash
        sealed
    }
    transactions[] = {
        canonicalSequence
        eventStreamId / eventPosition / eventHash?
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
a subset of the transaction's files. Unselected data is ignored during recovery and reclaimed only by delayed garbage
collection.

#### Per-database term adoption fence

Acquiring the leader-blob lease physically fences writes only to `cluster/leader.json`; it does not lease every
database state blob. A new leader must therefore adopt or create and fence each database active in its selected catalog
before starting canonical work:

1. For a continued instance, read the previous state and ETag. Its selected TRL prefix and checkpoint are the candidate
   durable predecessor. For an added instance, verify the exact seed manifest selected by the leader record's catalog
   transition and require that no state exists yet.
2. For a continued instance, create the new term's empty remote TRL file and immutable initial boundary descriptor
   linking the predecessor's complete transaction boundary. For an addition, create the empty post-genesis lineage
   linked to the selected seed checkpoint and its sequence-zero frame-chain identity.
3. For a continued instance, CAS-replace the database state using the ETag from step 1. Preserve the accepted sequence,
   frame hash, event position, and checkpoint, but publish the new leader term/session, selected catalog hash, and the
   new empty active lineage. For an added instance, conditionally create its state at canonical sequence zero from the
   selected seed checkpoint and genesis frame-chain identity. An unrelated preexisting state or instance ID is a safety
   fault, never an implicit predecessor.
4. If an in-flight previous-term state update wins first, the adoption CAS is rejected. Reread the newer predecessor,
   rebuild any derived index, and retry while still performing no canonical work for any database.
5. After adoption succeeds, every pending previous-term request using the older ETag is rejected. A correct old session
   is already self-fenced and cannot begin another update by rereading the state.

Only after every database active in the selected catalog has a matching state record adopted or created by the current
leader term/session may the cluster leader become write-ready. Retired states are deliberately not adopted and remain
frozen under their last canonical term. This is the non-Byzantine fence for per-database publication: the Azure lease
establishes the sole current session, while each continued-state ETag drains and excludes requests already issued by its
predecessor and each added-state conditional create binds the exact selected genesis.

### Election, renewal, and fencing

1. A joining node starts as a follower, reads `cluster/leader.json`, its immutable catalog, and its Azure lease state.
   It opens continued databases from their state/checkpoint boundaries, opens newly introduced local instances
   provisionally, and connects to the selected leader endpoint.
2. A same-generation node is election-eligible only when its catalog hash equals the selected catalog and every active
   database is reconstructed to its required durable boundary. A higher-generation node is eligible to perform a catalog
   transition only when its catalog declares the selected predecessor and it has complete seed manifests for every
   addition. A node below the selected application-generation floor is never eligible.
3. If the leader blob does not exist, candidates conditionally create an initial placeholder. Otherwise only loss of
   the selected leader session, inability to establish it, or an authority mismatch triggers candidacy; none of those
   observations revokes an active lease.
4. An eligible candidate waits randomized backoff and attempts to acquire the finite Azure Blob lease with a freshly
   generated lease ID. Azure grants it to at most one candidate after the prior lease is available or expired.
5. The lease winner rereads the JSON and ETag, then conditionally replaces it while presenting the lease ID. It names
   its session, increments the cluster term, publishes its nondecreasing application generation, immutable catalog and
   transition references, transport-defined peer endpoint and a new random API key, and records a unique operation ID.
6. A clean CAS rejection is reconciled while holding the lease. An ambiguous write is reconciled by reading the
   operation ID, term, and session. The node performs no leader work until both lease ownership and the matching JSON
   are confirmed.
7. For each continued database, it discards any directly received but unpublished predecessor tail and runs the
   per-database term-adoption fence. For each addition it creates state from the transition-selected seed. It leaves
   every retired database state untouched. A continued adoption starts a new term-qualified remote TRL lineage without
   advancing canonical sequence or event position.
8. Only after every active database state names the new term/session and catalog does the node mark its leader endpoint
   ready, accept canonical writes, publish direct frames, or start compaction. Before that it is an activating lease
   holder, not a write-ready leader.
9. The leader renews the finite Blob lease well before expiry through a dedicated client and connection pool. Lease
   renewal does not rewrite the JSON or contend with per-database state updates.
10. A failed/ambiguous renewal that cannot be reconciled before the conservative self-fence margin, a lease mismatch,
    or a leader record naming another term/session stops canonical work for **all** databases. Existing accepted roots
    may still serve reads.

The baseline unplanned takeover may roll each database back to a different durable sequence/event position because
their remote TRL boundaries and state records are independent. The new cluster term is shared, but replay is per
database from its own upstream event cursor. Peers that accepted an unpublished old-term tail rewind or restore that
database before accepting its new-term frames.

Term-qualified remote TRL keys are the data fence. A delayed old process may finish an in-flight append to its old
term's object, but physical length alone cannot make those bytes authoritative. Only a previous-term state CAS that wins
before the adoption fence can select a verified closed boundary; the adoption either chooses that boundary as the single
predecessor or fences the request out. Bytes beyond any descriptor-selected file cut remain ignored. Each direct frame
carries database name, cluster term, leader session, and leader-record revision. Followers periodically reread the leader
record and accept frames only while its cached Azure authority observation remains safely fresh.

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
   same-generation candidate must match the selected catalog, run a compatible protocol/BTDB format, expose a usable
   candidate endpoint, be within the handoff lag limit for every active database, and retain the events needed after
   every final durable base. It chooses randomly and sends the handoff offer over that follower's existing authenticated
   session. A prepared higher-generation target instead follows the catalog-transition readiness and seed rules above
   and has priority over same-generation shutdown targets.
6. The target obtains and verifies frames and prerequisites through each relevant shutdown canonical cut when available,
   and independently verifies every object-store-selected durable base. A higher-generation target additionally
   uploads and reports immutable seed closures for its added databases and explicitly lists retirements. The old leader
   may serve already existing bytes but cannot create or persist new canonical ones. The target responds `prepared` with
   progress vectors, any seed identities, and a freshly generated proposed Azure lease ID. An unpublished old-leader
   tail remains an optimization only; correctness permits dropping it and replaying its ordered application events.
7. The old leader keeps only the authority/control lane alive: leader-record reads, lease renewal, reconciliation reads,
   `Change Lease`, and eventual lease release. These operations preserve or transfer fencing authority and are the sole
   Blob-plane exception to the no-persistence rule; no TRL append, descriptor, database-state/checkpoint update, PVL/KVI
   write, or object deletion is dispatched. It calls `Change Lease` with the target's proposed ID.
8. The target proves ownership by renewing with that ID and CAS-publishes term `T + 1`. A same-generation target adopts
   every active database from its final durable base. A higher-generation target atomically selects its new catalog and
   seed descriptor, adopts continued databases, creates additions from those exact seeds, and freezes retirements. It
   then replays later ordered events. Verified exact frames or speculative entries may accelerate continued-database
   work, but they cannot replace the durable-base proof or the selected seed. Only after every active database is
   adopted or created may the target author canonical work and publish its endpoint as ready.
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

The failover protocol sees the remote TRL as a logically append-only **ordered file chain**. Conditional atomic tail
append applies to one physical file; transaction-aligned state publication composes one or more file updates into a
single durable BTDB boundary. The sequence for one database is:

1. While authority is fresh, consume one application event and commit its ordinary transaction to the real local TRL. Capture
   every ordered segment from transaction start through the final commit command, including all crossed TRL files.
2. Validate locally that the segments form one closed transaction from the preceding canonical cursor. A file rotation,
   `EndOfFile`, or TRL header is only a continuation; no frame or durable-publication candidate exists until the final
   commit command has been captured.
3. Assign the next canonical sequence and frame-chain hash, publish the accepted leader root, and make the complete
   multi-segment frame available through the direct endpoint.
4. Stream a compact `leaderLocal` descriptor and the complete frame through each follower's leader session. A follower
   may stage segments incrementally, but it advances the confirmed anchor only after all segments and the closing commit
   have been validated. Exact bytes advance the anchor in place; a difference restores that anchor and applies all
   leader-authored segments before rebuilding the speculative suffix.
5. Batch only whole contiguous canonical transactions. Revalidate leader authority within its safety margin, then
   conditionally append the required suffix to the previously active remote file and create or complete every newly
   rotated TRL object. Verify the accepted length and prefix hash of every touched file.
6. Create and verify an immutable boundary descriptor linking the previous durable boundary and listing all newly
   accepted transaction segments, file cuts, hashes, sequences, and the final closed-transaction cursor.
7. Revalidate authority before dispatch, then CAS-replace that database's state record. In one update, select the new
   boundary descriptor and final current-file cut, advance canonical sequence, frame hash and event position, and record
   the current term/session, revision, checkpoint fields when relevant, and operation ID.
8. After the database-state CAS succeeds, stream the transactions as `objectStorePublished` with the leader and database
   state revisions. Replicas may release rollback history older than that database's retained recovery boundary.
9. On any file-write rejection or ambiguity, reread and reconcile every affected object's expected length, token,
   bytes/hash, and operation identity; never blindly repeat a write. No descriptor may be published until the complete
   multi-file transaction is proven. If an unaccepted suffix cannot be proven to be this exact batch, abandon the
   lineage rather than append after unknown bytes.
10. On database-state CAS rejection or ambiguity, stop publishing that database until the record and operation identity
    are reconciled. Loss of cluster leadership stops all databases immediately.

Object-store publication may batch several **complete** transactions because a small lost tail can be regenerated from
the event log. There is no Azure request per application transaction unless the configured batch size is one. The live
leader TRL remains canonical while its authority is valid, but only the database-state-selected closed boundary survives
an unrecoverable leader loss by definition.

Normal BTDB TRL rotation can happen between transactions or inside one. It never independently advances durable state.
The next transaction-aligned descriptor seals every completed old-file cut, names the next remote file identity, and
publishes the entire crossed-file transaction in one database-state update. Batch sizing balances remote replay lag,
shutdown handoff latency, and storage request cost; provider-specific physical limits stay below this abstraction.

### Canonical frame and comparison protocol

The direct protocol needs its own versioned envelope around raw BTDB bytes. An illustrative frame is:

```text
CanonicalFrame
    protocolVersion
    clusterId
    databaseName
    databaseInstanceId
    databaseCatalogHash
    leadershipTerm
    leaderSessionId
    leaderRecordRevision
    authorityObservationDeadline
    lineageId
    canonicalSequence
    previousFrameChainHash
    eventStreamId / eventPosition / eventHash?
    baseCanonicalSequence
    baseLogicalStateIdentity
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
    resultingLogicalStateIdentity
    resultingFrameChainHash
    payload
```

`canonicalSequence` and event position advance together for every canonical frame. `termStart` is a separate
`LeaderMessage` control, not a BTDB transaction, and consumes neither. Compaction controls and PVL artifacts use their
separate out-of-band message family and never appear in this envelope. `trlSegments` is ordered and can cross any number
of file boundaries. A receiver may stream segments to temporary local files, but the canonical transaction frame is
indivisible: it is accepted only when the final segment ends exactly at the declared commit terminator and every segment
hash is valid.

Follower validation is deliberately strict:

1. validate the bearer API key and requested database name/instance against the leader-record-selected catalog;
2. verify that term/session, endpoint, and authority freshness agree with the Azure leader record, not merely the
   existing HTTP connection or its control messages;
3. require the exact confirmed base sequence, logical root identity, lineage, and previous frame-chain hash;
4. validate every segment's file identity, offsets, size, hash, continuity, and final commit terminator before using
   referenced bytes or advancing the confirmed anchor;
5. treat an exact duplicate as idempotent;
6. treat two different frames for the same `(databaseName, term, canonicalSequence, baseIdentity)` as an
   equivocation/safety fault, freeze that database, retain evidence, and reread leader and database state;
7. align the application event identity with the oldest unconfirmed speculative entry and compare the
   complete transaction encoding, including commit metadata and physical coordinates;
8. if the follower has not executed that event and its speculative head equals the confirmed anchor, replay the leader
   frame directly and advance both boundaries; follower input lag is not divergence;
9. on exact equality, advance the confirmed anchor to that entry's retained root and release superseded pins; do not
   replay the transaction, rewrite the BTree, or copy its already-identical local bytes;
10. on any difference, retain diagnostics and the ordered later event identities, restore the previous confirmed anchor,
   replay the leader frame, and re-execute the dependent speculative suffix. A logical-digest difference is reported as
   application divergence; byte-only or physical-layout differences are recorded as fast-path misses;
11. before publishing a replayed root, install every leader-authored byte/artifact it references. Exact-match promotion
    may reuse local cached bytes only because equality with the leader frame has already been proven.

`resultingLogicalStateIdentity` should be cheap to maintain. The term, canonical sequence, TRL cursor, and frame-chain
hash identify the accepted application history, but they must not pretend to identify one physical BTree layout because
out-of-band compaction may have been applied only on some nodes. A compaction operation separately names the physical
layout it expects and produces. Computing a cryptographic hash of the complete BTree on every commit would violate the
performance goal. A periodic logical-state audit hash can detect implementation or memory divergence beyond the frame
chain without burdening every transaction.

On an application mismatch, diagnostics should retain the database, term, sequence, event identity, base identity,
leader digest, follower digest, and a bounded/redacted mutation diff. The dependent speculative suffix is abandoned,
canonical bytes are replayed from the pinned anchor, and later events are re-executed. Whether a repaired node stays
ready or enters a degraded state is an operational policy; the mismatch must never be silently counted as success.

### Peer transport abstraction and Kestrel adapter

The core depends only on the peer transport and peer endpoint contracts described in the testability section. It does
not reference HTTP, ASP.NET Core, Kestrel, URI routing, headers, or sockets. Reconnect and resume are expressed in terms
of `(term, lineage, canonicalSequence, frameChainHash)` and typed transport outcomes.

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

Database routes accept only short names and instance IDs active in the selected catalog and never map arbitrary path
text to a filesystem path. Session IDs must belong to the currently open authenticated node/session connection.
Handoff offers and the shutdown progress vectors are leader-stream control messages: they distinguish each continued
database's last direct canonical cut from
its possibly older durable base and carry seed/retirement declarations for a catalog transition. A target's response is
cluster-wide and covers every database active in the catalog it proposes.

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

```json
{
  "format": 1,
  "clusterId": "stable-cluster-identity",
  "databaseName": "main",
  "databaseInstanceId": "stable-instance-id",
  "databaseCatalogHash": "...",
  "checkpointId": "position-and-content-derived-id",
  "leadershipTerm": 7,
  "lineageId": "term-and-checkpoint-derived-id",
  "eventStreamId": "ordered-stream-identity",
  "canonicalSequence": 789012,
  "frameChainHash": "...",
  "lastAppliedPosition": 123456,
  "lastAppliedEventHash": "optional-rolling-hash",
  "logicalStateHash": "optional-divergence-check",
  "files": [
    {
      "fileId": 17,
      "type": "kvi",
      "length": 1234,
      "sha256": "...",
      "objectKey": "objects/sha256/..."
    }
  ]
}
```

The manifest itself is immutable and uploaded with create-if-absent semantics after every referenced object is
present and verified.

Checkpoint publication sequence:

1. Capture and pin a self-consistent BTDB file set at canonical sequence `S`, event position `P`, and frame-chain hash
   `H`.
2. Hash and upload every required file under an immutable content- or attempt-qualified key.
3. Upload the immutable manifest only after all referenced files are present.
4. Read the leader record and verify that this process still holds its lease and is named by the same session and term,
   then read this database's state record and ETag.
5. If `(S, P)` equals the database state's checkpoint boundary, compare frame, event, and available logical-state
   hashes. Matching state keeps the existing checkpoint; a mismatch is a safety fault. An older checkpoint is never
   published.
6. Replace exactly the database-state ETag read in step 4, advancing only its checkpoint fields and recording a new
   operation ID. Lease renewal remains independent on the leader lane.
7. On a clean CAS rejection, reread both records and first verify that the same session and term are still leader. On
   ambiguity, reconcile the database operation ID, manifest hash, and checkpoint position.

The leader can continue applying events after the selected file set is safely pinned. If it loses leadership during
upload, its immutable objects are harmless, but the old term cannot publish them into current database state.

## Candidate object layout

An illustrative, deliberately non-final namespace is:

```text
cluster/
    format.json
    leader.json
databases/<database-name>/
    state.json
    trl/terms/<term>/<file-id>-<lineage-id>.trl
    trl/indexes/<index-hash>.json
    manifests/<canonical-sequence>-<event-position>-<manifest-hash>.json
    objects/sha256/<content-hash>
    attempts/<term>/<leader-session>/<attempt-id>/...
    gc/...
```

Important properties are more significant than the exact names:

- Bulk objects and manifests are immutable.
- The leader record contains no database data references. A database state record never references an object that was
  not uploaded first.
- Leadership term and lineage are explicit in mutable-TRL or range keys, so an old leader cannot extend a new term's
  accepted lineage.
- Leader-authored `.trl` bytes and every referenced file must be available on each replica that uses them. Live
  compaction may give replicas the same `.pvl` layout, while a missed control or recovery from an older KVI naturally
  retains older references because TRL contains no rewrite. Such a replica may have a different physical layout, but
  every reference must resolve to the same logical value and remain within files protected by retention rules.
- Content-addressed objects can deduplicate equal files, while attempt-qualified objects are a simpler fallback for
  files whose content differs because of compaction.
- A manifest contains every `.trl`, `.pvl`, and `.kvi` file needed to open its checkpoint. It must not depend on an
  unlisted local cache file.
- Remote physical deletion is initiated only by the current leader and only after no retained database state or fallback
  manifest references the object; local follower cleanup is independent and affects only disposable cache files.

## Event application, checkpoint, and recovery sequences

### Normal event application

Leader path:

1. Read event `N` only after accepted event position `N - 1` and verify that leadership authority remains fresh.
2. Run the deterministic application transaction, including event-position metadata.
3. Commit it to the real TRL, assign canonical sequence `S`, and capture every exact TRL segment through the closing
   commit command plus the logical change digest.
4. Publish the leader's accepted root and direct frame `S` under the current term.
5. Enqueue the frame for batched object-store publication.

Follower path:

1. Read the same event `N` after the local application head reaches `N - 1`; leader confirmation of `N - 1` is not
   required.
2. Run and synchronously commit the application transaction to the local speculative TRL cache. Retain its speculation
   generation, ordered byte segments across all touched files, digest, event identity, and resulting-root pin, then allow
   event `N + 1` immediately.
3. Independently consume canonical frames in sequence from the confirmed anchor. Network delay may grow the speculative
   suffix but does not impose a protocol window or wait on local write transactions.
4. For an application frame already executed locally, compare it with the oldest unconfirmed entry. Exact
   byte/coordinate equality only advances the confirmed anchor; the BTree is already at that state or later and does no
   replay. If the follower has not yet executed the event and has no speculative suffix, apply the leader frame directly.
5. On the first difference, fetch prerequisites, briefly serialize with local writing, restore the pinned anchor, apply
   the canonical application frame, and re-execute every later speculative event in order. Out-of-band compaction uses
   the separate physical-rebase protocol described above.
6. Advance confirmed sequence/event metrics independently from speculative-head metrics and report a logical mismatch
   without treating a physical-only fast-path miss as split-brain.

Stepping-down leader path after a database reaches its shutdown canonical cut:

1. Keep reading the ordered application stream from the database's process-local event position; do not start another
   canonical transaction.
2. Run each application transaction against the current volatile head. Store new non-inline values, and any TRL command
   bytes required by the selected BTDB implementation, only in the volatile `.temptrl` generation so reads within this
   and later transactions remain correct.
3. Return the explicitly local/volatile commit result and allow the next event immediately. Do not advance the canonical
   sequence or confirmed anchor, emit a frame, acknowledge durability, hard-flush the scratch file, or touch a remote
   database-data adapter.
4. Discard the volatile root and delete the scratch generation on exit. The successor adopts the final durable base and
   replays every later event; none of the old process's post-cut files is a recovery or handoff prerequisite.

There is no object-store request for each transaction. Lease renewal and durable batch publication run independently,
but the leader and followers use a conservatively cached authority deadline and freeze canonical acceptance before it
becomes unsafe. Non-authoritative speculative execution may continue.

### Per-database replay hierarchy

Replay is independent for every active database instance; the shared leadership term does not create one cross-database
replay cursor. A replica reconstructs one database in this order:

1. Validate and use a local KVI/file set when it exactly matches a retained manifest or accepted state identity.
2. Otherwise accept one `databaseBootstrap` naming the newest valid immutable manifest and KVI selected by that
   database's state record, and fetch its hash-verified closure through the leader artifact endpoint or object storage.
3. Follow the state-selected linked boundary descriptors and their exact file cuts, starting at the KVI's TRL cursor.
   Replay ordinary TRL transactions and cross-file continuations in explicit file/offset order. Bytes or objects not
   selected by those descriptors are ignored even when physically present.
4. Stop exactly at the state record's final `(fileId, offset)`. Require the decoder to have no open transaction and the
   resulting canonical sequence, frame-chain hash, event position, and root identity to match the selected boundary.
5. After reaching the Azure durable boundary, connect to the endpoint from `cluster/leader.json` and resume direct
   frames from the exact `(term, sequence, frameChainHash, fileId, offset)` cursor.
6. Consume the corresponding upstream application events to build the follower's speculative suffix and divergence
   evidence. The leader alone uses those events to regenerate canonical transactions lost beyond an old term's durable
   remote TRL boundary.

A planned handoff verifies the final Azure durable cursor for every continued active database before transferring the
leader lease and also reports how much of each newer shutdown canonical cut the target already has. A catalog transition
additionally verifies each immutable added-database seed and explicitly freezes retirements. Exact direct bytes may
reduce replay, but transfer does not wait for the canonical cut to become durable and never treats an unselected tail as
recovery truth. Both planned and unplanned takeover adopt each continued state-record boundary and replay missing events
independently; additions start from the selected seed, and the planned path is faster because the target and its caches
are already prepared.

A node may serve an already accepted local read while another database is replaying, subject to per-database readiness.
It cannot become cluster leader until every database active in the catalog it would select satisfies election or seed
eligibility and its application generation is not below the current floor.

### Checkpoint publication

At a configured event, sequence, time, or byte interval, the leader captures the accepted root with its canonical
sequence, frame-chain hash, and event position, pins the required file set, uploads it, and attempts that database's
state-record CAS. Canonical processing can continue while upload is in progress once the selected file set is safely
pinned. A leader compaction-completion KVI may become part of this ordinary checkpoint manifest, but checkpoint
publication does not push that KVI over live follower sessions. A follower obtains a checkpoint KVI only once, while
opening or rebuilding that database. Later selected checkpoints and their KVIs do not replace the running follower's
bootstrap KVI; a later restart or explicit rebuild may choose one of them as its new bootstrap source. Followers do not
publish competing checkpoints.

### Leader transition

1. Loss of the selected leader session, inability to establish it, or an expired leader-lease observation triggers
   candidacy, not authority. A follower first rereads the leader record so an ordinary leader change becomes reconnect,
   not unnecessary contention.
2. An eligible candidate reconstructs every continued active database to its state-record-selected durable boundary,
   verifies any transition-selected added-database seeds, and discards or parks later old-term roots.
3. It acquires the expired Azure lease and CAS-replaces the shared leader record with its session, term `T + 1`,
   nondecreasing application generation, catalog/transition references, peer endpoint, and new API key.
4. It runs the per-database term-adoption fence for continued instances and conditionally creates additions from the
   selected seeds, while leaving retired states frozen. These independent operations may run in parallel, but all active
   instances must finish before write readiness.
5. It emits a term-start frame for each adopted database, bound to that database's own base.
6. It seeks each upstream event log from that database's accepted event position and recreates discarded events as
   new-term application transactions.
7. It marks the leader endpoint ready and begins serving authenticated sessions with the confirmed cluster term,
   catalog, and per-database progress vector, then becomes write-ready when all active databases satisfy policy.
8. It creates per-database checkpoints before depending on long new-lineage tails for recovery.
9. Only after shared authority and every active database base are confirmed does it start scheduled full compaction.

The graceful `Change Lease` path above replaces the lease-acquisition part of steps 1-3 when the old leader is still
cooperating; per-database adoption is still mandatory.

### Restart or new replica

1. Read `cluster/leader.json`, verify its immutable selected catalog and transition descriptor, compare them with the
   node's own application generation/catalog, and read state only for relevant database instance IDs.
   An older node classifies missing local names as ignored additions and extra local names as retired-volatiles. A newer
   node under an older leader classifies its additions as provisional-local.
2. If a database state names a different term/session, the current lease holder is still activating or has failed.
   The node may retain or prefetch the previous accepted state, but it does not accept current-term frames or report
   current-term readiness until an adopted state is observed.
3. Validate any existing local directory as one complete recovery generation. Every referenced object's identity,
   length and hash, the KVI, descriptor chain, TRL links, and closed-transaction cursor must match durable state. Never
   combine plausible-looking remnants from different local generations.
4. If validation fails for any reason, quarantine or discard that local generation. Read the selected checkpoint
   manifest and transaction-boundary descriptors, then download all referenced files into a fresh temporary directory.
   Existing local files may be reused only after their exact identity and hash are proven.
5. Verify object hashes, sizes, database identity, BTDB format, descriptor linkage, segment continuity, and transaction
   closure before opening the restored database.
6. Replay in the temporary generation to the exact state-selected durable boundary. Require no open transaction at end
   and verify canonical sequence, frame-chain hash, TRL cursor, root identity, and event position.
7. Atomically install the fully verified local generation. A crash before installation leaves it unselected; a later
   restart validates whichever generation is present rather than trusting the rename or directory contents.
8. Replay every post-KVI transaction as an ordinary application transaction. No compaction-specific command or skip path
   exists. Verify that every file referenced by the selected KVI and resulting replay root is present and protected; a
   follower may later seal its local layout with cache-only KVI metadata and run local useless-file cleanup.
9. Read the current endpoint and API key from the leader record, connect the resumable leader session, and start
   proposing the corresponding upstream events from each accepted event position. A provisional addition instead keeps
   processing locally; a retired instance uses only `.temptrl` after reconstructing its frozen base.
10. Become election-eligible only after every database active in the candidate catalog satisfies the canonical lag or
   provisional-seed policy and the node's application generation is at least the selected floor.

If the newest checkpoint is corrupt or unavailable, recovery should follow a retained previous-manifest link or an
append-only manifest history rather than silently starting from an arbitrary mixed file set. If no checkpoint exists,
one elected bootstrap leader creates the canonical history from an empty database and stream origin; followers do not
independently declare their replay canonical.

A missing or corrupt local file discovered while running invalidates the affected local generation. The node stops
reporting that database ready and rebuilds from the selected durable boundary. If the current leader loses any part of
an accepted but not yet object-store-published canonical tail and cannot prove the exact bytes and roots from another
verified source, it self-fences instead of inventing a replacement in the same term; a higher term rewinds to the last
durable boundary and replays retained events. Losing every node's local disk is therefore recoverable under the stated
object-store and event-retention assumptions, while insufficient durable inputs cause explicit unavailability rather
than a corrupted database.

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

## Provider-specific object-storage behavior

Azure Blob and Amazon S3 conditional writes, native Azure leases, throughput concerns, and the concrete Azure
realization of conditional tail append are maintained in [ObjectStorages.md](ObjectStorages.md). The failover and replay
protocols depend on the semantic append contract, not its physical block layout.

## Read-serving and command consistency

Every follower maintains two read boundaries: the leader-confirmed anchor and the newer rollbackable speculative head.
Write transactions always start from the latest speculative head so leader distribution cannot throttle application
execution. Read consistency must be explicit rather than accidentally determined by whichever root current BTDB APIs
return.

- **Local accepted read** returns immediately from the current accepted root and includes its event position and
  canonical identity. It may lag both the leader and the local speculative head. The failover integration needs a way to
  create independent read views from the pinned confirmed root; it must not share one mutable transaction object among
  callers.
- **Optimistic local read** explicitly reads the speculative head and returns its speculative event position plus the
  confirmed anchor it descends from. Its result may disappear after divergence, a compaction rebase, or leader change;
  it must never be presented as canonical.
- **Provisional/retired local read** is explicitly labeled with `provisionalLocal` or `retiredVolatile`, application
  generation, database instance ID, and local event position. It lets the application use a newly added database while
  an old leader remains active, or a removed database on an old follower, but provides no canonical identity or
  durability claim. It may disappear when the database is closed and reopened from the selected leader.
- **Bounded-lag read** is admitted only while the replica is within a configured canonical-sequence/event/time distance
  from leader progress; otherwise readiness fails or the request waits.
- **Minimum-position read** carries a required event position, often returned after accepting a command, and waits or
  redirects until the local accepted root reaches it. This can provide read-your-writes after the event is replayed even
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
useful process-local outcomes only; the selected seed or frozen base, respectively, is the last state with any cluster
meaning.

Because a small direct-stream tail may be rolled back on leader loss, "canonicalized by the current leader" is weaker
than "durably published." The application-level event token remains replayable, but a client that requires no visible
rollback must wait for the durable boundary or use a stronger external acknowledgement contract.

## Expected failure behavior

| Failure | Required behavior |
| --- | --- |
| Several nodes start without local state | Each restores the active instances it hosts from selected state/checkpoints, opens higher-generation additions provisionally, and treats hosted retirements as frozen plus volatile. One eligible candidate acquires the shared leader lease and publishes the new term/catalog, while the others execute events speculatively and follow its frames. |
| Several nodes become candidates together | Independent randomized backoff reduces contention, but correctness comes from Azure granting one leader-blob lease and that holder CAS-publishing its session. Candidates do not need to coordinate with one another. |
| An old leader observes a prepared newer-generation follower | Give that target upgrade-handoff priority. At closed transaction boundaries switch old-leader writes to `.temptrl`, transfer the lease, and let the target atomically select its catalog and exact added-database seeds in term `T + 1`. |
| Several newer followers have independently run the same added database | Only the handoff target's immutable seed named by the new leader record becomes canonical. Every other copy is closed, discarded, restored from that seed, and reopened; never merge or promote it by equality. |
| The selected upgrade target dies before publishing the new leader record | No catalog transition committed. After the transferred lease expires or is reconciled, another eligible newer target may prepare and select its own seed. |
| The selected upgrade target dies after publishing the new leader record but before creating added database state | The immutable transition descriptor fixes the seed and generation. A successor at or above the generation finishes conditional state creation from that seed; it cannot choose its own provisional copy. |
| A newer leader catalog removes a database still hosted by an old follower | Freeze the database's prior canonical state. At the follower's next transaction boundary redirect subsequent work to local `.temptrl`; report only local/volatile progress and never publish or promote it. |
| Every node of the selected newer application generation is unavailable | Older nodes may follow compatible frozen/continued state but are below the persistent generation floor and cannot take leadership. Canonical progress stops until a qualifying node returns. |
| One follower's HTTP path to the leader is partitioned | That follower rereads the leader record, tries to reconnect, and may contend for the lease after backoff. An active lease rejects takeover; connected followers remain on the current leader. |
| The leader is unreachable over HTTP but still renews its Blob lease | It remains leader; transport failure cannot revoke the lease. Followers continue local speculation, may read durable progress from Azure, and lose any lease-acquisition attempts until authority becomes available. |
| The leader pauses beyond its lease safety margin | It self-fences all databases. A successor acquires the expired lease and publishes a higher cluster term; the resumed old session and its frames are rejected. |
| Leadership acquisition or record-update response is lost | The candidate remains inactive until it reconciles both the lease ID and leader-record operation ID, term, and session. |
| A delayed previous-term database-state CAS races activation | The new lease holder remains non-write-ready, rereads the winning predecessor, and retries its adoption CAS. After adoption, requests carrying the old ETag fail. |
| One active database cannot be adopted or created in the new term | The lease holder remains activating and authors no canonical work for any database; it retries safely or releases/expires the leader lease. Retirements are not adoption failures because their states intentionally remain frozen. |
| A follower cannot refresh leader authority | It may keep serving its accepted root and continue non-authoritative local speculation, but freezes new frame acceptance before the conservative deadline. |
| Leader prepares or streams a transaction but dies before publishing database state | Any subset of its file segments may exist in remote objects and replicas, but the next term selects the previous closed state-record boundary. Replicas ignore the unselected transaction, rewind or restore that database, then replay its events. |
| A transaction crosses several TRL files and the publisher crashes after any file operation | The immutable descriptor and database-state CAS are not yet selected, so every prepared segment is ignored. If the CAS landed, all referenced segments were already verified and the complete transaction is selected. No intermediate file boundary is recoverable state. |
| Leader publishes database state but dies before streaming its durability update | The progress remains authoritative and is discovered by the next database-state read or reconnect. |
| Two different frames claim the same database, term, sequence, and base | Freeze that database's acceptance, retain both hashes/peer identities, reread leader and database state, and raise a safety incident. Never choose by arrival order. |
| Shutdown is requested during a persistent application transaction | Stop admitting persistent transactions, let the current transaction finish or abort at its next safe boundary, then switch that database to its volatile `.temptrl` generation. Later application transactions and new value bytes use only local scratch files and perform no canonical or Blob writes. |
| Shutdown begins with Blob publication requests in flight | Dispatch no new data-plane request. Reconcile a previously dispatched database-state CAS using reads; if it landed, it already selects a complete transaction. Ignore unselected prepared bytes. |
| Graceful handoff has no eligible follower | Continue application transactions only against the volatile generation. Do not resume canonical persistence; release or let the finite lease expire at the shutdown deadline, then allow a fully reconstructed node to win normal election. |
| Chosen handoff follower fails before `Change Lease` | Remove it from the eligible set and try another random follower while the shutdown grace budget remains. The old leader may renew the lease and continue volatile execution, but never resumes canonical local or Blob persistence. |
| `Change Lease` succeeds or may have succeeded but the old leader loses contact | The old leader remains permanently non-canonical and performs only volatile `.temptrl` application work. The target proves ownership by renewing with the proposed lease ID before updating the leader record. |
| A handoff target fails after receiving the lease | No other node authors canonical work until the transferred finite lease expires; followers may keep executing locally into speculative cache or provisional-local mode. Normal election then selects a node qualified for every active database and the application-generation floor. |
| The old leader crashes with `.temptrl` files present | Before opening any database, delete every recognized prior-session `.temptrl` generation without parsing it. The successor starts from the final object-store-selected durable base and replays ordered events. |
| Startup cannot remove an abandoned `.temptrl` generation | Keep the database unavailable. Never enumerate, parse, replay, rename, or promote the scratch file as canonical input. |
| Temporary scratch storage becomes full, unavailable, or loses a live `.temptrl` file | Fail the affected local transaction and accelerate process exit. Never fall back to Blob storage and never claim the event as canonical. |
| Leader frame distribution is slower than follower event execution | Continue synchronously committing follower transactions into the local speculative cache with no fixed protocol window. Grow and report the speculative suffix; do not wait for transport or object storage. |
| A follower transaction exactly matches the leader frame | Advance the confirmed anchor to the retained transaction root and release older pins. Perform no BTree replay and no byte copy. |
| Follower application bytes differ from the leader frame | Briefly serialize with local writing, restore the pinned confirmed root, replay the leader transaction, and re-execute the complete dependent event suffix. Report divergence only when logical changes differ. |
| Logical changes match but encoded bytes or value references differ | Treat it as a physical fast-path miss, not split-brain; restore the confirmed anchor and replay canonical bytes before rebuilding the suffix. |
| A node detects leaked exact keys | Keep accepted state unchanged and ask the parent event publisher to publish one bounded ordinary leak-removal event carrying the observed accepted-root identity for diagnostics. |
| Canonical transactions advance after a leak scan | The candidate remains safe because exact keys are never reused. When the later ordinary event is consumed, each handler erases still-present keys and idempotently ignores already-absent ones. |
| Several nodes submit the same or overlapping leak lists | The parent deduplicates exact publication requests when possible. Distinct overlapping events remain correct because normal ordered consumption and non-reuse make later erases idempotent. |
| A leak candidate is malformed, unordered, duplicated internally, hash-invalid, or over budget | The parent event publisher rejects it before publication. Do not partially apply, truncate, reorder, or repair untrusted candidate bytes inside replication. |
| A publication response is lost after requesting a leak-removal event | Reconcile by `publicationRequestId`; do not blindly create another event. If the parent proves no event exists, detection may resubmit or rescan. |
| Leadership changes after a leak-removal event is published | Nothing leak-specific is transferred. The successor consumes the retained ordinary event in stream order and authors the normal application transaction. |
| A leak-removal event arrives while a follower has a speculative suffix | Handle it like any other application event. Its local speculative transaction aligns with the leader frame and follows the ordinary exact-match or rollback/replay path. |
| Startup encounters a committed leak-removal event after its selected KVI | Replay its ordinary application transaction with no special decoder branch. |
| A live compaction control has no local application entry | This is expected because it is out of band. Verify authority, base/layout, chunk order, and PVLs; apply the physical operation or report `retainedOlderLayout` without touching canonical sequence. |
| A live compaction target PVL is missing or corrupt | Do not apply the rewrite root. Refetch the PVL or retain the older valid physical layout and continue application processing. |
| The leader writes a compaction-completion KVI | Do not place that KVI on the live follower stream or mention it in `compactionComplete`. It reaches another node only if a later database open or rebuild selects a checkpoint containing it as that open's bootstrap KVI. |
| A follower restarts after a leader successfully compacted but did not distribute its KVI | Starting from the follower's older KVI naturally retains old references because TRL contains no compaction operation; alternatively restore a newer complete checkpoint. No skip logic runs. |
| The leader restarts after physical rewrite chunks but before its completion KVI | Restore from the selected older KVI and replay ordinary application transactions. Retain source files and let a later full compaction optimize again; no special TRL transaction exists. |
| The leader completes a rewrite while a follower still has a long-running read-only transaction on an old root | Send no deletion instruction. The follower's own root/file pins keep every referenced local source file, independently of `compactionComplete`, until its reader closes and local usefulness analysis proves the file unused. |
| A follower runs cleanup-only compaction | Calculate usefulness against that node's complete open-reader, root, history, and recovery pin set and delete only completely unused files from that follower's local cache. Stop before PVL creation or pointer replacement and make no canonical or object-store call. |
| Follower cleanup finds a local file unused by the current root but required by another pin or its only restartable KVI cut | Keep the file, or first create safe follower-local KVI metadata when allowed. Never damage a retained root; otherwise invalidate and rebuild the disposable local generation rather than guessing on restart. |
| A follower attempts to delete a Blob object | Reject the operation at the authority boundary. Only the current leader's retention/GC state machine may delete remote files. |
| Leadership is lost during compaction | Stop emitting out-of-band rewrite controls and stop remote GC. Never retire leader source files without the completion KVI or Blob objects without a selected durable recovery proof. Already applied physical chunks grant no authority. |
| Graceful shutdown starts during compaction | Request cooperative cancellation immediately. Finish or abort only the bounded chunk already inside the writer gate, stop at that safe boundary, retain sources when no completion KVI exists, and switch application writes to volatile `.temptrl` without waiting for the whole compaction. |
| Two physical checkpoints exist at the same sequence | The already published complete manifest remains selected if identities match; a conflicting hash is a safety fault. |
| Conditional-write response is lost | Read and reconcile the stored operation identity; do not convert the error into a clean rejection. |
| The leader loses any byte of its accepted but unpublished local tail | Stop canonical work for that database and self-fence cluster leadership unless the exact tail can be proven from a verified source. A higher term adopts the previous durable boundary and regenerates application transactions from retained events. |
| Leader compaction or overload delays session heartbeats | Followers may disconnect and begin redundant candidacy, but takeover still requires ownership of the Azure leader lease and a successful leader-record CAS. Lease renewal stays isolated from compaction and peer serving. |
| Leader progress or control messages are duplicated or reordered | Compare term, session, lineage, message sequence, and offsets; accept only current, contiguous progress. |
| The direct stream disconnects | Local follower transactions continue into the speculative cache. Reconnect with the exact confirmed sequence and frame-chain hash; duplicate frames are idempotent and a missing retained range falls back to object storage or restore/replay. |
| A TRL range from the leader is missing or has the wrong hash | Do not use it; request it again, fetch published canonical bytes from object storage, or restore/replay. |
| A process dies after committing BTDB but before committing its broker offset | The event may be delivered again; the cursor inside BTDB recognizes the duplicate without repeating its effects. |
| A broker offset is ahead of local BTDB | Ignore it and seek from the cursor committed inside BTDB; never skip directly to the broker offset. |
| An event position has a gap | Stop local event execution and fetch the missing input; do not invent a transaction for a later event. |
| Azure Blob Storage is unavailable | Current authoritative work may continue only until the conservatively cached authority deadline. Then accepted progress, compaction control, handoff, and election freeze. Followers may continue non-authoritative speculative execution from ordered events because it grants no leadership or canonical progress. |
| All leader HTTP sessions are unavailable but Azure works | The current leader can continue while it renews its Blob lease. Followers continue local speculation, discover authority and durable progress by reading Azure records, and may contend unsuccessfully until the lease becomes available. |
| The event log is unavailable | Replicas serve accepted reads. They may mirror canonical frames, but cannot run the normal application-divergence check for missing inputs; readiness policy must expose that degraded state. |
| Required events expire after the newest usable checkpoint | Recovery has a real unrecoverable gap and must fail loudly rather than initialize incomplete state. |
| Compactor obsoletes files during checkpoint upload | The capture pin prevents deletion until the attempt completes or is abandoned. |
| Garbage collection races restore | Retained manifests remain roots; referenced immutable objects are not deleted. |
| Any subset of local `.kvi`, `.trl`, `.pvl`, descriptor-cache, or metadata files is missing, truncated, stale, or from another generation | Reject the complete local generation, rebuild in a fresh directory from the selected durable graph, and become ready only after full replay and identity validation. Never fill gaps by guessing from surviving filenames or lengths. |
| Every node loses its complete local disk | Restore each database independently from its state-selected checkpoint and closed TRL boundary, then replay retained events. This affects recovery time, not canonical correctness. |
| A state-selected remote object is missing or hash-invalid | Do not open or serve the database. Use a retained earlier complete recovery root plus the full required event range only when that fallback is explicitly described and verified; otherwise fail closed. |
| Process restarts on the same StatefulSet pod | Treat each PVC database directory as a validated cache, reconcile it with that database instance's catalog mode and state record, and rewind, reopen, or discard it before election eligibility. |

## Testing strategy implied by the design

The event-application, follower-session, connected-follower-registry, leader-election, leader/database-state
publication, handoff, recovery, and garbage-collection decision cores must be deterministic and independent of every
concrete I/O adapter. A simulator can then inject:

- arbitrary delays and reordered completions;
- lost success responses;
- clean CAS rejections;
- ambiguous `5xx`, timeout, and connection failures;
- process crash at every awaited operation;
- paused leaders, slow compaction, and skewed clocks;
- independently partitioned follower-to-leader sessions while object storage and the event log remain reachable;
- connection churn, heartbeat timeouts, stale follower status, duplicate/out-of-order control messages, and candidate
  storms;
- lease expiry while an old leader has local commits, direct frames, appends, or uploads in flight;
- previous-term database-state writes racing each new term's adoption fence, including one database that never adopts;
- multiple databases at different sequence, event, upload, and checkpoint lags during election and shutdown;
- shutdown requested at every phase of an application transaction and every cooperative compaction safe point, including
  a rewrite chunk that finishes or aborts while later application transactions continue;
- every graceful-handoff boundary, including a final durable base older than the shutdown canonical cut, target failure
  and reselection without resuming persistence, ambiguous `Change Lease`, lost progress replies, and target death after
  lease transfer;
- overlapping old/new application generations with database additions, removals, unchanged catalogs, and simultaneous
  prepared followers from several higher generations;
- independently advancing provisional copies of each added database, selecting each possible target seed, forcing every
  nonselected follower to discard and reopen, and crashing before and after the leader-record catalog CAS and every
  added-state conditional create;
- old followers switching retired databases at every transaction boundary to `.temptrl`, restarting from the frozen
  canonical base, and attempting unsuccessfully to publish, promote scratch, or regain leadership below the generation
  floor;
- an upgrade handoff while the old process remains alive, proving that continued databases discard their handoff scratch
  and reopen as followers while retired databases remain volatile and additions unknown to that binary are ignored;
- a stepping-down leader continuing application commits through `.temptrl`, including values above the inline limit and
  reads from later transactions, while canonical-file, hard-flush, and object-store adapters fail the test if called;
- the production-shaped temporary-file overlay and deterministic in-memory equivalent using the same shutdown
  transaction semantics and never publishing their bytes;
- volatile allocation after arbitrary persistent file-ID histories, proving collision-free routing and correct reads of
  both pre-cut and post-cut values;
- every outcome of a data request dispatched before the shutdown switch, including an ambiguous database-state CAS that
  is later reconciled as applied or absent;
- scratch-disk exhaustion, live `.temptrl` loss, clean deletion, crash residue, and restart cleanup failure without Blob
  fallback or canonical acknowledgement;
- malformed scratch names and links proving cleanup never escapes the configured failover scratch root;
- stale endpoints, stale/rotated API keys, unauthorized peer requests through both in-process and HTTP adapters, and
  accidental secret logging;
- duplicate, delayed, and missing events;
- arbitrarily long follower speculative suffixes while leader frame delivery is delayed or disconnected;
- exact transaction matches that advance the confirmed anchor without replaying or copying bytes;
- a mismatch at the first, middle, and last unconfirmed transaction, followed by canonical replay and deterministic
  re-execution of every dependent event;
- matching logical changes with different encoded bytes or provisional long-value addresses;
- reconciliation racing an active local writing transaction, with the writer gate entered only at a transaction
  boundary;
- process failure with confirmed and unconfirmed local cache prefixes at every persistence boundary;
- canonical transactions spanning two, three, and many TRL files, with a crash or lost response after every file append,
  file creation, descriptor write, verification read, and state CAS;
- random deletion, truncation, replacement, and cross-generation mixing of every local file type before startup, during
  restore installation, and while a node is running, including simultaneous loss of every node's local disk;
- missing, duplicated, conflicting, truncated, and reordered canonical frames;
- a new term rewinding an old term's directly accepted but unpublished tail;
- leak detection on a node's pinned accepted root while its speculative head continues through many application
  transactions, proving that detection neither observes nor mutates the speculative suffix;
- malformed, unordered, duplicate-key, hash-invalid, empty, and over-budget leak candidates rejected by the parent event
  publisher, including exact boundary values for key count, individual key length, and total encoded bytes;
- ordinary events racing every point between leak scan, publication request, parent acceptance, and later consumption,
  proving that newer positions remain safe, still-present exact keys are removed, and absent keys remain idempotent;
- duplicate and overlapping requests from several nodes, lost/ambiguous publication responses, deduplication, leader
  change, retry, and recomputation, proving that only parent-published events enter recovery state;
- a leak-removal application event interleaved at every point of a long speculative suffix and handled through the same
  exact-match or rollback/replay path as any other event;
- leak-removal application transactions spanning two, three, and many TRL files with a crash at every publication
  boundary, plus startup replay using only the ordinary transaction decoder;
- leader-local and follower-local detection using the same parent-publication port with no replication peer shortcut;
- compaction target PVLs and out-of-band rewrite controls interleaved at every point of a long follower speculative suffix;
- leader full-compaction runs that distribute every required PVL and bounded control while proving that no compaction
  operation changes canonical sequence/frame hash, enters TRL, pushes the completion KVI, or carries a local-delete
  instruction;
- crashes before, during, and after each local/control rewrite chunk and immediately before or after leader completion-KVI
  creation and checkpoint publication;
- follower startup replay from older and newer KVIs after incomplete and successful compactions, proving that the TRL
  contains only ordinary application transactions and no compaction skip branch runs;
- followers missing, rejecting, duplicating, or reordering compaction controls while application frames continue,
  proving that retaining the older physical layout does not block logical convergence;
- leader compaction completion while different followers hold old roots in long-running read-only transactions,
  proving that each node retains and later reclaims different local source-file sets without any distributed deletion;
- initial follower open and later live checkpoints/compactions, proving that exactly one selected bootstrap KVI is
  transferred for that open and that no subsequent KVI appears on the live follower stream;
- follower cleanup stopping exactly before PVL creation and pointer replacement, with files referenced by confirmed,
  speculative, reader, history, and recovery pins protected under every interleaving;
- follower-local KVI creation, deletion, restart, and later leadership, proving that its cache-only identity never
  collides with leader-assigned canonical TRL/PVL IDs and is never streamed or uploaded;
- attempted object-store deletion from every follower and stale leader path, proving that only the current leader's
  remote-GC state machine can delete a Blob and that retained manifests prevent premature deletion;
- slow peer consumers, disconnect/resume, bounded queues, HTTP proxy buffering, and unavailable retained ranges;
- stale local caches;
- partially uploaded checkpoints and missing listings;
- compaction concurrent with restore and garbage collection.

Safety assertions should include:

- every API and metric labels the confirmed anchor separately from the rollbackable speculative head; no optimistic read
  is reported as canonical;
- follower application commit completion never depends on a peer, object store, lease, timer, or background
  confirmation queue;
- after each shutdown canonical cut, application transactions continue against the volatile BTree head and all newly
  written values remain readable from `.temptrl` while no canonical-file, hard-flush, or remote data-plane write occurs;
- every volatile value reference resolves to the scratch generation, every pre-cut reference still resolves to its
  original file, and volatile file IDs can neither collide with persistent IDs nor escape into canonical state;
- a stepping-down leader's only post-cut Azure mutations are lease authority operations needed to preserve or transfer
  fencing; it never appends TRL, updates database state/checkpoints, writes PVL/KVI, or deletes an object;
- volatile shutdown transactions and `.temptrl` bytes never receive canonical identities, advance confirmed/durable
  state, emit canonical frames, or become a handoff prerequisite, and dropping the whole generation followed by event
  replay is deterministic;
- startup deletes every recognized prior-session `.temptrl` before constructing the canonical file collection;
  no scratch byte is ever parsed, replayed, renamed, or promoted;
- application generation in the leader record never decreases, and a same-generation catalog-hash conflict always
  fails closed;
- the leader is not write-ready until every active continued database is adopted and every active addition is created
  from the exact transition-selected seed; retired state is never adopted into the new term;
- only the selected target's provisional seed can establish a new database's sequence-zero genesis; every other
  provisional local generation is discarded and reopened without merge or equality promotion;
- all post-retirement writes from older applications stay in `.temptrl`, while the last canonical database state and
  retained remote closure remain unchanged;
- every unconfirmed transaction boundary needed for zero-work confirmation or rollback remains pinned until it is
  confirmed or its suffix is abandoned;
- an exact transaction match advances only confirmation metadata/root retention and performs no BTree replay;
- an application's mutation and event cursor are either both in one canonical transaction or neither is;
- every canonical BTDB transaction corresponds to exactly one ordinary application event; term-start, compaction,
  artifact, checkpoint, KVI, and file-retirement operations never consume canonical transaction sequence;
- canonical sequence and application event position advance together without a replication-specific transaction kind;
- leak detection and parent-publication requests never erase a key, advance an accepted root, or create canonical state;
- leak-removal exact keys are never reused after deletion, so scan/publication delay cannot make an old event name new
  data and already-absent keys are idempotent;
- only consuming the parent-published ordinary event can erase leaked keys, using the same writer transaction that stores
  its event position on every replica;
- every direct frame and durable boundary ends after a complete commit command; an `EndOfFile` marker or new TRL header
  never confirms a transaction;
- a transaction spanning multiple TRL files advances durable state through one descriptor-selecting CAS or not at all;
  physically visible but unselected segments never affect recovery;
- duplicates do not repeat effects and gaps never advance accepted state;
- replicas at the same canonical identity have identical logical content and every value reference resolves to
  identical bytes; physical coordinates may differ through out-of-band compaction while both layouts remain valid;
- an application mismatch always restores the pinned confirmed root, discards the entire dependent speculative suffix,
  converges by canonical replay, and re-executes later ordered events from the corrected head;
- unmatched follower-produced cache bytes are never published as canonical;
- an out-of-band compaction control has no application entry, never raises application divergence, and never changes
  canonical sequence or frame-chain identity;
- a leak-removal event does have a normal follower application entry and uses ordinary divergence comparison;
- a live compaction root cannot publish pointers into a target PVL before that PVL is complete and verified;
- live compaction distribution contains sealed PVL prerequisites and out-of-band bounded controls but never a TRL
  transaction, the leader's completion KVI, or a command to delete follower-local files;
- a follower cleanup-only compactor never creates a PVL, rewrites a BTree pointer, authors a transaction, publishes a
  checkpoint, or deletes an object-store file; every file it removes is completely unused by all protected local roots
  and recovery cuts, including every currently open read-only transaction;
- an already running follower never receives a replacement KVI; only database open or rebuild transfers one selected
  bootstrap KVI, after which physical changes arrive only as PVL artifacts and out-of-band rewrite controls;
- no leader message or compaction result authorizes follower-local deletion, because only that follower can evaluate its
  complete live-reader and retained-root pin set;
- leader-local source files cannot be retired before a valid completion KVI seals that layout, and remote source objects
  cannot be deleted before a selected durable checkpoint plus retained-manifest analysis proves them unreachable;
- only the current leader can execute object-store garbage collection; a follower or stale leader has no deletion
  authority even when it has already removed the equivalent local file;
- shutdown cancellation stops compaction no later than the next bounded writer-gate boundary and never requires
  application transaction execution to wait for the overall compaction;
- startup replays only ordinary application transactions after its selected KVI and has no compaction command or skip
  path, regardless of whether the leader completed a later physical compaction;
- HTTP connection state, disconnect, follower status, and handoff messages alone never authorize leader work;
- exactly one session holds the leader-blob lease and is named by the matching leader record before any database
  accepts its frames;
- the current lease holder authors no canonical frame until every active database state has been adopted or created by
  its term/session and selected catalog;
- a successful adoption CAS prevents every already-issued prior-term state update from publishing with its old ETag;
- only that cluster-wide session can author accepted frames or advance checkpoints for any database;
- every takeover strictly increases the shared term, selects one durable base independently for each database, and
  rejects old-term frames;
- two conflicting frames for one term/sequence/base always cause a safety stop;
- accepted per-database remote TRL boundaries contain contiguous, ordered, checksum-valid segments and end with no open
  transaction;
- checkpoint sequence is monotonic and always resolves to a complete, checksum-valid manifest;
- only the lease- and CAS-confirmed cluster leader starts full pointer-rewriting compaction; followers may run only
  local cleanup-only compaction;
- every restored BTDB cursor, sequence, frame hash, and event position exactly match its manifest;
- every accepted local generation is fully hash- and identity-validated against one durable recovery graph; arbitrary
  surviving local fragments are never combined into inferred state;
- leader-owned remote garbage collection never removes an object referenced by a retained recovery root;
- ambiguous lease changes, leader-record updates, database-state updates, and TRL appends are reconciled, never guessed;
- a replica with a gap, corrupt artifact/checkpoint, stale authority, or unrepaired divergence never reports ready.

Core performance tests must compare the pre-change and post-change standalone `BTreeKeyValueDB` hot paths, including
small inline values, large TRL-backed values, point updates, range erases, commit latency, throughput, allocations, and
database-open replay. Failover benchmarks are useful too, but may not excuse a regression when failover is disabled.
Follower-mode benchmarks must additionally prove that disconnected or slow leader transport does not change local
transaction throughput beyond the configured synchronous cache/index work.

Each test scenario must be runnable with multiple complete nodes inside one process, virtual time, in-memory files,
in-memory object storage, in-memory event input, and independently faulted follower-to-leader links. The same transport
conformance suite must run against both the in-process transport and an actual loopback Kestrel adapter. The
deterministic safety suite may not rely on `Thread.Sleep`, real ports, or process-global singleton state; separate
adapter integration runs may bind ephemeral ports.

Provider integration tests must run against real Azure Blob Storage in addition to Azurite, including the conditional
atomic-tail-append contract, multi-file transaction preparation followed by one boundary CAS, finite lease
acquire/renew/change/release, ambiguous-response reconciliation, and the four-step CAS probe. S3 endpoint qualification
belongs to the later S3 adapter.

## Open decisions

1. What exactly is an event position: one integer, Kafka topic/partition/offset, or an application sequence plus a
   stream identity and generation?
2. Does version one require one partition per logical database, or will an upstream component provide a deterministic
   total order across partitions?
3. How does each replica receive the full stream, and which component assigns unique consumer groups or performs
   explicit seeking?
4. Does `BTDB.Replication` own event consumption, or expose an async transaction callback that validates and stores the
   caller's event identity with its mutation?
5. What is the smallest opt-in BTDB API for speculative commit capture, historical-root pinning, confirmation-anchor
   advancement, committed-suffix rollback, canonical replay, and checkpoint pinning without adding work to standalone
   transactions?
6. Which exact bytes and metadata constitute transaction equality, and how is comparison aligned with the one ordinary
   event identity that produced each canonical transaction?
7. What local file/cache layout lets an arbitrarily long speculative suffix retain large values, exact transaction
   boundaries, and root pins without requiring a fixed in-memory window?
8. What is the canonical logical-change encoding? Must it compare exact operation order as well as final state, and
   which hash plus bounded diagnostic diff is appropriate?
9. Where is canonical transaction sequence persisted in BTDB, and what local counter semantics avoid confusing reused
   `TransactionId` values across abandoned speculation generations without changing standalone behavior?
10. What exact fields constitute `baseLogicalStateIdentity`, and what separate physical-layout identity is sufficient
    to precondition a compaction rewrite without a full-tree hash on every transaction?
11. Can root pins be coalesced while still making every exact-match confirmation a no-replay operation, and how are
    abandoned speculative cache generations reclaimed after readers and repairs release them?
12. What is the exact versioned encoding, chunk hash/order, retry/resume rule, and size limit for out-of-band compaction
    controls, and which physical-layout identity lets an incompatible follower safely retain its older layout?
13. What exact canonical-ID allocator covers leader-authored `.trl` and `.pvl` files, and what disjoint local namespace
    or mapping lets followers create cache-only KVI files without colliding after later leadership?
14. Besides the mandatory current confirmed anchor and unconfirmed transaction boundaries, how much older accepted-root
    history should each node retain, and when is full checkpoint restore preferable?
15. Is version one always allowed to discard the unpublished predecessor tail, or should a candidate be able to prove,
    upload, and adopt an identical tail obtained from peers before takeover?
16. What conservative follower cache deadline around the finite Azure Blob lease guarantees that a follower stops
    accepting old-term frames before a successor can become active under pauses, long requests, and partitions?
17. How frequently must the leader Blob lease be renewed independently of transaction rate, and what happens to an
    in-progress leader commit at the local self-fence boundary?
18. What does command acknowledgement mean: event-log accepted, leader canonicalized, object-store durable, or
    observed by a requested set of replicas? What rollback can each level expose?
19. What readiness and read contracts are required for the confirmed anchor versus the speculative head: exact
    canonical catch-up, bounded lag, explicitly rollbackable optimistic reads, or a caller-supplied minimum position?
20. After a repaired application mismatch, should the node remain ready with an alert, enter degraded readiness, or
    require operator acknowledgement?
21. What exact frame size, heartbeat, backpressure, retention, range-fetch, reconnect, and follower-acknowledgement
    semantics belong to the transport-neutral peer contract?
22. What exact shared wire codec and conformance suite must the in-process and HTTP/Kestrel adapters both implement?
23. What exact registration, heartbeat, status cadence, connection-replacement, and stale-message rejection semantics
    should the leader-centered follower session use?
24. How aggressively should a disconnected follower reread the Azure leader record, retry the same endpoint, and
    contend for the lease, and is the shared per-term API key sufficient or is per-node authentication also required?
25. How caught up must every continued active database be, and how fresh must each provisional seed be, before a node is
    election-eligible? May an unplanned candidate acquire the leader lease while one database still needs replay?
26. What exact validation makes the split leader record and per-database state records safe despite the absence of a
    multi-object Azure transaction, especially while term `T + 1` is starting each database independently?
27. Should version one upload sealed whole BTDB files, immutable chunks of active `.trl`, or both behind one manifest?
28. What conditional-append batch size and maximum delay balance replay lag, graceful-handoff latency, storage request
    cost, and ambiguous-append reconciliation?
29. How often are TRL batches/checkpoints published, how large may the leader and follower rollback tails become, and
    what local-storage, root-retention, replay-time, and event-retention budgets constrain operations without imposing a
    protocol-level speculative window?
30. How many previous manifests are retained, and what is the garbage-collection proof for old terms, attempts,
    `.kvi`, `.pvl`, `.trl`, direct-range retention, and concurrent restores?
31. Should leader and follower canonical mirrors hard-flush each transaction, or is local replay after process loss
    acceptable for each role independently?
32. Can `BTDB.AzureStorage` transfer code be reused without inheriting unconditional overwrite and asynchronous
    `HardFlush` semantics?
33. What happens during storage-account regional failover, and which Azure redundancy modes satisfy the intended
    leadership and checkpoint availability objectives?
34. Which metrics are contractual: speculative/confirmed/durable sequence and event position, speculative suffix
    count/bytes/age, frame hash, source and leader lag, exact-match/fast-path-miss/divergence/repair counts,
    leader/term/lease margin, stream reconnect, checkpoint age, compaction state, CAS ambiguity, and GC backlog?
35. What exact independent version ranges cover the peer protocol including compaction controls, checkpoint format,
    catalog format, and seed manifests so an incompatible reader fails safely without changing ordinary BTDB TRL?
36. What exact syntax and maximum length define a database short name and database instance ID?
37. What shutdown grace budget and eligibility threshold make random graceful handoff reliably faster than lease
    expiry when the durable-base vector may trail the shutdown canonical cut, and should a failed prepared target be
    temporarily penalized before another random selection?
38. What exact immutable boundary-descriptor encoding efficiently represents whole transactions spanning many TRL files,
    and how are descriptor chains compacted without weakening transaction-aligned recovery?
39. What failover-mode integrity metadata and validation cadence detect missing or truncated local files while a process
    is running without adding cost to standalone `BTreeKeyValueDB`?
40. What exact per-node/session scratch-directory layout, `.temptrl` naming, file-ID namespace, quota, and deletion
    protocol keeps temporary files disjoint from canonical allocation and makes startup cleanup safe and bounded?
41. What exact local commit result, optimistic-read contract, and readiness state should a stepping-down process expose
   for its volatile generation so callers cannot mistake it for leader-canonical or durable progress?
42. How is the per-database shutdown-cut vector captured when databases reach their writer boundaries independently, and
   what is the maximum allowed transition latency for an unusually long application transaction?
43. Beyond authentication, non-reuse, and structural validation, should the parent event publisher require a compact
    reachability proof, independently re-run detection, or trust compatible non-Byzantine detectors?
44. What exact key encoding version, maximum key length/count/encoded bytes, transaction-time budget, and rescan cadence
    keep one ordinary leak-removal event bounded, and may the parent split one candidate into several ordered events?
45. How are leak-detector versions qualified across rolling upgrades, which accepted roots are old enough to scan, and
    how are duplicate scans scheduled across nodes without turning scheduling into a correctness dependency?
46. Which exact checks and optional local-KVI step form the node-local cleanup boundary, especially for a completely
    unused TRL whose deletion would invalidate that node's previous restart cut, while guaranteeing that open read-only
    transactions remain pinned without any leader-provided delete decision?
47. After leader full compaction creates its completion KVI, should ordinary checkpoint publication run immediately or
    on its normal schedule, and how long must old Blob objects remain retained before that KVI is selected durably?
48. What remote-GC authorization token and manifest-reachability proof ensure that only the current leader can delete
    Blob objects and that a delayed old-term deletion request cannot succeed after takeover?
49. Who allocates and persists `applicationGeneration`, and what deployment validation prevents two independently built
    releases from accidentally reusing one generation with different catalogs before they connect to the cluster?
50. What exact provisional seed checkpoint API captures application event position, logical identity, BTDB format, and
    a closed file graph without pausing local use longer than one bounded transaction boundary?
51. What retention period and explicit administrative operation eventually delete a retired database's frozen remote
    closure while preventing an old still-running follower or delayed restore from treating deletion as ordinary cache
    loss?

## Preliminary direction

The current Azure-first direction is:

- one failover cluster may contain multiple short-named and instance-identified `BTreeKeyValueDB` databases, with an
  immutable catalog and monotonic application generation selected by the leader record;
- heterogeneous database sets during rolling upgrades: additions run provisionally until one prepared newer target's
  seed becomes canonical, removals freeze canonical state and continue on older followers only in `.temptrl`, and the
  selected generation becomes a persistent no-downgrade election floor;
- one retained, totally ordered application event stream per logical database;
- one cluster-wide leader as the sole canonical TRL author and full-compaction
  initiator for every database active in its selected catalog;
- one small `cluster/leader.json` block blob containing term, session, application generation, immutable catalog and
  transition references, transport-defined peer endpoint, and a rotating API key, protected by ETag CAS and one finite
  Azure Blob lease;
- one global leadership term but independent canonical sequence, event position, checkpoint, and replay progress for
  each database;
- canonical BTDB TRL containing only ordinary transactions produced by consuming application events, with no
  replication-specific command or transaction kind;
- one CAS state record and one term-qualified remote TRL lineage per database;
- batched conditional atomic tail append using the previously observed opaque token and length, with one immutable
  multi-file descriptor and database-state CAS selecting only a complete transaction boundary while provider layout
  stays hidden behind the storage adapter;
- an arbitrarily long follower-local speculative transaction suffix, committed synchronously to local cache without
  waiting for leader, transport, or object storage, and rooted at a pinned leader-confirmed read-only boundary;
- transaction-by-transaction exact comparison: identical local bytes advance the confirmed anchor with no BTree work,
  while the first difference restores that anchor, replays leader bytes, and re-executes the dependent event suffix;
- leader-defined canonical TRL bytes mirrored on followers or satisfied by proven byte-identical local cache bytes, with
  every referenced TRL/PVL byte available before a canonical root uses it;
- leak detection allowed on any node against a pinned accepted root; it asks an injected parent-system port to publish a
  bounded ordinary application event, and only consuming that event removes still-present exact keys on every replica;
- the leader runs full normal compaction and distributes sealed PVLs plus bounded pointer-rewrite controls out of band
  over follower sessions, without writing compaction into TRL, consuming canonical sequence, distributing local-file
  deletion, or pushing a later KVI;
- one selected checkpoint KVI is transferred when a follower opens or rebuilds a database. It remains that open's
  bootstrap KVI; later leader-created KVIs never travel on the live stream, although a future restart may bootstrap from
  a newer selected checkpoint;
- follower compaction runs only node-local completely-unused-file cleanup and stops before PVL creation or pointer
  replacement. It protects that follower's open read-only transactions and retained roots, receives no deletion decision
  from the leader, and any follower-local KVI is cache-only with a non-canonical identity;
- Blob/object-store file deletion is performed only by the current leader after selected-checkpoint and retained-manifest
  proofs permit remote garbage collection;
- an opt-in BTDB path for synchronous local speculative capture, root pin/restore, and asynchronous reconciliation, with
  the existing standalone hot path and transaction format unchanged;
- isolated leader-lease renewal, conservative self-fencing, and follower validation against the Azure leader record;
- fast graceful shutdown without application quiescence: close each persistent canonical generation at a transaction
  boundary, continue subsequent writes in disposable local `.temptrl` files so new values remain readable, cancel
  compaction cooperatively, dispatch no further canonical-file or remote database persistence, randomly prepare an
  eligible follower, and transfer only the Azure authority lane with `Change Lease`;
- a leader-centered topology in which followers discover the leader from Azure, maintain authenticated HTTP sessions,
  report progress to that leader, and contend for the lease only after their leader session is unavailable;
- a transport-neutral peer streaming/RPC boundary with deterministic in-process routing, plus a resumable binary HTTP
  adapter mapped into the parent application's Kestrel pipeline; both use the same codec, authorization, resume, and
  failure semantics, with object-store range and checkpoint fallback;
- explicit injected ports for storage, leases, event input and parent event publication, time, scheduling, randomness,
  identity, local files, lifecycle, and leader-centered peer communication, with no ambient process-global dependencies
  in the protocol core;
- local disks treated only as validated disposable caches: arbitrary partial loss triggers fail-closed rebuild, and loss
  of every local copy remains recoverable from a closed object-store boundary plus retained ordered events;
- term-qualified data lineages so stale leaders cannot contaminate successor state;
- a deliberate ability to rewind an unpublished leader tail and replay its upstream events after takeover;
- a complete immutable checkpoint manifest selected by each database's state-record CAS;
- sealed or pinned immutable BTDB files for checkpoints, alongside the conditionally appended active TRL;
- no Azure request per transaction unless batch size is explicitly configured as one;
- Deployment support as the correctness baseline, with StatefulSet volumes only as validated canonical caches;
- reads served from accepted roots on sufficiently caught-up replicas, with explicit term/sequence/event semantics;
- real-provider CAS capability probes and standalone BTDB performance gates before production use.

Amazon S3 remains research for a later provider adapter. Its lack of ordinary append and a native finite object lease
means it must not constrain or complicate the first Azure implementation.
