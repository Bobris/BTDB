# BTDB.Replication

Status: Architecture design; no implementation has started.

`BTDB.Replication` is a planned high-availability layer for running one or more logical BTDB databases on multiple
compute nodes. It combines a single canonical database history in object storage with fast disposable local caches on
the nodes. An ordered upstream event log remains the source for replaying a recent tail that has not yet been published
to object storage.

## Why this design

### Split-brain safety by construction

Only one node may author the canonical database history. A node becomes leader only after acquiring a finite,
storage-enforced lease and publishing a new monotonic leadership term through compare-and-swap. Losing contact with the
leader does not grant authority to a follower, and nodes reject work from expired or older leadership terms.

The protocol therefore fails closed: when authority cannot be proven, canonical progress stops instead of allowing two
independent histories to develop. A network partition may reduce availability, but it cannot give two nodes valid
authority or create two accepted versions of the same database.

### One shared durable database, independent of node count

The cluster stores one shared canonical dataset in object storage rather than one complete durable database copy per
node. Checkpoints, transaction-log generations, and retained recovery artifacts may coexist, but they all belong to the
same database history. A node's local BTDB files are only a validated, disposable cache and never become a separate
source of truth.

Adding a replica therefore does not multiply the durable object-storage capacity required by the database. Storage
cost and remote garbage collection are primarily functions of database size and retention policy, not replica count.

### Disposable compute without mandatory persistent volumes

A new or replaced node can reconstruct its local cache from the shared checkpoint, the canonical transaction-log
boundary, and retained ordered events. Stable local volumes can improve restart time, but they are an optimization, not
part of the ownership protocol. This makes ordinary deployment replicas, autoscaling, and node replacement natural
operational choices.

### Fast local application processing

Followers execute ordered application events and commit them synchronously to their local speculative cache without
waiting for the leader, object storage, or acknowledgements from other replicas. When leader TRL arrives, followers compare decoded structure over the same event range,
ignoring different batching and physical framing. A match advances confirmation metadata without changing the BTree.
A structural mismatch restarts the follower and rebuilds it from canonical state.

All replicas may batch independently. Each event ends with an in-transaction TRL cursor marker, allowing comparison
to ignore transaction grouping. Replication retains no historical BTree roots and
performs no live suffix repair. Pending comparison data can stay on disk. A follower whose current tree is ahead of
confirmation serves speculative reads; it has no separate older confirmed snapshot.

A failed batch on any replica is retried event by event. An individually failing handler is rolled back and followed by an
ordinary metadata-only commit advancing its event cursor. Followers replay that skip without application-data changes.
Failed-attempt bytes and mutations are never promoted. Optimistic TRL stays separate; on takeover, the new leader
appends only validated complete events after the adopted canonical end, without copying overlaps or rerunning their
handlers. If that end lies inside a local batch, the remaining events receive valid new transaction framing.

Replication latency therefore does not normally become application-transaction latency, while the accepted state still
converges to one leader-authored sequence.

### Read and failover capacity without duplicated ownership

Additional nodes can provide read capacity and warm failover candidates while authority remains centralized and
explicitly fenced. Replicas may be temporarily at different confirmed positions, but two ready replicas at the same
canonical position must expose the same logical state.

### Centralized compaction and recovery

Only the leader runs and distributes full pointer-rewriting compaction or deletes objects from shared storage. The
physical rewrite travels out of band over follower sessions rather than adding a BTDB transaction kind. Logical cleanup,
such as leak removal, is published by the parent system as an ordinary ordered application event. Local-file deletion is
never distributed: every node independently protects files referenced by its own open readers and reclaims only its own
proven-unused disposable cache files. A follower receives one KVI when opening or rebuilding the database; later KVIs
created by the leader are not pushed to the running follower. This avoids unsafe cross-node cleanup decisions and
per-node durable histories that would otherwise have to be reconciled after a failure.

Object-store publication advances only at complete BTDB transaction boundaries. Missing, stale, truncated, or mixed
local files cause validation and rebuild or fail-closed unavailability; they are never used to infer canonical state.

### Rolling application upgrades can change the database set

A newer application generation may add or remove logical databases without requiring every replica to change at once.
While an older leader is active, a new database runs provisionally on each newer node. The older leader gives priority
to a prepared newer node, which initializes added databases with an empty transaction and starting event cursor;
all newer nodes discard their provisional copies and reopen canonical state. If initialization was never published,
a successor initializes that database from scratch.

When the newer database set removes a database, old nodes continue independently from their own local views in
disposable `.temptrl` files until shutdown. No exact final shared position is required, and database names are never reused. The leader record retains
a monotonic application-generation floor, so an older binary can keep following compatible databases but cannot later
be elected and resurrect a retired database.

Remote cleanup does not wait for followers restoring older checkpoints. After a complete replacement checkpoint is
published, obsolete remote files may be deleted immediately. A follower whose startup loses a file restarts and loads
the newest published KVI; running replicas retain local files according to their own reader lifetimes.

## Design documents

- [Architecture](Architecture.md#how-to-use-this-specification) maps the normative invariants, shared identities,
  database-state publisher, transition engine, follower acceptance, and recovery rules.
- [Open decisions](Architecture.md#open-decisions) separates implementation blockers from integration and policy choices.
  The proposal's safety and availability goals still require these mechanisms and their fault tests.
- [Object storage research](ObjectStorages.md) describes the provider-neutral storage contract and the Azure-first
  implementation direction.
