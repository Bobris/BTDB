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
waiting for the leader, object storage, or acknowledgements from other replicas. When the leader's canonical transaction
arrives, an exact match confirms the already computed local root without replay. A difference causes rollback to the
last confirmed root followed by deterministic replay.

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

### Rolling application upgrades can change the database catalog

A newer application generation may add or remove logical databases without requiring every replica to change at once.
While an older leader is active, a new database runs provisionally on each newer node. The older leader gives priority
to a prepared newer node, whose exact provisional snapshot becomes the one canonical genesis during the fenced handoff;
the other newer nodes discard their provisional copies and reopen from that selected state.

When the newer catalog removes a database, its last canonical state is frozen instead of being advanced by an old
binary. Older followers that still use it continue only in disposable local `.temptrl` files. The leader record retains
a monotonic application-generation floor, so an older binary can keep following compatible databases but cannot later
be elected and resurrect a retired database.

## Design documents

- [Architecture](Architecture.md) describes the replication, failover, recovery, and testing model.
- [Object storage research](ObjectStorages.md) describes the provider-neutral storage contract and the Azure-first
  implementation direction.
