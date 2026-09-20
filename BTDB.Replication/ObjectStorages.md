# BTDB.Replication Object Storage Research

Status: Provider research plus an implemented provider-neutral file inventory boundary; production Azure transport remains pending.

The `denoland/celld` research snapshot is 2026-08-29 and is pinned to release `v0.4.0`, commit
`a52f9905425bc41134d817694bdc2c50bcc5e856`. Azure details were rechecked on 2026-08-30. Provider behavior and limits
must still be requalified against the selected account type, endpoint, SDK, and service version before production use.

This document contains the object-storage abstraction, the `celld` reference study, and provider-specific details.
The failover state machine, canonical TRL protocol, and checkpoint design remain in [Architecture.md](Architecture.md).

## What BTDB.Replication needs from object storage

The object store has three distinct roles:

- **Coordination plane**: one small shared leader record selects the cluster-wide leader and publishes its
  transport-defined peer endpoint and API key. Azure version one protects it with ETag CAS and one finite native Blob
  lease.
- **Per-database publication**: CAS on canonical TRL directly publishes complete transactions. A native KVI
  is published after all its prerequisite files; no separate checkpoint pointer or manifest selects it.
- **Data plane**: active TRLs are logically append-only, term-qualified remote files; native KVIs and
  compaction artifacts are immutable or attempt-qualified. Takeover fences the writable predecessor TRL by CAS before using a new term-qualified continuation.

The upstream event log permits loss and deterministic replay of a small unpublished application tail. Object storage
therefore does not need to prove durability before every event is acknowledged, but it must never allow two accepted
canonical histories or an accepted prefix ending inside a transaction.

### Implemented file inventory boundary

`IRemoteFileCollection` exposes asynchronous inventory enumeration, version-bound range reads and fresh remote ID
reservation. `RemoteFile` describes the numeric ID, native type, length, opaque version, sealed state and optional
whole-file SHA-256. The adapter must reject a read if the selected version changed or disappeared; partial range
success must never silently combine versions. Reservation uses remote inventory and authority, never a local maximum,
and must preserve non-reuse across retirement and reconcile uncertain outcomes before returning an ID.
`ICheckpointStorage` extends that boundary with confirmed PVL/TRL prerequisites and native KVI publication.

`ReplicationFileSet` implements `IFileReplicatedCollection` exposed to BTDB. `GetCount`, `GetFile`, and `Enumerate`
operate only on physical local cache/storage; `GetRemoteCount`, `GetRemoteFile`, and `RemoteEnumerate` expose the
selected remote inventory after initialization. Remote handles are read-only and version-bound. Neither inventory
lookup nor local lookup downloads a body. Local additions/deletions leave the remote inventory unchanged. After a complete successful listing, initialization removes local files without a selected remote mapping and retains
only mapped files whose extension, then length, then locally calculated SHA-256 match remote metadata. Invalid or unverifiable
cache files are removed without downloading replacements; prefetch reuses validated files without hashing again.
The collection uses `IKeyValueDBLogger` (the same instance may be passed to `KeyValueDBOptions.Logger`) to report
why each local file is removed, including local/remote IDs and mismatched metadata. Downloads retain canonical native
filename extensions so a verified cached file can pass subsequent startup validation. `GetLocalFileId` exposes memory-only session assignments; a fresh session loses
different-ID placements and may remove/redownload such cached files. KVI recovery translates PVL references to local
IDs. `PublishPureValuesAsync` is the leader checkpoint path for unmapped sealed local PVLs; it records confirmed
placements and retains reserved remote IDs for retries. Failed discovery does not delete local files; repeated initialization preserves new
session-local files. The owner awaits `InitializeAsync` before calling `BTreeKeyValueDB.OpenAsync`. Open and prefetch never initialize
implicitly; remote inventory/metadata access fails until initialization succeeds. Standalone constructors retain their original
synchronous opening and eager metadata loading. File IDs and types are parsed from numeric filenames and extensions;
higher fileIds identify newer TRLs and KVIs within each sequence, without using native generation. Inventory discovery reads no bodies; lazy native metadata reads use small version-bound ranges.
`PrefetchAsync(fileId)` shares one bounded cache-population transfer between callers, verifies cache candidates and
uses exact-ID imports internally. The checkpoint publisher uses the same verified PVL placements. Matching numbers or lengths do not imply equal bytes. Tests reproduce distinct local/remote contents
under the same ID, remote allocation independent of a higher local maximum, out-of-order downloads, version changes,
checksum failures, cancellation and retry. This is the concrete reason for separate inventories and exact imports.
The checkpoint/receipt lifetime rules remain owned by [Architecture.md](Architecture.md); this interface is not an
Azure transport or a complete startup/authority implementation.

### Minimum coordination properties

An endpoint is suitable only if it provides all of these properties:

1. **Conditional create**: create key `K` only when no current object exists at `K`.
2. **Conditional replace**: replace key `K` only when its current opaque version token equals the token returned by a
   preceding read.
3. **Strong read-after-write consistency**: after a successful write, subsequent reads observe that write.

No part of the design may assume an atomic transaction across two object keys. The leader record grants authority but
contains no database progress. Required files must be completed before the canonical TRL CAS exposes a complete
transaction referencing them. The TRL operation itself establishes durability; no separate state CAS follows it.

### Candidate provider-neutral interface

This is a semantic sketch, not a proposed C# API:

```text
Read(key)
    -> Found(body, token, metadata) | NotFound | Failed(error)

Head(key)
    -> Found(length, token, metadata) | NotFound | Failed(error)

CreateIfAbsent(key, body, operationIdentity)
    -> Applied(newToken) | Rejected | Ambiguous(error)

ReplaceIfCurrent(key, expectedToken, body, operationIdentity)
    -> Applied(newToken) | Rejected | Ambiguous(error)

PutImmutable(key, body, contentHash)
    -> Applied(token) | AlreadyPresentAndEqual(token) | Conflict | Ambiguous(error)

GetRange(key, offset, length)
    -> Found(bytes, objectLength, token) | NotFound | Failed(error)

ListPage(prefix, continuationToken, limit)
    -> Page(objects, commonPrefixes, continuationToken) | Failed(error)

DeleteMany(keys)
    -> PerKeyOutcome[]

ProbeConditionalWrites()
    -> Conformant | Violation(reason) | Inconclusive(error)
```

The coordination core needs only `Read`, `CreateIfAbsent`, and `ReplaceIfCurrent`. `Head`, range reads, paginated
listing, bulk deletion, and multipart upload support checkpoint transfer, restore, and garbage collection. Listings
discover published native KVIs. Validate their native file references and canonical ancestry; listing alone does not
prove arbitrary files belong to a committed history. No separate checkpoint manifest is required.

`DeleteMany` is exposed only to the current leader's remote-garbage-collection state machine. Followers may delete
completely unused files from their own local BTDB file collections, but never call object-store deletion. Such local
deletion is never distributed: only the node can see all files pinned by its open read-only transactions and retained
roots. Leader authority alone is not a deletion-safety proof because the lease on `cluster/leader.json` does not
physically fence a previously dispatched request to another Blob. Every remote deletion candidate must therefore be
absent from the currently published recovery closure, and its key must never be reused. Publish the replacement
required value/log files first and the KVI last before superseded objects become deletion candidates. Plan a configurable
operational delay of about one day from obsolescence before actual deletion, without tracking follower restores. A follower
losing a file while opening restarts and loads the newest published KVI. Old restore attempts and diagnostic KVI references
do not pin remote files; a delayed old-term delete must remain harmless to the current closure.

The required version-one tail-append capability and provider-specific lease operations are exposed separately from
the portable coordination operations:

```text
AppendIfCurrent(key, expectedToken, expectedLength, suffix, operationIdentity)
    -> Applied(newToken, newLength) | Rejected | Ambiguous(error)

AcquireFiniteLease(key, duration)
RenewLease(key, leaseId)
ChangeLease(key, currentLeaseId, proposedLeaseId)
ReleaseLease(key, leaseId)
BreakLease(key, breakPeriod)
```

For `AppendIfCurrent`, `Applied` guarantees that the new visible content is exactly the previously observed content
followed by `suffix`; it may not modify any earlier byte. Azure version one uses this operation plus the lease group,
including `ChangeLease` for fast planned transfer to a prepared follower. How Azure realizes the append is hidden below
this semantic contract. Ordinary Amazon S3 has no efficient equivalent, so a future S3 adapter may select a different
TRL representation.

### Transaction-aligned publication above the storage interface

The normative [TRL publisher](Architecture.md#trl-publisher) owns append, genesis, rotation/adoption and checkpoint
selection. A successful conditional publication on the canonical TRL exposing a complete transaction establishes its
durability. There is no second write to a per-database state record. Staged blocks, unlinked successors, and incomplete
transactions do not establish additional durable progress.

Application commits consume input or record an explicit application-selected skip; schema commits preserve the cursor.
The core validates complete transaction history and rollback evidence, including ordered ranges across TRL files.
A per-object CAS is not a multi-object transaction: complete recovery closure must be reachable before a cross-file
commit is reported durable. Prepare successor files first, then expose their complete chain by CAS on the canonical predecessor TRL.
The selected ordering still requires provider qualification; no second state CAS is added.

Genesis and schema operations immediately request asynchronous canonical TRL publication after local commit, without
waiting for the CAS or delaying dependent local work. The publisher still includes required predecessor history in
order. Ordinary application publication may use lazy batching; local commit completion never implies Blob durability. There is no additional database-state acknowledgement.

The adapter preserves every published prefix and treats tokens as opaque. If continuation metadata accompanies a TRL
publication, content and that metadata must change atomically under the same token, not through an independent later
metadata update. This is a capability to qualify for the chosen rotation codec, not an assumed multi-object transaction.
A timeout/cancellation remains ambiguous until the actual TRL operation can be reconciled.

### Conditional-write outcomes

The distinction between the three outcomes is safety-critical:

- `Applied` means the provider confirmed that this operation changed the object and returned the new opaque token.
- `Rejected` means the provider definitively reported that the supplied precondition was false. This is an ordinary
  lost-race result.
- `Ambiguous` means the operation may have committed, but the caller cannot prove whether it did. Timeouts, connection
  resets, cancellation, most `5xx` responses, authentication failures after dispatch, and a success response without
  a usable new token belong here.

Every conditional body should carry a unique operation identity, session identity, and intended revision. After an
ambiguous result, the caller reads the object and decides whether its exact operation landed, a competitor won, or the
answer is still inconclusive. A read of the old body does not prove an outstanding request cannot still commit.
Cancellation also does not fence that request. It must not blindly retry the old conditional request.

Transparent retries are particularly dangerous for CAS. The first attempt can commit and lose its response; an
automatic retry then uses the old token, receives a clean precondition failure against its own update, and falsely
reports that the operation lost. CAS traffic needs a separate client or retry policy with automatic retries disabled.

### Safety before automatic takeover

CAS chooses one winner for a leader-record replacement, but CAS alone does not tell a successor when an unreachable
leader has stopped using authority granted by an earlier update. Automatic takeover additionally requires one of:

- a provider-enforced finite lease whose expiry is evaluated by the storage service;
- a portable deadline protocol with qualified bounds for clock uncertainty, storage latency, and conservative
  leader/follower self-fencing margins;
- or a slower protocol that confirms every accepted batch against storage before exposing it.

Loss of the HTTP leader session may trigger an early leader-record read and candidacy, but it cannot expire authority.
If the deployment cannot establish a safe expiry bound, automatic takeover must remain frozen or require an explicit
authority procedure. Availability is sacrificed rather than split-brain safety.

Even a native provider lease fences only operations against the leased object. Bulk objects remain immutable and
term-qualified, and followers still validate the leader-record-selected term and session before accepting directly
streamed TRL frames. Because the leader lease does not fence other blobs, every new term must
CAS-fence each writable predecessor TRL before canonical work begins. That adoption excludes prior requests carrying the
old ETag. Removed databases need no adoption or retirement fence: names are never reused, and a delayed old write to an
abandoned namespace is accepted. This exception does not weaken fencing for active databases.

### Live capability probe

Provider branding or support for the right HTTP header names is insufficient. On every provider/endpoint combination,
run these four operations against one unique temporary key:

1. create the absent key conditionally; it must succeed;
2. conditionally create the same key again; it must be rejected;
3. conditionally replace it using the current token; it must succeed;
4. conditionally replace it using the stale token; it must be rejected.

Delete the probe object afterward. A provider that accepts conditional headers but ignores them is unsafe. An
ambiguous response makes the probe inconclusive; it is not evidence of conformance.

The application event log supplies input durability/replay. The Blob publication boundary below is a recovery-base
watermark; it does not introduce a transaction acknowledgement or require application commits to await storage.

## `denoland/celld` reference study

### Source map and an important naming detail

At the pinned commit, `celld` does not define one small Rust trait that represents its object-storage contract. Its
practical interface is the concrete, cloneable `Bucket` adapter over `Arc<dyn object_store::ObjectStore>`. It retains:

- an ordinary store for reads and retryable writes;
- a `PaginatedListStore` for bounded listings with continuation tokens;
- a separate `cas_store` whose automatic retries are disabled;
- a `StorageBackend` value that selects the provider's conditional-write dialect;
- a bucket/container name and an optional key prefix applied by the adapter.

`BucketOwnership` is a separate higher-level adapter that maps ownership and node-lease records onto `Bucket`. Calling
either one “the interface” hides a useful layering boundary, so both operation inventories are recorded below.

Primary sources:

- [`Bucket` and conditional-store adapter](https://github.com/denoland/celld/blob/a52f9905425bc41134d817694bdc2c50bcc5e856/crates/celld/bucket.rs)
- [`BucketOwnership` adapter](https://github.com/denoland/celld/blob/a52f9905425bc41134d817694bdc2c50bcc5e856/crates/celld/ownership_store.rs)
- [correctness guarantees and fencing protocol](https://github.com/denoland/celld/blob/a52f9905425bc41134d817694bdc2c50bcc5e856/docs/guarantees.md)
- [epoch-qualified replication](https://github.com/denoland/celld/blob/a52f9905425bc41134d817694bdc2c50bcc5e856/crates/celld/replication.rs)

### Backend dialects

| `StorageBackend` | URL scheme | Opaque CAS token | Conditional update dialect |
| --- | --- | --- | --- |
| `S3` | `s3://` | ETag | `If-None-Match` / `If-Match` |
| `Gcs` | `gs://` | Object generation | `x-goog-if-generation-match` |
| `Azure` | `az://` | ETag | `If-None-Match` / `If-Match` on `Put Blob` |
| `Local` | `dev://` internally | Local-store ETag | Local development implementation |

GCS is a genuinely different dialect: it does not apply `If-Match` to object PUT in the way needed here, so `celld`
uses object generations. Azure shares the S3 ETag dialect but uses a separate Azure client and credential path. A
missing or empty token is an error because the resulting version could not be fenced by a later update.

The bucket specification accepted by the cloud constructor is `s3://NAME[/PREFIX]`, `gs://NAME[/PREFIX]`, or
`az://NAME[/PREFIX]`; for Azure, `NAME` is a container. The adapter owns prefixing so callers always use unscoped keys.

### `Bucket` construction and inspection operations

| Operation | Visibility | Purpose |
| --- | --- | --- |
| `Bucket::open` | Public | Opens a cloud bucket/container from its URL, endpoint, region, credentials, and traffic label. |
| `Bucket::open_with_sources` | Public, doc-hidden | Same construction path with explicit GCS/Azure configuration sources rather than ambient environment only. |
| `Bucket::open_dev` | Crate-private | Opens the machine-local development store. It is deliberately not a fleet bucket. |
| `Bucket::with_stores` | Internal-test configuration | Injects ordinary and CAS stores for contract tests. |
| `scheme` | Public | Returns the operator-facing backend scheme. |
| `backend` | Public, doc-hidden | Returns the conditional-write dialect selector. |
| `gcs_replica_store*` | Public, doc-hidden helpers | Builds a separate retryable GCS transport for replica traffic. |
| `azure_replica_store*` | Public, doc-hidden helpers | Builds a separate retryable Azure transport for replica traffic. |

Each `open` creates its own HTTP transport, so a separately constructed client also creates a separate connection pool
and traffic lane. `BucketOwnership` relies on that property for isolated lease traffic.

### Core `Bucket` object operations

| Operation | Result and semantics |
| --- | --- |
| `get(key)` | Returns the complete body and required CAS token, or `None` when absent. |
| `head(key)` | Returns object size and required CAS token, or `None` when absent. |
| `put(key, body)` | Unconditional write through the ordinary retryable store. |
| `head_with_meta(key, name)` | Returns size and one user-metadata value, or `None`; this helper does not return a CAS token. |
| `put_with_meta(key, body, meta)` | Unconditional write carrying user metadata. |
| `put_cas(key, body, token)` | `token=None` means create-if-absent; `Some(token)` means replace exactly that version. |
| `delete(key)` | Idempotent delete; an absent key counts as success. |
| `delete_many(keys)` | Batches deletes and returns keys now gone; failed keys remain for a later pass. |
| `list(prefix)` | Drains all pages and returns every object under the prefix. |
| `list_any(prefix)` | Stops after enough work to answer whether any object exists. |
| `common_prefixes_page(...)` | Returns one bounded delimiter-listing page with `start_after`, provider page token, and `max_keys`. |
| `common_prefixes(prefix)` | Drains all pages and returns immediate child prefixes. |

`put_cas` has this exact semantic result shape:

```text
token = None       -> provider conditional create
token = Some(T)    -> provider conditional update matching T

Ok(Some(newToken)) -> applied
Ok(None)           -> clean provider-enforced precondition rejection
Err(error)         -> ambiguous; the write may have committed
```

`celld` recognizes the underlying `object_store::Error::Precondition` and `AlreadyExists` variants as clean CAS
rejections. Every other error remains ambiguous. An applied response that lacks the required ETag or generation also
becomes an error rather than inventing a token.

Azure has one listing-specific limitation in this interface: `common_prefixes_page` rejects `start_after`, because
Azure listing has no equivalent start-after parameter. Provider continuation tokens still work. This matters for
bounded discovery scans, including native KVI discovery; validate the selected files and canonical ancestry independently.

### Extended blob operations

The same adapter contains an extended API used to emulate Cloudflare R2-style blob bindings. These are available
operations, but they are not all needed for `celld` ownership CAS or for the minimum BTDB.Replication control plane.

| Operation | Result and semantics |
| --- | --- |
| `head_blob(key)` | Returns `BlobMeta`, including size, ETag, version, normalized CAS token, upload time, HTTP attributes, and user metadata. |
| `get_blob(key, range, conditions)` | Returns `BlobRead::Missing`, `Unmet(meta)`, or `Hit(blob)` with a streaming body. |
| `put_blob(key, body, attributes, conditions)` | Performs unconditional overwrite when there are no conditions; otherwise evaluates conditions against a head and executes a CAS create/update. `Some(meta)` means applied and `None` means rejected. |
| `list_page(prefix, after, limit, delimiter)` | Returns one bounded `BlobPage` with objects, rolled-up prefixes, truncation state, and a resumable cursor. |
| `begin_multipart(key, attributes)` | Returns the underlying multipart-upload handle; part writes, completion, and abort are operations on that handle. |

Supporting public value types are:

- `BlobRange`: `Whole`, `From(offset)`, `Bounded { offset, length }`, or `Suffix(length)`;
- `BlobConditions`: `if_match`, `if_none_match`, `uploaded_before_ms`, and `uploaded_after_ms`;
- `BlobAttributes`: content type, language, disposition, encoding, cache control, and user metadata;
- `BlobMeta`, `Blob`, `BlobRead`, `BlobEntry`, `BlobPage`, and `CommonPrefixPage` for results.

The upload-time conditions are checked by the adapter because HTTP date conditions have only second precision while
the R2-facing contract uses milliseconds. For a conditional `put_blob`, the adapter binds its read-side decision to
the exact CAS token in the eventual write so a racing update cannot slip between the check and overwrite.

### Validation and CAS probing operations

| Operation | Purpose |
| --- | --- |
| `validate()` | Proves the bucket is reachable and credentials are accepted by requesting one listing result within the configured prefix. |
| `probe_cas()` | Executes the four-step live conditional-write contract and fails on any semantic violation. |
| `probe_cas_steps()` | Crate-private form that distinguishes a permanent `Violation` from a transient or ambiguous error. |

The CAS probe uses a unique random key so concurrent node probes cannot collide. It attempts cleanup on every path; a
cleanup failure can leave one tiny object under `probe/` and is logged.

### Retry and transport behavior

At the pinned commit, the ordinary store is configured with two automatic retries and a 30-second retry timeout. The
CAS store is configured with zero automatic retries. Client request and connection timeouts are bounded at 15 and 3
seconds respectively. These values are part of `celld`'s self-fencing timing argument, not merely performance tuning.

The separation is more important than those particular numbers. BTDB.Replication should provide an isolated control
lane with bounded request time and no hidden conditional-write retries. Bulk upload traffic may use normal retry and
connection-pool behavior without starving lease renewal or control reconciliation.

### `BucketOwnership` operations

`BucketOwnership` turns generic object operations into the following ownership and node-liveness interface:

| Operation | Purpose |
| --- | --- |
| `new(bucket, lease_bucket, node, probe_public_key)` | Creates the adapter with a normal bucket and an isolated lease-pool bucket. |
| `with_lease_ttl_ms(ttl)` | Configures the node-lease lifetime used by peer recency logic. |
| `lease_ttl_ms()` | Returns that configured lifetime. |
| `bucket_client()` | Returns a cheap clone of the normal bucket client. |
| `live()` | Returns the live load counters published with node renewals. |
| `storage_scheme()` | Returns the backend scheme for diagnostics. |
| `process_generation()` | Returns the stable identity of this exact lease-writing process when configured. |
| `read_owner(cell)` | Reads `cells/<cell>/own.json` into an optional owner, epoch, and CAS token. |
| `read_node_lease(owner)` | Reads `nodes/<owner>.json` through the normal pool. |
| `read_self_node_lease(owner)` | Reads the process's own authority record through the isolated lease pool. |
| `read_capacity_peers()` | Lists recent node records, then reads and decodes them with bounded concurrency. |
| `release_owner(cell, epoch)` | Reads the exact current record and conditionally replaces it with an unowned record only if this node still owns that epoch. |
| `cas_owner(cell, guard, epoch)` | Conditionally creates or replaces the ownership record for this node and epoch. |
| `cas_node_lease(guard, record, stamped)` | Conditionally creates or replaces this node's lease record through the isolated pool and returns its new token. |

Crate-private supporting operations update and observe the folded node log carried by a lease renewal:
`set_own_log`, `own_log`, and `applied_log`. Test-only operations inject or inspect load sampling.

The guards and outcomes deliberately hide provider details:

```text
CasGuard = Absent | Match(token)
CasOutcome = Applied | Rejected
LeaseCasOutcome = Applied { token } | Rejected
```

Errors remain the ambiguous third state at the Rust `Result` level. `release_owner` first verifies both node identity
and epoch, then uses the token from that exact read; it cannot erase a successor's claim after a race.

The node lease body includes expiry, direct address, probe public key, peer protocol, process generation, load, and a
folded log state. A separate lease client and connection pool prevent ordinary object traffic from consuming the
authority-renewal lane.

### Ownership, fencing, and replicated-data pattern

`celld` separates three concepts:

1. `cells/<cell>/own.json` names the one owner session and a monotonically advancing fencing epoch.
2. `nodes/<node>.json` is a renewable process lease with an expiry. The documented default lifetime is 10 seconds,
   renewal starts after one third of the lifetime, and the process self-fences after the published expiry or as soon
   as another writer replaces/removes its lease record.
3. Bulk SQLite/LTX data is written with ordinary PUT under `cells/<cell>/ltx/e<epoch>/`. The epoch in the key is the
   data fence: a delayed old owner can write only into its superseded prefix, while restore selects the accepted
   lineage.

Each cell activation advances the epoch, including a local wake. Small mutable authority state uses CAS; large data
uses unique fenced keys. This is directly applicable to a BTDB leadership term and term-qualified TRL/checkpoint
objects.

`celld` additionally waits for a durability proof and rechecks ownership before acknowledging a write, providing its
documented RPO=0 contract. BTDB.Replication intentionally does not copy that acknowledgement gate because the retained
upstream event log can recreate a discarded unpublished tail.

Other important differences are:

- `celld` has no append primitive in `Bucket`; it uses PUT/multipart behavior and epoch-qualified keys. BTDB's semantic
  conditional tail append is a separate adapter capability, not a feature inherited from the `celld` abstraction.
- A stale `celld` owner may continue writing an old epoch prefix because the current ownership record selects the
  authoritative prefix. BTDB additionally has a direct TRL stream, so every follower must validate term, session,
  sequence, and frame chain before accepting bytes.
- `celld`'s clock and lease timing is evidence for a design pattern, not proof that BTDB's multi-node Azure failover
  timing is safe. It must be independently modeled and fault-tested.

### Provider qualification lessons from `celld`

The pinned guarantees document names Amazon S3, Cloudflare R2, Tigris, Google Cloud Storage, and Azure Blob Storage as
qualified stores. Its release tests use R2; the S3-compatible path shares the same client and conditional headers.

It also records useful negative evidence:

- Backblaze B2, Hetzner Object Storage, and DigitalOcean Spaces did not implement the required conditional writes and
  are unsafe for `celld` ownership at that snapshot.
- MinIO Community Edition passed the storage probe but was not production-qualified; one specifically identified 2025
  release had a conditional-create regression.
- Azure was qualified on 2026-08-18 with an account key, VM managed identity, and AKS workload identity, but only in a
  single-node setup. That is not qualification of BTDB.Replication's Azure leader-election protocol.

The reusable conclusion is to qualify behavior, not product names. S3-compatible endpoints in particular vary in
whether they enforce the required conditions.

## Azure Blob Storage

Azure is the primary provider for the first design. It offers the portable ETag CAS primitives, conditional Block Blob
publication for implementing atomic tail append, and service-enforced finite Blob leases.

### Consistency and conditional writes

Azure Blob Storage provides strong consistency and snapshot-isolated reads. Without a condition, concurrent writes
are last-writer-wins.

- `If-None-Match: *` on a supported write implements create-if-absent; an existing blob produces HTTP `412
  Precondition Failed`.
- A read or write returns an ETag. `If-Match: <etag>` applies the next write only if no intervening operation changed
  the blob; a stale ETag produces HTTP `412`.
- ETags are opaque version tokens. Their formatting and any relationship to content must not be interpreted.
- Conditions apply to one Blob operation. They do not create a transaction across the leader record, a TRL, a KVI, and its prerequisite data objects.

The shared leader object should be a small block blob replaced with `If-Match` when leadership changes or the current
owner updates timeout skip entries. Its finite native lease renews independently without changing the ETag.
Canonical TRL publications use `If-Match` directly; their success establishes transaction durability.
Checkpoint selection remains a separate metadata operation, not a per-append acknowledgement. Bulk objects are immutable or term-qualified, so a stale
session cannot place its later bytes into a current-term accepted recovery graph.

References:

- [Azure Blob Storage conditional headers](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations)
- [Azure Blob Storage concurrency model](https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage)
- [`Put Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob)

### Conditional tail append with Block Blob

The storage port requires `AppendIfCurrent`: if the opaque token and length still match, atomically expose the old file
followed by the supplied suffix. Azure version one plans to implement that per-file semantic operation with a Block
Blob, but the block layout is not part of the failover protocol. The transaction-aware publisher above this adapter
invokes it for completed local transactions. On the canonical chain, success exposing a complete transaction
establishes durable database progress directly.

The intended adapter keeps completed logical blocks and rebuilds only the final partial 4 MiB block together with any
new blocks. `Put Block` stages those bytes without changing the visible blob. A final `Put Block List`, guarded by
`If-Match` against the previously observed blob ETag, atomically publishes the new ordered file. The adapter must also
verify the expected logical length and preserve every byte before it; a successful operation therefore appears to the
core as an append, even though Azure committed a new block list. A transaction may span several TRL blobs; the core owns their recovery closure and publication ordering.
Azure blocks are transport/storage chunks, distinct from TRL file boundaries.
`Put Block List` can carry blob metadata in the same conditional operation; separate `Set Metadata` afterward must not
be required for transaction durability. Exact continuation metadata and its size bounds remain adapter/core integration work.

Important Azure constraints remain inside this adapter:

- `Put Block` itself does not support normal conditional headers and staged blocks are invisible until commit;
- `Put Block List` supports `If-Match` and can mix existing committed blocks with newly staged blocks;
- block IDs are fixed-length within one blob, and uploading the same uncommitted ID again replaces its staged content,
  so one ID must never denote different bytes across attempts;
- a block blob supports 50,000 committed and 100,000 uncommitted blocks; uncommitted blocks expire after about a week;
- with 4 MiB logical blocks the committed capacity is about 195 GiB, well above BTDB's current per-TRL limit;
- `Put Block List` overwrites blob properties and metadata unless the adapter supplies the intended values again;
- an ambiguous conditional commit is reconciled through the resulting ETag, length, committed block list, expected
  suffix hash, and operation identity rather than blindly retried.

If the canonical block-list CAS succeeds but its response is lost, its complete published transactions are durable;
reconcile that operation before retrying. Previously staged blocks or unlinked rotated files alone are not canonical.
The append invariant protects earlier published bytes. Takeover fences the old writable TRL version and creates a
term-qualified continuation; it cannot discard a successfully published transaction as merely awaiting state selection.

The existing `BTDB.AzureStorage` uses the same general stage-and-commit family with deterministic 128 KiB blocks, but
its commits are unconditional. That code remains a transfer reference, not the failover concurrency contract.

References:

- [Azure `Put Block`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block)
- [Azure `Put Block List`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list)

### Native Blob leases

Azure can place an exclusive write/delete lease on one blob:

- a finite lease lasts 15 to 60 seconds; an infinite lease is also available;
- a lease can be acquired, renewed, released, changed, or broken;
- writes and deletes to that blob must carry the active lease ID or fail with HTTP `412`;
- ordinary reads remain possible without the lease ID;
- the `Lease Blob` operation, including acquire, renew, and change, does not change the blob's ETag;
- a container lease protects container deletion, not writes to all blobs in that container.

A native finite lease on `cluster/leader.json` provides the storage-service-evaluated cluster authority boundary while
ETag CAS serializes leader identity changes. Azure `Change Lease` also provides the fast graceful-handoff path without
waiting for lease expiry: the old holder supplies its current lease ID and the target's proposed GUID. The target
immediately renews with that proposed ID to prove ownership and reset the finite lease clock; `Change Lease` changes the
ID and cannot change the configured duration. Separate leases on every remote TRL file are unnecessary initially: the
conditional append operation serializes each file, and every new cluster term uses new object keys. The leader lease is
still not a physical fence on other objects, local disk writes, or direct follower traffic, so term-qualified names,
per-database TRL adoption, and follower-side authority validation remain required.

Graceful shutdown separates the Azure **data plane** from the narrow **authority lane**. After a database reaches its
shutdown canonical cut, the old leader dispatches no `AppendIfCurrent`, immutable index/artifact upload,
KVI publication, or deletion. It may still read and reconcile earlier operations and may renew or change
the lease on `cluster/leader.json`; those lease calls do not publish database data and are required to keep fencing valid
until safe handoff. A data request dispatched before the cut may already complete, so its outcome is reconciled rather
than guessed, but no follow-up data write is issued.

Reference: [Azure `Lease Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob).

### Throughput, hot objects, and traffic isolation

Azure currently documents a target of up to 3,000 requests per second for one block blob. Actual throughput depends on
request size, account type, concurrency, and name distribution. Hot partitions can produce `503 Server Busy` or `500
Operation Timeout`; retryable data-plane operations need bounded exponential backoff.

The design should not perform an object-store request for every transaction. Whole canonical transactions and
checkpoints are published in batches while a conservatively cached authority window remains valid. A batch may cross
TRL files but may never end inside a transaction. Throttling then increases durable-boundary lag and possible event
replay rather than normal transaction latency.

The single leader blob is intentionally a serialization point and must not be overloaded with per-transaction
updates. Lease/CAS traffic should use an isolated client and connection pool so large uploads cannot starve authority
renewal. Alert before remote progress lag approaches upstream event retention or available local comparison-log capacity.
There is no replication-owned historical-root rollback window.

Reference: [Azure Blob Storage scalability targets](https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets).

### Azure-first preliminary choice

The current preference is:

- one small cluster-wide leader block blob containing endpoint and API key, protected by ETag CAS and a finite native
  Blob lease;
- direct conditional publication on the active canonical TRL for each database, with term-qualified continuations;
- batched conditional atomic tail append, with the Block Blob and block-list mechanics hidden inside the Azure adapter;
- ordered continuation between TRL files, including transactions spanning files; prepared-successor publication through predecessor TRL CAS still to qualify;
- immutable native KVI and compaction artifacts, with prerequisites uploaded before KVI and no separate checkpoint pointer;
- a dedicated no-retry CAS/lease transport lane;
- Azure `Change Lease` for planned handoff to a prepared follower;
- after the old leader's graceful-shutdown cut, no further database data publication from that session; only
  read/reconciliation and lease renew/change/release remain available for authority transfer;
- replication-mode TRL rotation with complete-transaction recovery, and reconciliation of every ambiguous TRL publication and
  adoption operation.

The native lease improves the Azure expiry story, while ETag CAS remains the revision serializer and term-qualified
keys remain the bulk-data fence.

## Amazon S3

Ordinary Amazon S3 remains a possible later target for immutable checkpoints and TRL ranges plus CAS publication of a
small future provider-specific leader/state design.

### Portable S3 behavior

- `If-None-Match: *` provides create-if-absent.
- `If-Match: <etag>` provides conditional replacement of the current object.
- A stale condition normally produces HTTP `412 Precondition Failed`; concurrent delete/write races can also produce
  operation-specific `409 Conflict` or `404 Not Found` results that require reconciliation.
- S3 provides strong read-after-write consistency for PUT and DELETE and subsequent GET, HEAD, and LIST operations.
- An update to one key is atomic: readers see the previous or new object, never partial content.
- ETags are opaque CAS tokens and must not be treated as content hashes.
- Bucket policy can require `If-Match` or `If-None-Match`, reducing the risk of an accidental unconditional state
  writer.
- S3 has no general-purpose object lease equivalent to Azure Blob leases.

Ordinary S3 cannot append to an existing object. A future S3 canonical TRL representation must therefore use immutable
range objects, sealed files, or chunks with an explicitly qualified conditional publication mechanism. It must
publish whole transactions; future S3 research does not reintroduce a second state commit into the Azure TRL path.

References:

- [Amazon S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Amazon S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)

### S3 Express One Zone exception

Directory buckets using S3 Express One Zone support an append-like `PutObject` mode:

- `WriteOffsetBytes` must equal the object's current length;
- one append request may contain at most 5 GB;
- each append creates a part, and an object may have at most 10,000 parts;
- `CopyObject` can reset the accumulated part count.

This is restricted to directory buckets backed by S3 Express One Zone, a single-Availability-Zone storage class. It
should not define the portable failover contract. It may be evaluated later as an explicitly non-portable performance
option.

Reference: [Appending data in S3 Express One Zone directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html).

### S3 takeover limitation

Conditional control replacement can serialize candidates, but ordinary S3 supplies no server-enforced renewable
lease. Automatic timed takeover therefore depends on qualifying the portable deadline and clock-uncertainty model. If
that model cannot be proven for the deployment, S3 mode must freeze on ambiguous authority or require an explicit
takeover procedure rather than risk two leaders.

## Conclusions carried into the architecture

- Use one leased/CAS leader record for cluster authority; canonical TRL CAS directly publishes per-database progress.
  Prepare prerequisites under immutable or term-qualified keys and fence predecessor TRL tokens on takeover.
- Treat version tokens as opaque and preserve `Applied`, `Rejected`, and `Ambiguous` as distinct outcomes.
- Disable transparent retries for conditional writes and reconcile ambiguity by reading operation identity.
- Isolate authority traffic from checkpoint and TRL upload traffic.
- Run a destructive-but-self-cleaning four-step CAS probe against the real endpoint before enabling leadership.
- Discover native KVIs through file listing and validate references/ancestry. Publish KVI last; only then delete
  obsolete files. No separate checkpoint pointer or manifest is required.
- Expose remote deletion only to leader-owned GC and apply the configured deletion delay after publishing the complete replacement closure, without restore pins; never reuse retired keys; every node independently deletes only its own locally unpinned files, with no distributed
  deletion instruction.
- Treat canonical TRL CAS exposing a complete transaction as durable publication, without a second state CAS.
  Qualify cross-file transaction publication, continuation discovery and predecessor fencing.
- Prefer sealed files or immutable chunks as the first portable checkpoint format.
- Use Azure finite Blob leases to strengthen the Azure implementation, without confusing one-blob lease enforcement
  with a database-wide fence.
- Expose only conditional atomic tail append to the failover core. Implement it in the Azure adapter with conditional
  Block Blob block-list publication; S3 may need a different TRL representation.
- Independently qualify the complete multi-node failover protocol even when a provider already passed `celld`'s
  storage probe.

### Whole-file checksums for cache reuse

For sealed files, startup can skip payload download when the local file's recomputed whole-file SHA-256 and length match trusted metadata
for the selected remote object version. Store the hash in blob metadata during the same publication as the data; no
checksum sidecar or checkpoint pointer is needed. Hash sealed files once while reading/uploading. Do not compute or maintain a whole-file hash for a growing TRL.
Always download the last active TRL again on restore, even when the local length matches or checksum metadata exists.
After sealing, publish its final checksum for later cache reuse. Version-bound reads and ordinary transfer integrity
checks still apply to the active tail.

Get Blob Properties returns metadata, length and ETag without the payload. Bind subsequent range reads to that ETag
with If-Match; a changed version requires reconciliation. ETag is an opaque version token, not a content hash. Azure's
Content-MD5 is optional: Put Block List stores the supplied whole-blob MD5 without validating it and clears it if omitted.
Neither the block-list request checksum nor individual block checksums establish an automatically available whole-file
checksum. Legacy files without a trustworthy digest remain readable, but do not get the no-download optimization.

Use bounded per-file/intra-file transfer concurrency and prioritize ascending TRL replay dependencies. Downloaded bytes
remain unavailable to replay until their selected version, length and integrity checks pass. Concurrent transfer does
not authorize application readiness or election before recovery finishes.

Sources: [Get Blob Properties](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties),
[Put Block List](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list).

KVI upload has a strict start barrier: all required PVLs and canonical TRL through the KVI's fixed file/offset must
already be published before the first KVI upload request, including Put Block staging. Successful KVI completion last
is insufficient if its transfer started earlier. Reconcile ambiguous prerequisite publication before dispatching KVI;
newer tail bytes beyond the KVI cursor do not extend this barrier. Local staging of the remote KVI can run ahead of it; node-local compaction creates no KVI.

### Separate remote compaction inventory

Remote compaction is leader-only and plans against verified Blob objects and the selected recovery closure, not the
node's local file listing. Local PVL IDs can differ from remote destinations; replication does not use native file generations. The native KVI export maps
source references into the planned remote namespace before serialization; see
[local and remote compaction](Architecture.md#independent-local-compaction-and-leader-only-remote-compaction).
Neither compaction mode distributes results to running peers. Remote PVL/KVI output is consumed through normal Blob
restore. Conditional publication, KVI-last ordering, source/destination pins and key non-reuse still apply; exact mapping
and interruption semantics require B3/B5/Q6 tests.


## M1 live Azure capability evidence — 2026-09-14

Ran [azure_probe.py](../BTDB.Replication.Test/Integration/azure_probe.py) against a temporary Standard_LRS StorageV2
account in DEV Sandbox / West Europe, using REST version `2023-11-03` and Azure CLI account-key access.
The probe makes individual HTTP calls without retries and keeps credentials in memory.
[Recorded results](../BTDB.Replication.Test/Integration/azure-2026-09-14.json) contain 24 requests, all with expected
status codes. The private test container was deleted in `finally`; the temporary resource group/account was then
deleted, and `az group exists` returned `false`.

Observed directly: staged bytes did not change the committed body/ETag; Put Block List changed content and metadata
together; same-byte term adoption changed the ETag and rejected an old append with 412. Lease acquire/change/renew
left the leader blob ETag unchanged, blocked an unleased write to that blob, and did not block another blob. Renewal
with the old ID after change returned 409; the new ID succeeded.

These observations exercise the relevant [Put Block List](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list)
and [Lease Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob) contracts. They do not qualify
all concurrency schedules, network failures, real clock drift/suspend, expiry timing, credential renewal or throughput.
The ambiguity case discards a known successful response locally; genuinely pending effects after timeout are covered
by the deterministic simulator, not claimed as a real Azure network-fault experiment. Payloads are small diagnostic
bytes; separate native tests establish TRL/KVI compatibility. The probe's extra `btdb_format` diagnostic metadata is
not a required field in the candidate `TrlMetadata` codec.
