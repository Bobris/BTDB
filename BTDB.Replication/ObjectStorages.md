# BTDB.Replication Object Storage Research

Status: Brainstorming; no implementation has started.

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
- **Per-database publication plane**: one small mutable state record per short-named database selects that database's
  transaction-aligned durable TRL boundary and checkpoint under the shared leadership term.
- **Data plane**: active TRLs are logically append-only, term-qualified remote files; checkpoints, manifests, and
  compaction artifacts are immutable or attempt-qualified. Competing terms never share a mutable data key.

The upstream event log permits loss and deterministic replay of a small unpublished application tail. Object storage
therefore does not need to prove durability before every event is acknowledged, but it must never allow two accepted
canonical histories or an accepted prefix ending inside a transaction.

### Minimum coordination properties

An endpoint is suitable only if it provides all of these properties:

1. **Conditional create**: create key `K` only when no current object exists at `K`.
2. **Conditional replace**: replace key `K` only when its current opaque version token equals the token returned by a
   preceding read.
3. **Strong read-after-write consistency**: after a successful write, subsequent reads observe that write.

No part of the design may assume an atomic transaction across two object keys. The leader record grants authority but
contains no database progress. Data objects must be completed first; a conditional update of the corresponding
database state record then makes that database's complete transaction boundary or immutable graph authoritative.

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
must never define the authoritative restore set; an immutable manifest must name every required object explicitly.

`DeleteMany` is exposed only to the current leader's remote-garbage-collection state machine. Followers may delete
completely unused files from their own local BTDB file collections, but never call object-store deletion. Such local
deletion is never distributed: only the node can see all files pinned by its open read-only transactions and retained
roots. Leader authority alone is not a deletion-safety proof because the lease on `cluster/leader.json` does not
physically fence a previously dispatched request to another Blob. Every remote deletion candidate must therefore be
irrevocably unreferenced by all retained manifests/recovery roots so a delayed old-term delete remains harmless.

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

Decision recorded 2026-08-30: every database-state commit must select a complete BTDB transaction boundary, including
all file segments when the transaction spans several TRL objects.

`AppendIfCurrent` is atomic for one physical object, not for one BTDB transaction. BTDB can rotate TRL files while a
transaction is open, so a canonical transaction is an ordered set of one or more file segments. The authoritative
durable cursor is defined over that ordered TRL chain and must always be immediately after a complete `Commit` or
`CommitWithDeltaUlong`; `EndOfFile`, a new-file header, physical object length, and a successful per-file append are not
transaction boundaries.

No remote data write for a transaction is dispatched until the leader has locally closed and captured the whole
transaction. The publisher then:

1. conditionally appends or creates every physical TRL file segment touched by a batch of whole transactions;
2. verifies the exact accepted length and prefix hash of every touched file;
3. writes an immutable, hash-addressed boundary descriptor linking the previous accepted boundary and naming every
   ordered segment, file cut, transaction end, sequence, and frame hash;
4. conditionally replaces the one database state record so it selects that descriptor and its final closed cursor.

Step 4 is the only durable BTDB publication point. Individual Azure `Put Block List` calls can make prepared file data
physically visible, and an intermediate TRL object may end at a cross-file continuation point, but those objects remain
unaccepted staging data until the single state-record CAS selects the complete multi-file transaction. Listings and raw
object lengths never make staging data authoritative.

This gives a binary crash result despite the lack of a multi-object transaction:

- before the state CAS, recovery selects the previous complete transaction boundary and ignores all new objects;
- after the state CAS, every referenced file cut and descriptor was already uploaded and verified, so recovery selects
  the new complete boundary;
- after an ambiguous state-CAS response, the publisher reads the state and operation identity and chooses one of those
  two results; it never guesses or publishes a subset.

A publication batch may contain many transactions and one transaction may contain many TRL segments. Batching may
change request frequency only; it may never cut a transaction or advance state while the replay decoder would still
hold an open transaction.

The failover core receives these operations through an injected storage port and never references an Azure or S3 SDK
type. A deterministic in-memory implementation must model opaque version tokens, leases, conditional rejection,
ambiguous completion, visibility, and independently delayed responses. The in-memory and real-provider adapters must
pass the same semantic conformance suite; provider tests then add service-specific limits and failure behavior.

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
answer is still inconclusive. It must not blindly retry the old conditional request.

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
streamed TRL frames. Because the leader lease does not fence separate database state blobs, every new term must
CAS-adopt each database state before canonical work begins. That adoption drains prior requests carrying the old ETag.

### Live capability probe

Provider branding or support for the right HTTP header names is insufficient. On every provider/endpoint combination,
run these four operations against one unique temporary key:

1. create the absent key conditionally; it must succeed;
2. conditionally create the same key again; it must be rejected;
3. conditionally replace it using the current token; it must succeed;
4. conditionally replace it using the stale token; it must be rejected.

Delete the probe object afterward. A provider that accepts conditional headers but ignores them is unsafe. An
ambiguous response makes the probe inconclusive; it is not evidence of conformance.

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
bounded operational scans, but not correctness because manifests must link the accepted data graph directly.

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
- Conditions apply to one Blob operation. They do not create a transaction across the leader record, a database state
  record, a manifest, and its data objects.

The shared leader object should be a small block blob replaced with `If-Match` only when leadership changes; its finite
native lease renews independently without changing the ETag. Per-database state objects also use `If-Match` for durable
transaction-aligned TRL-boundary and checkpoint publication. Bulk objects are immutable or term-qualified, so a stale
session cannot place its later bytes into a current-term accepted recovery graph.

References:

- [Azure Blob Storage conditional headers](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations)
- [Azure Blob Storage concurrency model](https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage)
- [`Put Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob)

### Conditional tail append with Block Blob

The storage port requires `AppendIfCurrent`: if the opaque token and length still match, atomically expose the old file
followed by the supplied suffix. Azure version one plans to implement that per-file semantic operation with a Block
Blob, but the block layout is not part of the failover protocol. The transaction-aware publisher above this adapter
invokes it only for already completed local transactions and does not treat its success as durable database progress.

The intended adapter keeps completed logical blocks and rebuilds only the final partial 4 MiB block together with any
new blocks. `Put Block` stages those bytes without changing the visible blob. A final `Put Block List`, guarded by
`If-Match` against the previously observed blob ETag, atomically publishes the new ordered file. The adapter must also
verify the expected logical length and preserve every byte before it; a successful operation therefore appears to the
core as an append, even though Azure committed a new block list. If a transaction spans several TRL blobs, their block
lists are committed as prepared data and one later database-state CAS publishes their immutable boundary descriptor.

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

If one or more Block Blob updates succeed but the following database-state CAS does not, physical lengths may be greater
than their accepted cuts and newly rotated files may be present. The append invariant keeps the older state-selected
boundary byte-identical and readable; the unselected multi-file transaction is ignored as a unit. A takeover starts a
new term-qualified remote lineage rather than extending an unaccepted predecessor suffix.

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
per-database state adoption, and follower-side authority validation remain required.

Graceful shutdown separates the Azure **data plane** from the narrow **authority lane**. After a database reaches its
shutdown canonical cut, the old leader dispatches no `AppendIfCurrent`, immutable descriptor/artifact upload,
database-state/checkpoint update, or deletion. It may still read and reconcile earlier operations and may renew or change
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
renewal. Alert before remote progress lag approaches upstream event retention or the locally retained rollback window.

Reference: [Azure Blob Storage scalability targets](https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets).

### Azure-first preliminary choice

The current preference is:

- one small cluster-wide leader block blob containing endpoint and API key, protected by ETag CAS and a finite native
  Blob lease;
- one CAS state record and one active term-qualified remote TRL lineage per short-named database;
- batched conditional atomic tail append, with the Block Blob and block-list mechanics hidden inside the Azure adapter;
- one immutable multi-file boundary descriptor per publication batch, selected only by a state CAS ending at a complete
  transaction;
- immutable checkpoint and compaction-artifact blobs selected by each database's state record;
- a dedicated no-retry CAS/lease transport lane;
- Azure `Change Lease` for planned handoff to a prepared follower;
- after the old leader's graceful-shutdown cut, no further database data publication from that session; only
  read/reconciliation and lease renew/change/release remain available for authority transfer;
- normal BTDB TRL rotation, including transactions that cross files, and reconciliation of every ambiguous append and
  state publication.

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
range objects, sealed files, or chunks selected through conditional state publication. The same immutable boundary
descriptor must select only whole transactions even though the physical data representation differs from Azure.

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

- Use one leased/CAS leader record for cluster authority and separate CAS state records for per-database progress;
  publish data under immutable or term-qualified keys.
- Treat version tokens as opaque and preserve `Applied`, `Rejected`, and `Ambiguous` as distinct outcomes.
- Disable transparent retries for conditional writes and reconcile ambiguity by reading operation identity.
- Isolate authority traffic from checkpoint and TRL upload traffic.
- Run a destructive-but-self-cleaning four-step CAS probe against the real endpoint before enabling leadership.
- Do not use listing as a correctness primitive; manifests explicitly name the accepted object graph.
- Expose remote deletion only to leader-owned GC and delete only objects proven permanently unreachable from every
  retained recovery root; every node independently deletes only its own locally unpinned files, with no distributed
  deletion instruction.
- Treat per-file writes as preparation only; publish one immutable multi-file descriptor through one state CAS, always
  ending at a complete transaction.
- Prefer sealed files or immutable chunks as the first portable checkpoint format.
- Use Azure finite Blob leases to strengthen the Azure implementation, without confusing one-blob lease enforcement
  with a database-wide fence.
- Expose only conditional atomic tail append to the failover core. Implement it in the Azure adapter with conditional
  Block Blob block-list publication; S3 may need a different TRL representation.
- Independently qualify the complete multi-node failover protocol even when a provider already passed `celld`'s
  storage probe.
