# BTDB.Replication Object Storage

Status: the provider-neutral contract, `ReplicationFileSet` inventory, remote maintenance and the Azure SDK adapters
(`AzureLeaderStorage`, `AzureReplicationStorage`) are implemented and tested against Azurite; see the
[Azure adapter guide](../BTDB.Replication.Azure/README.md). Live Azure qualification is open beyond the small M1
capability probe below. Amazon S3 is later provider research.

The `denoland/celld` snapshot is 2026-08-29, pinned to release `v0.4.0`, commit
`a52f9905425bc41134d817694bdc2c50bcc5e856`. Azure documentation was rechecked on 2026-08-30. Requalify provider
behavior and limits against the selected account type, endpoint, SDK and service version before production use.

This document owns the storage contract, the `celld` study and provider behavior. The failover protocol, canonical
TRL publication rules, checkpoint selection and compaction design are owned by [Architecture.md](Architecture.md).

## What replication needs from object storage

- **Coordination plane**: one small cluster-wide leader record (`leader.json`) selects the leader and publishes its
  transport-defined peer endpoint and API key. Azure protects it with ETag CAS plus one finite native Blob lease.
- **Per-database publication**: a conditional write on the canonical TRL directly publishes complete transactions;
  there is no second state CAS. A native KVI is published after all its prerequisites, with no separate manifest or
  checkpoint pointer.
- **Data plane**: canonical TRLs are logically append-only; PVL/KVI files are immutable and never reuse a key.

The upstream application event log can deterministically replay a lost unpublished tail, so storage need not prove
durability before a local commit completes. It must never allow two accepted canonical histories or an accepted
prefix ending inside a transaction.

## Provider-neutral contract

### Required provider properties

1. **Conditional create**: create key `K` only when no current object exists at `K`.
2. **Conditional replace**: replace `K` only when its opaque version token equals one returned by an earlier read.
3. **Strong read-after-write consistency** for reads and listings.
4. **Semantic conditional tail append**: if token and length still match, atomically expose the old content followed
   by the suffix, and never change an earlier byte. Content and object metadata change under one new token.
5. **Service-evaluated finite lease** on the leader record for automatic takeover (see below).

No operation may assume an atomic transaction across two keys. The leader lease fences only the leader blob, not TRL
or PVL/KVI objects, local disks or peer traffic; every new term therefore CAS-fences the writable predecessor TRL
(an unchanged-content conditional write) before canonical work. Successor IDs use shared keys across all terms.

### Implemented interfaces

| Interface | Operations |
| --- | --- |
| `IReplicationLeaderStorage` | `AcquireAsync` (finite lease), `RenewAsync(handle)`, `TransferAsync(current, proposed)` (planned handoff, confirmed by the target's renewal); `ReadAsync` -> body + token; `WriteAsync(leaseHandle, expectedToken, json)` -> `Applied`/`Rejected`/`Ambiguous`. |
| `IRemoteFileCollection` | `EnumerateAsync` and version-bound `ReadAsync` of `RemoteFile(FileId, FileType, Length, Version, IsSealed, Sha256)`. |
| `IReplicationStorage` | Canonical TRL `ReadAsync`/`ReadRangeAsync`/`WriteAsync`; `EnsurePureValuesAsync`, `ProtectPureValuesAsync`, `PublishKeyIndexAsync`; `ResolveRecoveryRootAsync`; `EnumerateMaintenanceAsync`, `ScheduleDeletionAsync`, `CancelDeletionAsync`, `DeleteAsync`. |

A read returns the listed content or fails; partial success never combines contents. A newer version counts only
when it provably keeps the listed bytes: a canonical TRL (append-only) at least as long, or an immutable PVL/KVI with
the same length and SHA-256, whose version changes only through deletion-mark metadata. The Azure adapter and
`CanonicalTrlInventory` then continue from that version, so a restore survives a leader that keeps publishing and
cleaning up; a deleted or otherwise changed object fails the read. Tokens are opaque. File IDs and types come from numeric names and extensions without reading native headers; higher IDs order
TRLs and KVIs within their sequences, and native generations are not used.

### Conditional-write outcomes

- `Applied`: the provider confirmed the change and returned a new token.
- `Rejected`: the provider definitively reported a false precondition (an ordinary lost race).
- `Ambiguous`: the request may have committed. Timeouts, connection resets, cancellation after dispatch, most `5xx`
  responses and a success without a usable token belong here.

Neither cancellation nor a later read of the old body fences a dispatched request; reconcile by reading the object.
Transparent SDK retries are unsafe for CAS: a first attempt can commit and lose its response, and the retry then fails
its precondition against its own update. Configure SDK clients with automatic retries disabled
(`Retry.MaxRetries = 0`). The canonical publisher may resend exactly the unresolved request, but never reads a
rejection of that resend as a lost race.

Implemented reconciliation needs no extra operation-identity field:

- **Canonical TRL**: an applied CAS from the expected version keeps its first `ExpectedLength` bytes, so the publisher
  compares only the intended appended bytes (and length) in bounded chunks, with no hashes.
- **PVL/KVI**: a lost or rejected create is confirmed by matching length and `btdb_sha256`. Anything else is a `RemoteFileConflictException` that fences the session; never overwrite it.
- **Leader record and lease**: after any non-applied selection write, only a reread equal to the exact intended JSON
  (which carries a fresh session ID) confirms it; a lost lease acquire or transfer is confirmed only by renewing the
  proposed lease ID.

### Safety before automatic takeover

CAS picks one winner for a leader-record replacement, but does not tell a successor when an unreachable leader stopped
using authority. Automatic takeover needs a provider-enforced finite lease, or a portable deadline protocol with
qualified clock-uncertainty, latency and self-fencing bounds. Without either, takeover must freeze or require an
explicit operator procedure. Loss of the peer session may trigger candidacy but never expires authority.

### Live capability probe

Header support is not proof of enforcement. Qualify each provider/endpoint with one unique temporary key: conditional
create succeeds, a repeated create is rejected, replace with the current token succeeds, replace with a stale token
is rejected; then delete the key. An ambiguous response makes the probe inconclusive. The probe is qualification
tooling, not a mandatory startup sequence; production handles lease and conditional-I/O errors normally.

### Implemented file inventory boundary

`ReplicationFileSet` implements `IFileReplicatedCollection`. `GetCount`, `GetFile` and `Enumerate` see only the local
cache; `GetRemoteFile` and `RemoteEnumerate` expose the selected remote inventory as read-only, version-bound
handles. No lookup downloads a body, and local additions/deletions never change the remote inventory. Remote inventory
is not refreshed during a session.

The owner awaits `InitializeAsync` before `BTreeKeyValueDB.OpenAsync`; open and prefetch never initialize implicitly,
and remote access fails until initialization succeeds. Initialization:

1. lists the complete remote inventory first, so a failed listing deletes nothing;
2. maps every selected remote file to its own local ID and forgets earlier placements;
3. removes local files with no remote counterpart, including copies cached under another ID;
4. validates cached candidates by extension, then length, then locally computed SHA-256 against remote metadata,
   hashing in parallel within the download bound (1 GB from disk: 2.7 s serially, 1.1 s with four);
5. publishes the inventory only after the whole attempt succeeds.

Active (unsealed) files and files without SHA metadata are never reused. Removals are logged through
`IKeyValueDBLogger` with local/remote IDs and the mismatch. `PrefetchAsync(fileId)` shares one bounded transfer per
file between callers. Downloads use 4 MiB ranges with four in flight per file, verify a sealed file's SHA-256 while
writing and keep native extensions so the copy passes the next startup validation; a partial or invalid download is
removed. Native header reads use small version-bound ranges.

`GetLocalFileId` exposes session-only mappings: identity after restore, and remote-to-local for PVLs this session
uploaded. Tests reproduce distinct local/remote contents under one ID, remote allocation independent of a higher local
maximum, out-of-order downloads, version changes, checksum failures, cancellation and retry; this is why local and
remote inventories stay separate.

### Remote PVL/KVI creation

Only the leader's checkpoint lane allocates remote IDs. It lists the remote inventory once per new checkpoint and
chooses the next even ID above every observed remote even ID and this session's earlier choices; no reservation
object, counter or ledger exists. Cleanup keeps the highest even ID as the allocation anchor. TRLs use odd native IDs.

`PublishPureValuesAsync` (internal to `ReplicationFileSet`) uploads each unmapped sealed local PVL with
`EnsurePureValuesAsync` and records the confirmed placement. The checkpoint's ID scan is the maintenance listing, so
it also reports deletion marks: a reused placement, including a verified download, that it listed unmarked needs no
request. Every other placement is revalidated by `ProtectPureValuesAsync`. Clearing a deletion mark changes the
version so an older delete cannot match; an unmarked object keeps its version, so restores reading it by ETag
continue, and the positive deletion delay keeps a late stale mark from becoming due before cleanup clears it. An
absent object gets a fresh ID, never its retired key. A follower's cached placement is not trusted after promotion:
the first checkpoint's listing decides. `CheckpointPublisher` keeps the chosen KVI ID, snapshot and mapping until confirmed;
PVL placements also keep their IDs across retries. Restart rediscovers remote state; no journal is persisted.

KVI upload starts only after all required PVLs and the canonical TRL through the KVI cut are published, with ambiguous
prerequisites reconciled; no KVI block is staged earlier. Before staging, the Azure adapter lists the database
namespace once, verifies that every native TRL dependency exists, and clears deletion marks from the retained suffix.
A steady-state checkpoint therefore needs
three listings (ID scan, KVI preparation, cleanup) and no request per retained file; with 20 PVLs and 153 retained
TRLs it previously needed four listings and 173 HEAD requests (`CheckpointRequestTest`, Azurite). The KVI streams from native serialization without a local
staging file.

### Whole-file checksums for cache reuse

Sealed PVL/KVI objects carry whole-file SHA-256 (`btdb_sha256`) committed atomically with their content; no sidecar
is needed. Length or version token alone never proves equal bytes.

A canonical TRL gets its whole-file SHA-256 in the same atomic write that seals it: every native file except the last
of a publication plan has a successor, so the publisher hashes the complete local file and sends it with that file's
final append (or, for a tail already published to its end, with one unchanged-content commit). Every other TRL
commit replaces the metadata without a checksum. Restore reuses a cached sealed TRL whose length and checksum match
and downloads the active tail again. Never hash a growing TRL.

### Remote cleanup and recovery roots

Only the leader deletes remote files, after a positively confirmed KVI makes them unnecessary; followers delete only
their own unpinned local files, and no deletion instruction is distributed. Cleanup:

- skips the pass if a higher KVI exists than the one just confirmed;
- keeps that KVI, its PVL/TRL dependencies, every canonical TRL from the oldest dependency onward (including
  intervening links) and the highest even ID;
- persistently marks other files with a deletion deadline, never extending an existing one, and deletes a file only
  after rereading it: the same version must still carry an elapsed deadline;
- clears the marks of files it keeps.

The deletion delay must be positive (production assumes at least a day), so a fenced predecessor's in-flight mark
cannot become due before the new leader protects the file. There are no follower acknowledgements or restore leases;
the delay does not guarantee that an old KVI remains restorable, and a restore that loses a file restarts from the
newest published KVI. Retired keys are never reused, so a delayed old delete stays harmless.

Discovery lists the shared native files and native KVI references select the required recovery closure. Old TRLs,
including genesis, can be pruned once the new checkpoint no longer needs them. `ResolveRecoveryRootAsync` returns
the oldest remaining TRL for bootstrap detection; a KVI without any retained TRLs fails instead of appearing empty.
Native open rejects missing required history. No recovery-root metadata or sidecar is stored.

## `denoland/celld` reference study

### Source map

`celld` has no small storage trait. Its practical interface is the concrete, cloneable `Bucket` adapter over
`Arc<dyn object_store::ObjectStore>`, which keeps an ordinary retrying store, a `PaginatedListStore`, a separate
`cas_store` with automatic retries disabled, a `StorageBackend` dialect selector, and an adapter-owned key prefix.
`BucketOwnership` maps ownership and node-lease records onto `Bucket`.

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

GCS does not apply `If-Match` to object PUT as needed, so it uses generations. Azure shares the ETag dialect with a
separate client and credential path. A missing or empty token is an error, because that version could not be fenced.
Bucket specifications are `s3://NAME[/PREFIX]`, `gs://NAME[/PREFIX]` or `az://NAME[/PREFIX]` (container for Azure);
callers always use unscoped keys. Each `Bucket::open` creates its own HTTP transport and connection pool.

### `Bucket` operations

| Group | Operations |
| --- | --- |
| Construction | `open`, `open_with_sources` (explicit GCS/Azure config), crate-private `open_dev`, test `with_stores`; `scheme`, `backend`; doc-hidden `gcs_replica_store*`/`azure_replica_store*` build separate retrying replica transports. |
| Objects | `get`/`head` return body or size plus required token, or `None`; unconditional `put`/`put_with_meta`; `head_with_meta` (no token); `put_cas`; idempotent `delete`; `delete_many` returns keys now gone. |
| Listing | `list` drains all pages; `list_any` stops at the first object; `common_prefixes_page` is bounded with `start_after`, page token and `max_keys`; `common_prefixes` drains it. |
| Extended (R2-style) | `head_blob`, ranged/conditional `get_blob`, conditional `put_blob`, `list_page`, `begin_multipart`, with `BlobRange`, `BlobConditions`, `BlobAttributes` and result types. |
| Validation | `validate` lists one object in the prefix; `probe_cas` runs the four-step probe; crate-private `probe_cas_steps` separates `Violation` from transient errors. |

`put_cas` semantics:

```text
token = None       -> provider conditional create
token = Some(T)    -> provider conditional update matching T

Ok(Some(newToken)) -> applied
Ok(None)           -> clean provider-enforced precondition rejection
Err(error)         -> ambiguous; the write may have committed
```

Only `object_store::Error::Precondition` and `AlreadyExists` count as clean rejections; an applied response without an
ETag or generation becomes an error. On Azure, `common_prefixes_page` rejects `start_after` because Azure listing has
no equivalent; continuation tokens still work. A conditional `put_blob` binds its read-side decision to the exact token
of the eventual write; its millisecond upload-time conditions are checked by the adapter because HTTP dates have
second precision. The CAS probe uses a random key and cleans up on every path; a failed cleanup can leave one small
object under `probe/`.

### Retry and transport behavior

At the pinned commit the ordinary store has two automatic retries and a 30-second retry timeout, the CAS store has
none, and request/connection timeouts are 15/3 seconds; these values are part of `celld`'s self-fencing timing
argument. The reusable rule is the separation: an isolated control lane with bounded request time and no hidden
conditional retries, so bulk transfers cannot starve lease renewal or reconciliation.

### `BucketOwnership` and the fencing pattern

`BucketOwnership` takes a normal bucket and an isolated lease-pool bucket. It reads and conditionally writes
`cells/<cell>/own.json` (`read_owner`, `cas_owner`, and `release_owner`, which rechecks node and epoch and uses that
read's token so it cannot erase a successor), and `nodes/<node>.json` (`read_node_lease`, `read_self_node_lease` via
the lease pool, `cas_node_lease`, `read_capacity_peers`). Outcomes hide provider details: `CasGuard = Absent |
Match(token)`, `CasOutcome = Applied | Rejected`, `LeaseCasOutcome = Applied { token } | Rejected`, with errors as the
ambiguous third state. The node lease body carries expiry, address, probe key, peer protocol, process generation,
load and a folded log state.

`celld` separates three concepts:

1. `own.json` names the owner session and a fencing epoch advanced on every activation.
2. `nodes/<node>.json` is a renewable process lease (default 10 s, renewed after one third); the process self-fences
   after its published expiry or when another writer replaces or removes the record.
3. Bulk SQLite/LTX data uses ordinary PUT under `cells/<cell>/ltx/e<epoch>/`; the epoch in the key fences a delayed
   old owner into a superseded prefix.

Small mutable authority uses CAS and bulk data uses unique fenced keys, which informed BTDB's leader record; BTDB now uses shared native TRL keys across terms. Differences:

- `celld` waits for a durability proof and rechecks ownership before acknowledging a write (RPO=0). BTDB does not,
  because the upstream event log recreates a discarded unpublished tail.
- `Bucket` has no append primitive; BTDB's conditional tail append is its own adapter capability.
- BTDB also streams TRL bytes directly to followers, so they validate term, session and chain before accepting bytes.
- `celld`'s lease timing is evidence for a pattern, not proof that BTDB's multi-node Azure failover timing is safe.

### Provider qualification lessons

The pinned guarantees name Amazon S3, Cloudflare R2, Tigris, Google Cloud Storage and Azure Blob Storage as qualified;
release tests use R2, and the S3-compatible path shares its client and headers. Backblaze B2, Hetzner Object Storage
and DigitalOcean Spaces lacked the required conditional writes. MinIO Community Edition passed the probe but was not
production-qualified, and one identified 2025 release had a conditional-create regression. Azure was qualified on
2026-08-18 (account key, VM managed identity, AKS workload identity) only in a single-node setup, which does not
qualify BTDB's leader election. Qualify behavior, not product names.

## Azure Blob Storage

Azure is the version-one provider: ETag CAS, conditional Block Blob commits that implement tail append, and
service-enforced finite Blob leases.

### Consistency and conditional writes

Azure Blob Storage is strongly consistent with snapshot-isolated reads; unconditional concurrent writes are
last-writer-wins. `If-None-Match: *` creates only if absent; `If-Match: <etag>` applies a write only to that version;
failures return HTTP `412`. ETags are opaque. A condition covers one blob operation, never several blobs.

- [Conditional headers](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations)
- [Concurrency model](https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage)
- [`Put Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob)

### Layout and metadata

`AzureLeaderStorage` uses one caller-supplied leader blob. It conditionally creates the initial JSON before the first
acquisition (empty-cluster bootstrap) and replaces it with both the lease ID and `If-Match`.

`AzureReplicationStorage` is constructed per database with its own nonempty prefix, because cleanup lists everything
below it. Native files live directly under this database prefix. Every term shares `{id}.trl`; immutable files use `{id}.pvl` and `{id}.kvi`.
IDs are positive unpadded decimals. `Bind(inventory)` gives a restore view and `Bind(inventory, authority)` a maintenance
view. Every publisher checks live authority before dispatch.

| Metadata | Object | Meaning |
| --- | --- | --- |
| `btdb_sha256` | PVL/KVI | Whole-file SHA-256, committed with the content. |
| `btdb_delete_after` | any | Invariant round-trip UTC deletion deadline. |

These are the only BTDB Blob metadata fields. TRL IDs come from filenames; lineage and checkpoint dependencies
come from native file contents. There are no term directories, successor pointers, recovery hints or extra manifests.
Earlier name/metadata formats are not supported. A create collision compares the existing native bytes; equal content
can be reused and an equal shorter prefix extended with version-bound CAS. Divergence requires restart.

### Conditional tail append with Block Blob

The failover core sees only the semantic append; the Block Blob layout stays in the adapter.

- A write expecting token `T` obtains `T`'s committed block list: from its own previous commit when `T` is that
  commit's ETag, otherwise from Get Block List, whose response ETag must equal `T`. The committed length must equal
  the expected length.
- Committed 4 MiB blocks are reused. Only the appended suffix is staged, up to four blocks at once, as new blocks
  with unique random IDs, so a losing request can never replace bytes a winning commit references. Trailing partial blocks accumulate up to 64 and
  are then merged into full blocks restaged from the verified local prefix, bounding block count and rewritten bytes.
- `Put Block List` with `If-Match: T` (or `If-None-Match: *` for a new TRL) atomically commits the new list and the
  TRL metadata: `btdb_sha256` on the write that seals the file, none otherwise. Adoption is an unchanged-content
  commit keeping every block.
- A transient failure after the commit was dispatched is `Ambiguous`; the publisher reconciles it by comparing the
  appended bytes. `404`, `409` and `412` are `Rejected`.

Relevant Azure constraints: `Put Block` has no normal conditional headers and staged blocks are invisible until
commit; `Put Block List` supports `If-Match` and may mix committed and newly staged blocks; block IDs have fixed length
per blob; a blob holds 50,000 committed and 100,000 uncommitted blocks, and uncommitted blocks expire after about a
week; with 4 MiB blocks committed capacity is about 195 GiB, above BTDB's per-TRL limit; `Put Block List` replaces
properties and metadata unless they are supplied again.

`BTDB.AzureStorage` uses the same stage-and-commit family with 128 KiB blocks but commits unconditionally; it is a
transfer reference, not the replication concurrency contract.

- [`Put Block`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block)
- [`Put Block List`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list)
- [`Get Block List`](https://learn.microsoft.com/rest/api/storageservices/get-block-list)

### Immutable PVL/KVI upload and cleanup

PVL/KVI uploads stage 4 MiB blocks, up to four concurrently (128 MB PVL on Azurite: 595 ms serially, 330 ms), hash
the stream, and commit with `If-None-Match: *` plus `btdb_sha256` in the same request.
Separate `Set Metadata` is never needed for durability. Reads bind to the listed ETag with `If-Match`.

Before a KVI commit, the adapter clears deletion marks on the retained canonical TRL chain, because a promoted follower
may depend on older sealed TRLs. It never rewrites an unmarked TRL, so ETags that concurrent restores read stay
valid. Deadline marking, mark clearing and PVL protection are ETag-conditional metadata writes that preserve other
metadata; deletion rereads properties and uses `If-Match`. Deadlines use an injected `TimeProvider` (system UTC by
default); lease authority uses its own monotonic scheduler. No Azure lifecycle rule is installed.

Azure `Content-MD5` is not a substitute for `btdb_sha256`: `Put Block List` stores a supplied whole-blob MD5 without
validating it and clears it if omitted, and per-request or per-block checksums yield no whole-file checksum.
[Get Blob Properties](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties) returns
metadata, length and ETag without the body.

### Native Blob leases

- A finite lease lasts 15-60 seconds (the adapter requires whole seconds); infinite leases also exist.
- Leases can be acquired, renewed, changed, released or broken; the adapter uses acquire, renew and change.
- Writes and deletes of a leased blob require the lease ID or fail with `412`; reads do not.
- `Lease Blob` operations do not change the blob ETag.
- A container lease protects container deletion only, not writes to its blobs.

`Change Lease` implements planned handoff without waiting for expiry: the source supplies its lease ID and the target's
proposed GUID, and the target renews that GUID to prove ownership, also when the source lost the response. Change keeps
the source's duration, so renewing a transferred or unknown handle assumes a conservative 15 seconds. A lost acquire
response is likewise confirmed by renewing the proposed ID. After a graceful-shutdown cut, the old leader dispatches no
data-plane write but may still reconcile earlier operations and renew or change the leader lease.

Reference: [`Lease Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob).

### Throughput and traffic isolation

Azure documents a target of up to 3,000 requests per second per block blob; hot partitions can return
`503 Server Busy` or `500 Operation Timeout`, so retryable data operations need bounded backoff. Publication coalesces
complete transactions instead of issuing one request per transaction, so throttling increases publication lag and
possible event replay rather than local commit latency. The leader blob is a deliberate serialization point and gets
no per-transaction traffic. Use separate authority and data clients so large transfers cannot occupy the authority
connection pool. Alert before remote publication lag approaches upstream event retention.

Reference: [Scalability targets](https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets).

### Evidence

**Azurite** (`azurite@3.35.0`, `BTDB.Replication.Azure.Test`): the actual SDK, conditional operations, deliberately lost
responses, lease expiry and transfer, block reuse, native publication, activation, checkpoint restore, delayed
cleanup, and restore of native BTDB after the genesis TRL prefix was deleted. Azurite results say nothing about live
availability, throttling or throughput.

**M1 live Azure probe, 2026-09-14**: [azure_probe.py](../BTDB.Replication.Test/Integration/azure_probe.py) ran against
a temporary Standard_LRS StorageV2 account (West Europe, REST `2023-11-03`, account key, no retries). All 24 requests
in the [recorded results](../BTDB.Replication.Test/Integration/azure-2026-09-14.json) returned the expected status;
the container, resource group and account were deleted afterwards. Observed: staged blocks did not change the
committed body or ETag; `Put Block List` changed content and metadata together; same-byte term adoption changed the
ETag and made an old append fail with `412`; lease acquire/change/renew left the leader ETag unchanged, blocked an
unleased write to that blob but not to another blob; after change, renewal with the old ID returned `409` and the new
ID succeeded. The probe used small diagnostic payloads (its diagnostic metadata is not part of the current production format), and
its ambiguity case only discarded a known successful response.

**Live Azure qualification, 2026-09-28**: `BTDB.Replication.Azure.Test` and `BTDB.Replication.Process.Test` ran with
`BTDB_AZURE_BLOB_ENDPOINT` against a Standard_LRS StorageV2 account in Sweden Central from an E8ads_v5 VM in the same
region, authenticated by its managed identity (Storage Blob Data Contributor, shared keys disabled, SDK retries off).
All 31 adapter tests passed in four consecutive runs and all 8 subprocess tests (leader kill, `SIGSTOP`, stalled
publication, divergence, follower crash) passed. Observed:

- A 15 s lease expired between 14.94 s and 15.06 s after its acquire was dispatched (24 trials, 50 ms polling); in
  one trial it was already free 14.997 s after dispatch, so the service does not guarantee the full duration from the
  client's dispatch and a zero safety margin is unsafe. With 1000 ppm drift and a 250 ms margin the local deadline
  passed at least 200 ms before the last reply that still reported the lease held, in every trial and also when the
  acquire reply was delayed by 3 s (`LocalLeaseDeadlineEndsWhileTheServiceStillHoldsTheLease`).
- Requests dispatched with valid authority and delivered after takeover were rejected: a TRL append by the adopted
  tail's ETag, a renewal and a leader-record write by the new lease
  (`DelayedPredecessorRequestsAreRejectedAfterTakeover`). A stale delete of a marked PVL lost to protection's version
  change, and a late mark of an unmarked dependency was cleared by the successor's cleanup long before due
  (`DelayedPredecessorCleanupCannotDeleteAFileTheSuccessorProtects`). Two sessions committing different PVL content to
  one identity: whichever lands second fences itself (`StalePureValuesUploadFencesWhicheverSessionLandsSecond`).
- Restores with 100 ms added to every read, while the leader published every transaction and checkpointed, compacted
  and marked files every few transactions, all opened complete, consistent states at least as new as the history
  published when they started, without retries (`RestoresCompleteConsistentlyWhileTheLeaderPublishesAndCollects`).
  Before the fix every such restore failed with `412` on a changed version. A restore whose selected files were
  deleted failed with `IOException` and a fresh discovery restored the newest state.

### Open Azure work

- Throttling and genuine network faults under production load. Credential renewal is qualified (a managed-identity
  token refreshed on every renewal keeps one lease authority); throughput, eight concurrently publishing databases and
  the 100 GB startup target are measured in [Measurements.md](Measurements.md).

## Amazon S3 (later research)

Ordinary S3 is a possible later target. Its portable behavior:

- `If-None-Match: *` creates only if absent; `If-Match: <etag>` replaces only the current object.
- A stale condition normally returns `412`; concurrent delete/write races may return operation-specific `409` or
  `404`, which need reconciliation.
- PUT and DELETE are strongly consistent with subsequent GET, HEAD and LIST; a single-key update is atomic.
- ETags are opaque tokens, not content hashes.
- Bucket policy can require `If-Match` or `If-None-Match`, preventing accidental unconditional writers.
- There is no object lease comparable to Azure Blob leases.

Ordinary S3 cannot append, so an S3 canonical TRL needs another representation (immutable range objects, sealed files
or chunks) with a qualified conditional publication step that still publishes whole transactions without adding a
second state commit. Without a service-evaluated lease, automatic takeover depends on a qualified deadline and
clock-uncertainty model; otherwise S3 mode freezes on ambiguous authority or requires explicit takeover.

S3 Express One Zone directory buckets offer append via `PutObject` with `WriteOffsetBytes` equal to the current length
(at most 5 GB per append, 10,000 parts per object; `CopyObject` can reset the part count). It is single-AZ and
non-portable, so it could only be a later optional optimization, not the failover contract.

- [Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
- [Appending data in S3 Express One Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html)
