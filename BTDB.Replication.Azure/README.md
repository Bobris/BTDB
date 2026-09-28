# Azure replication adapters

This project implements the Azure SDK boundary of `BTDB.Replication` through public provider contracts:

- `AzureLeaderStorage`: `leader.json` with a finite native Blob lease (15–60 whole seconds): conditional initial
  creation, acquire/renew, lease `Change` for prepared handoff, and lease-plus-ETag record replacement. It implements
  `IReplicationLeaderStorage`.
- `AzureReplicationStorage` (`IReplicationStorage`): canonical TRL reads, conditional append and unchanged-content
  adoption, numeric PVL/KVI discovery, immutable publication with atomic SHA-256 metadata, shared native-file discovery and
  delayed conditional cleanup.

## Usage

Construct one data adapter per database with its own nonempty prefix; cleanup lists everything below that prefix.
Discover the canonical chain with `CanonicalTrlInventory.DiscoverAsync`, then bind it: `storage.Bind(inventory)` for
restore, `storage.Bind(inventory, authority)` for leader maintenance. Bindings are separate instances whose selected TRL
versions and authority never change. The [hosting guide](../Doc/ReplicationHosting.md) shows the complete wiring.

Supply authenticated `BlobClient` / `BlobContainerClient` instances; authentication and container provisioning belong
to the host. Configure SDK clients with `Retry.MaxRetries = 0`: replication reconciles conditional-write ambiguity
itself. Use separate authority and data clients so large transfers cannot occupy the authority connection pool. The
initial leader JSON has `format: 1`, the expected `clusterId`, `term: 0`, `revision: 0`, `applicationGeneration: 0`
and `databaseNames: []`. Transient provider failures and throttling surface as retryable `IOException`s.

## Behavior

- Canonical appends reuse committed 4 MiB blocks and restage only the suffix; trailing partial blocks are merged
  occasionally so small commits cannot exhaust Azure's block-count limit. Staged block IDs are unique, so a lost
  request cannot change a winning commit. The final `Put Block List` carries `If-Match` and no TRL protocol metadata.
  An append that expects this adapter's own previous commit reuses that block list instead of reading it again.
- PVL/KVI files are created with `If-None-Match: *` and SHA-256 metadata in the same commit; their blocks are staged up
  to four at a time (128 MB PVL on Azurite: 595 ms serially, 330 ms). Native KVI references
  define the recovery closure without additional metadata.
- Cleanup marks obsolete TRL/PVL/KVI objects with a deletion deadline and deletes only the unchanged marked version
  after it passes; the deadline survives leader changes. The delay must be positive. Reusing a PVL clears its mark and
  changes its version. Checkpoint publication clears marks on its retained TRL chain but never rewrites an unmarked
  TRL, so restores reading by ETag stay valid.
- Prepared handoff uses lease `Change` after the grant drain. The target renews its proposed ID to confirm ownership,
  including after a lost `Change` response; an unknown or transferred handle uses Azure's 15-second minimum duration.

Metadata keys, key layout and provider research are in [ObjectStorages.md](../BTDB.Replication/ObjectStorages.md).

## Tests

Install `azurite@3.35.0` globally, then run:

```sh
dotnet test BTDB.Replication.Azure.Test/BTDB.Replication.Azure.Test.csproj
```

Tests start an isolated loopback Azurite process with temporary storage (set `BTDB_AZURITE_EXECUTABLE` if it is not on
PATH) and exercise the actual SDK with conditional operations, lost responses, lease expiry and transfer, native
publication, activation, checkpoint restore and cleanup. To qualify live Azure instead, set
`BTDB_AZURE_BLOB_ENDPOINT=https://<account>.blob.core.windows.net`; `DefaultAzureCredential` then needs Storage Blob
Data Contributor on that account, and each test uses its own temporary container. The recorded live run is in
[ObjectStorages.md](../BTDB.Replication/ObjectStorages.md).

A restore keeps reading a selected object after the leader changes its version, as long as the new version provably
keeps the selected bytes: a canonical TRL that is at least as long, or a PVL/KVI with the same length and
`btdb_sha256` (deletion marks change only metadata). Provider semantics were checked against Microsoft's
[Lease Blob](https://learn.microsoft.com/rest/api/storageservices/lease-blob),
[Get Block List](https://learn.microsoft.com/rest/api/storageservices/get-block-list) and
[Put Block List](https://learn.microsoft.com/rest/api/storageservices/put-block-list) documentation.
