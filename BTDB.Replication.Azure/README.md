# Azure replication adapters

This project implements the Azure SDK boundary for the existing internal replication components:

- `AzureLeaderStorage`: finite lease acquisition/renewal and lease-plus-ETag leader JSON replacement.
- `AzureCanonicalTrlStorage`: canonical metadata/read ranges and atomic conditional native TRL append/adoption.
- `AzureCheckpointStorage`: numeric PVL/KVI discovery, version-bound reads, native KVI streaming, and conditional
  immutable publication with atomically committed SHA-256 metadata.

Supply authenticated `BlobClient` / `BlobContainerClient` instances, the database prefix, initial leader JSON and
configuration. Configure SDK clients with `Retry.MaxRetries = 0`; protocol components own conditional-write ambiguity
and retries. Use separate authority/data clients so large transfers do not occupy the authority connection pool.
Authentication and container provisioning are supplied by the host. The project remains internal and non-packable,
matching the current replication API; it does not add another local file collection or a new database-open algorithm.

The initial leader JSON has `format: 1`, the expected `clusterId`, `term: 0`, `revision: 0`,
`applicationGeneration: 0` and `databaseNames: []`. Pass a fresh leadership session ID and API key on every acquisition.
Existing skip entries and unknown JSON fields are preserved during selection.

## Activation lifecycle

1. Restore required databases through the existing `ReplicationFileSet.InitializeAsync` and `BTreeKeyValueDB.OpenAsync`.
   Keep the verified startup cut separate from the advancing peer comparison acknowledgement. Retained canonical
   links can start at the TRL required by that restored KVI; history before the verified base is not compared again.
2. Run `LeaseSessionController.RunAsync` independently of application commits, transfer and activation work.
   Await its task on shutdown. A new `LeaseAuthority` instance signals a new acquisition; old instances remain fenced.
3. Construct one `LeaderSelection` and `LeadershipSession` for that acquisition. Supply every selected database by
   name, its restored base, canonical starting link, local capture and a new-term successor-key function.
4. Call `LeadershipSession.ActivateAsync`. A null selection result is unresolved; retry that same session while
   authority remains valid. I/O failures retry through discovery. `InvalidDataException` means canonical restore
   is needed; do not modify a live divergent suffix to make it pass. A generation floor rejection fences authority.
5. Only the returned publishers may start ordinary canonical publication. Their adoption phase never publishes
   local bytes beyond the selected Blob boundary. Matching optimistic local bytes may subsequently be published
   through the normal capture-backed publisher, without executing the application again.
6. Publish checkpoints using the existing `CheckpointPublisher`; it establishes TRL/PVL prerequisites before KVI
   staging. Give local maintenance and remote publication independent cancellation tokens. Do not reuse an expired
   authority object, leader selection, or publisher after reacquisition.

A missing retained file during takeover requires ordinary restore, not a special repair path or a retained-root registry.
The owner still handles application compatibility, new-database initialization and node lifecycle. This implementation
is not an ASP.NET transport, a schema-upgrade coordinator or remote garbage collection.

## Tests

Install `azurite@3.35.0` globally, then run:

```sh
dotnet test BTDB.Replication.Azure.Test/BTDB.Replication.Azure.Test.csproj
```

Tests start and stop an isolated loopback Azurite process with temporary storage. Set `BTDB_AZURITE_EXECUTABLE`
if the executable is not on PATH. Tests exercise the actual SDK, conditional HTTP operations, deliberately lost
responses, lease expiry, native publication, activation and checkpoint restore. No cloud account is accessed.

The adapter reuses committed 4 MiB prefix blocks and replaces the partial last block when appending, preventing
small commits from exhausting Azure's block-count limit. Staged block IDs are unique, so losing requests cannot
replace bytes referenced by a winning commit. Get Block List's response ETag is checked before its blocks are reused;
the final commit still carries `If-Match`. Sealed PVL/KVI files use `If-None-Match: *` and SHA metadata in that same commit.

Provider semantics were checked against Microsoft's [Lease Blob](https://learn.microsoft.com/rest/api/storageservices/lease-blob),
[Get Block List](https://learn.microsoft.com/rest/api/storageservices/get-block-list) and
[Put Block List](https://learn.microsoft.com/rest/api/storageservices/put-block-list) documentation.
Azurite results are not live Azure availability, throttling or throughput qualification.


Prepared handoff uses native lease Change after confirmation-grant drain. The target renews its proposed UUID to
confirm ownership, including when the source lost the Change response. Renewal of an unknown/transferred handle
uses a conservative fifteen-second duration because Change retains the source lease's duration. Leader discovery
conditionally creates the initial record before the first acquisition, allowing empty-cluster bootstrap.

Maintenance uses a physical, database-scoped listing and ETag-bound deletion under live session authority.
Reusing a PVL conditionally touches its metadata before KVI publication so a delayed delete from an earlier leader
cannot erase that dependency. Canonical writes include `btdb_file_id` metadata for native identity; older objects
without it are conservatively ignored by cleanup. Canonical TRL links are retained because this adapter discovers
history from a supplied root. PVL/KVI cleanup is supported; TRL pruning requires independently resolvable retained
roots and is not enabled by this adapter. Restore still uses the existing selected inventory and native open path.
