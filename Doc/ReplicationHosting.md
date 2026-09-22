# Hosting BTDB replication

The source projects now expose public application/storage contracts and ASP.NET hosting extensions. They remain
non-packable while production qualification continues. Reference `BTDB.Replication.Http` and, for Azure storage,
`BTDB.Replication.Azure`. No `InternalsVisibleTo` access is needed. The
[subprocess application](../BTDB.Replication.Process.Test/Program.cs) builds and runs using only these public APIs.
Its loopback controls, test clock and simulated application input are test infrastructure, not production defaults.

## Composition

Register one node per ASP.NET host. The application supplies:

| Service | Responsibility |
| --- | --- |
| `IReplicationNodeHost` | Restore databases, supply fresh candidate identity, report completed application progress, prepare genesis/schema and handle restart/removal/detachment. |
| `ILeaderRecordStorage` | Version-bound leader-document reads and lease-plus-version conditional replacement. |
| `IReplicationLeaseStorage` | Confirm exclusive finite lease acquisition/renewal; return no grant when ownership is uncertain. |
| `IReplicationScheduler` | Node-local monotonic elapsed time and serialized scheduled callbacks, including the required process/OS pause and clock-drift behavior. |

The same Azure leader adapter implements both storage interfaces and native prepared lease transfer. Clients are
created and authenticated by the application. Configure SDK retries to zero because replication reconciles conditional
write ambiguity. Keep authority clients separate from data-transfer clients. For example, inside application setup:

```csharp
// application, scheduler and authorityContainer are supplied by the host.
var leaderStorage = new AzureLeaderStorage(
    authorityContainer.GetBlobClient("cluster/leader.json"),
    TimeSpan.FromSeconds(15),
    """{"format":1,"clusterId":"my-cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """);

builder.Services.AddSingleton<IReplicationNodeHost>(application);
builder.Services.AddSingleton<IReplicationScheduler>(scheduler);
builder.Services.AddSingleton<ILeaderRecordStorage>(leaderStorage);
builder.Services.AddSingleton<IReplicationLeaseStorage>(leaderStorage);
builder.Services.AddBTDBReplication(
    new ReplicationNodeOptions("my-cluster", "https://node.example", pollInterval,
        leaseRetryInterval, requestTimeout, confirmationDuration, applicationGeneration),
    maximumClockDriftPpm, safetyMargin);

var app = builder.Build();
app.MapBTDBReplication();
await app.RunAsync();
```

Intervals, drift and safety margin are host choices requiring deployment qualification. DI deliberately installs no
wall-clock scheduler fallback. The HTTP endpoint must be an HTTPS origin without a path; HTTP is accepted only on
loopback. The adapter maps `POST /_btdb/replication`. TLS certificates, routing and external authentication to Blob
storage belong to the host. Do not log Authorization headers or raw leader JSON.

## Restore and application ownership

`RestoreAsync` must restore every required published database before returning `ActivationDatabase` entries. Discover
selected canonical links with `CanonicalTrlInventory.DiscoverAsync`, optionally combine them with native PVL/KVI
inventory through `AzureReplicationStorage.Bind(inventory)`, initialize `ReplicationFileSet`, and call `BTreeKeyValueDB.OpenAsync`.
For Azure, use one data adapter per database:

```csharp
var storage = new AzureReplicationStorage(dataContainer, databasePrefix);
var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, selectedRoot, cancellation);
var remote = storage.Bind(inventory); // Read-only restore; no leadership authority needed.
var files = new ReplicationFileSet(localFiles, remote);
await files.InitializeAsync(cancellation);
// During CreateMaintenance, bind the same inventory with the supplied session authority:
// IReplicationStorage maintenanceStorage = storage.Bind(inventory, authority);
```

`IReplicationStorage` combines TRL, immutable-file publication and maintenance operations. `IRemoteFileCollection`
remains the read-only inventory contract accepted by `ReplicationFileSet`. A binding preserves its selected TRL
versions and optional authority; create a new binding after acquisition rather than modifying the old one.

The supplied `TransactionLogCapture` belongs to that opened database. `RestoredBase` is the fixed verified startup
file/offset; it is not the follower's moving comparison acknowledgement. Preserve the actual selected root/key and
return a fresh-session successor-key function. A zero restored base denotes an unpublished addition, never a missing
published file. Failed restore attempts must release their resources before retry.

`ReplicationFileSet` keeps local operations local. Initialize/refresh remote inventory explicitly; local counts, lookup
and enumeration never mean remote inventory. The current built-in backing is `InMemoryReplicationFileStorage`.
The application owns the database and backing storage and disposes them only after coordination has stopped.

The application owns the ordered event stream and handler execution. All nodes apply the same inputs in the same
order using `StartWritingTransaction(eventId)`. Local commits never wait for peers or Blob. Publish an atomic
`LeaderTrlProgress` through `GetProgress` only after complete local work and before starting the next application
transaction. This is a completed native cut, not a durability acknowledgement or proof of reader visibility during
virtual batching. `ReportStatus` reports the node role, not a comprehensive readiness/lag signal.

`CreateCandidate` must return fresh session and API-key values for each acquisition, with the configured cluster,
endpoint, generation and complete selected database set. `CaptureInitializationCursorAsync` supplies the predecessor
cursor for a genuinely new database. `PrepareSchemaAsync` uses ordinary ObjectDB startup initialization and rechecks
the supplied authority before writes/commit. The coordinator publishes genesis/schema before serving as leader.

## Maintenance and authority

`CreateMaintenance` may construct `ReplicationMaintenance` with the existing file set, supplied canonical publisher,
an authority-bound `IReplicationStorage` (such as `AzureReplicationStorage`), scheduler and maintenance/deletion
intervals. These callbacks run within the coordinator's serialized leader publication lane. The publisher is borrowed:
do not dispose it, retain it for another leadership session or start a separate publication loop. The coordinator owns
the returned maintenance job. Local compaction has separate lifetime/cancellation from remote publication.

`LeaseAuthority` is supplied by the coordinator. Applications/adapters may inspect its deadline/validity or fence it,
but its constructor and renewal operations are internal. Fencing is permanent. Provider port implementations must
honor the documented conditional-write, version-bound read and ambiguity contracts; public data records do not grant
authority. `LeaderRecord`, `LeaderCandidate` and `LeaseGrant` redact credentials from `ToString`; raw JSON/properties
still contain credentials and must not be logged by destructuring/serialization.

## Startup, shutdown and current limits

The hosted worker starts after Kestrel is listening. Missing routes and invalid/duplicate registration fail early.
Host stopping immediately fences lease authority and cancels coordination; the hosted task joins cleanup. A provider
ignoring cancellation cannot renew the old authority, but may still consume the host's shutdown deadline. Application
writers/readers and database disposal remain host-owned. Rebuild requests and fatal worker exits stop the host, even
when its background-failure policy is `Ignore`; the supervisor must implement process restart/termination policy.

Coordinator transitions, lease selection/activation, peer sessions, comparers, scanners and HTTP wire DTOs remain
internal. The public surface provides application and provider integration, not independent election control.

Still pending: production clock qualification, full readiness/metrics, broader network/process-pause/upgrade scenarios,
exact-skip and backup-reset integration, Azure retained-root discovery for TRL pruning, live-Azure/performance
qualification and release packaging. Public accessibility does not mark replication production-ready.
