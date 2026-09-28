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

The Bearer API key is authenticated against the active leader before reading or decoding the request body. The full
term/session identity is checked after decoding and again after awaited operations, including key rotation or fencing.

Each follower step sends the leader one poll: the progress, published cut and latest schema-commit position of every
compared database and of every new database the leader selects, plus a single confirmation grant. For each compared
database the follower also sends the position its comparison resumes from, and the leader returns its complete TRL
bytes from there inline (up to 1 MiB per poll), so a caught-up follower needs no separate range reads, even across TRL
rotations. The same request, with no databases, is the authority heartbeat of a schema-detached node.

Peer messages use a compact versioned binary encoding (`application/octet-stream`), not JSON. Requests are limited to
16 KiB; a poll response may add the requested inline budget. Inline chunk bytes are sliced from the response without
copying.

Optional `DetachedLeaderTimeout` (default 15 minutes) is how long a schema-detached node may go without leader
evidence before it requests a restart.

`RequestTimeout` bounds each leader discovery and follower control/comparison step. Leader activation, TRL
publication and checkpoint maintenance transfer bulk data and are not cut off by it; lease loss cancels them, and
`ProgressTimeouts` bounds stalled work. Checkpoint maintenance runs beside publication, so a long PVL/KVI upload
never holds back canonical TRL publication. Azure adapters report throttling and other transient provider failures
as retryable `IOException`s.

A node can win the lease while its local execution is still behind already published history. Activation then
keeps the lease and waits: the host must continue executing ordered inputs while the role is `Activating`, and each
retry validates only newly completed local bytes. Only mismatching bytes or an incompatible local continuation require
a restart and canonical restore. Enable the activation progress deadline to bound a candidate that stops catching up.

## Restore and application ownership

`RestoreAsync` must restore every required published database before returning `ActivationDatabase` entries. Discover
shared native TRL objects with `CanonicalTrlInventory.DiscoverAsync`, combine them with the complete native PVL/KVI
inventory through `AzureReplicationStorage.Bind(inventory)`, initialize `ReplicationFileSet`, and call `BTreeKeyValueDB.OpenAsync`.
Writable legacy open conditionally publishes a header-only successor before returning; all nodes derive its ID from
that common remote inventory. It needs no opt-in flag or leader lease. Use writable `IReplicationStorage` as the remote
adapter for automatic conversion. An extended successor causes a restore retry; conflicting headers request restart.
For Azure, use one data adapter per database:

```csharp
var storage = new AzureReplicationStorage(dataContainer, databasePrefix);
var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, selectedRoot, cancellation);
var remote = storage.Bind(inventory); // Restore and create-only legacy bootstrap need no leadership authority.
var files = new ReplicationFileSet(localFiles, remote);
await files.InitializeAsync(cancellation);
// During CreateMaintenance, bind the same inventory with the supplied session authority:
// IReplicationStorage maintenanceStorage = storage.Bind(inventory, authority);
```

`IReplicationStorage` combines TRL, immutable-file publication and maintenance operations. `IRemoteFileCollection`
remains the read-only inventory contract accepted by `ReplicationFileSet`. A binding preserves its selected TRL
versions and optional authority; create a new binding after acquisition rather than modifying the old one.

The supplied `TransactionLogCapture` belongs to that opened database. `RestoredBase` is the fixed verified startup
file/offset (`BTreeKeyValueDB.ReplicationRestoredPosition`); it is not the follower's moving comparison acknowledgement
or the physical end of an unfinished Blob transaction. Return `{id}.trl` for every term. A zero restored base denotes an unpublished addition, never a missing
published file. Failed restore attempts must release their resources before retry.

`ReplicationFileSet` keeps local operations local. Initialize the remote inventory explicitly; local counts, lookup
and enumeration never mean remote inventory. Production nodes use `OnDiskReplicationFileStorage(directory)`: one
memory-mapped `{id:D8}.{hint}` file per ID in a node-private directory, surviving process restarts. After a crash, files
may end with zero padding; restore validates cached files against the selected remote inventory and discards the rest,
so reuse the same directory on restart. `InMemoryReplicationFileStorage` is intended for tests. Both implement
`IReplicationFileStorage`.
The application owns the database and backing storage and disposes them only after coordination has stopped.

The application owns the ordered event stream and handler execution. All nodes apply the same inputs in the same
order using `StartWritingTransaction(eventId)`. Local commits never wait for peers or Blob. Publish an atomic
`LeaderTrlProgress` through `GetProgress` only after complete local work and before starting the next application
transaction. This is a completed native cut, not a durability acknowledgement or proof of reader visibility during
virtual batching. `ReportStatus` reports the node role, not a comprehensive readiness/lag signal.

`CreateCandidate` must return fresh session and API-key values for each acquisition, with the configured cluster,
endpoint, generation and complete selected database set. `CaptureInitializationCursorAsync` supplies the predecessor
cursor for a genuinely new database; keep application input for that database stopped until it is initialized.
`PrepareSchemaAsync` uses ordinary ObjectDB startup initialization, must be idempotent across interrupted attempts,
rechecks the supplied authority before writes/commit and updates `GetProgress` with the completed cut, including
genesis or schema-only work. The coordinator publishes genesis/schema before serving as leader.

## Upgrades, schema detachment and leak events

Set `PreparedUpgrade` only after the host has validated compatibility, bounded lag with retained replay input, and
every added/removed database requirement. Use a fresh transfer UUID per prepared target and keep it across retries.
Only an offer of a higher generation than the leader's starts a handoff; among equal offers the first observed wins.
The leader drains grants and transfers through `IReplicationLeaseTransferStorage` (Azure native lease Change); the
target proves ownership by renewal and then activates normally.

`SchemaDetached` is called when the leader announces a schema commit beyond the follower's canonical base, before
comparison. Report that database as local-only and keep serving its ordinary reads/writes; the node can no longer
become leader in this session. `DatabaseRemoved` means the selected database set no longer contains the database;
it stays application-owned and continues locally.

Pass the ObjectDB and a `publishLeakEvent` callback to `ReplicationMaintenance` to submit bounded
`LeakRemovalCandidates` as an application event. Transport its `EncodedKeys` and `KeyCount`, and apply it on every
replica with `ApplyTo(transaction)` inside that event's ordinary transaction and commit. The application owns ordering
and retries; do not use candidates after restoring a different database history. Replicated compaction disables
automatic leak erasure; standalone ObjectDB behavior is unchanged.

## Maintenance and authority

`CreateMaintenance` may construct `ReplicationMaintenance` with the existing file set, supplied canonical publisher,
an authority-bound `IReplicationStorage` (such as `AzureReplicationStorage`), scheduler, checkpoint interval and a
positive deletion delay. The coordinator separately runs ordinary local `Compact` on every node every
`CompactionInterval` (default five minutes). These callbacks run within the coordinator's serialized leader publication lane. The publisher is borrowed:
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

Coordinator transitions, lease selection/activation, peer sessions, comparers and HTTP wire DTOs remain
internal. The public surface provides application and provider integration, not independent election control.

Still pending: production clock qualification, full readiness/metrics, broader network/process-pause/upgrade scenarios,
broader lifecycle qualification, live-Azure/performance
qualification and release packaging. Public accessibility does not mark replication production-ready.

## Readiness and progress

`AddBTDBReplication` registers a thread-safe `ReplicationStatus` singleton and the standard ASP.NET health check
`btdb-replication` with the `ready` tag. Map it explicitly using the application's routing/access policy:

```csharp
app.MapHealthChecks("/health/ready", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = registration => registration.Tags.Contains("ready")
});
// Application diagnostics can read this without contacting storage or peers:
var status = app.Services.GetRequiredService<ReplicationStatus>().Current;
```

Readiness means restoration/initialization finished and the node can serve ordinary local work. Restore, activation,
schema detachment, restart and shutdown are unhealthy. A disconnected restored follower may remain ready: neither
live confirmation nor Blob publication is required for local work. This check does not grant leadership authority.
Shutdown clears readiness synchronously even if a provider ignores cancellation; late callbacks cannot restore it.

Each immutable sample reports the monotonic sample time, role and per-database completed local, compared and published
cuts plus removal/detachment flags. Null cuts are unavailable. `Compared` is historical byte equality, not an unexpired
confirmation grant. A follower whose local execution lags the leader still compares the complete local prefix the
leader's cut covers; `Compared` then reports the host's local progress at that cut. `Published` is available only from this node's active publisher. Local completed progress comes
from the host and does not claim reader visibility during virtual batching. Do not use these sampled cuts to gate
writes, confirmed-only reads or external effects. Samples update on coordinator transitions/polls; blocked operations
can make them stale. The application owns reader-visible progress and input lag reporting.

The host-scoped `BTDB.Replication` meter exports `btdb.replication.ready` (0/1), `btdb.replication.role`
(`ReplicationNodeRole` numeric value), and `btdb.replication.status.age` (seconds since the last sample).
Instruments have no database/node/endpoint labels or credential values. Configure collection/export through the host's
normal .NET metrics pipeline. No diagnostic HTTP endpoint is automatically exposed.

## Pending-work deadlines and fatal recovery

Set `ReplicationNodeOptions.ProgressTimeouts` to a `ReplicationProgressTimeouts` with deployment-qualified activation
and publication no-progress budgets plus a fixed `RestartDelay`. All three values must be positive. The default is disabled; replication does not
infer an application handler timeout. When enabled, the registered `IReplicationNodeHost` must also implement
`IReplicationFatalRecovery`. Missing support fails before restore or lease acquisition. For example,
`new ReplicationProgressTimeouts(Activation: TimeSpan.FromMinutes(2), Publication: TimeSpan.FromMinutes(1),
RestartDelay: TimeSpan.FromSeconds(90))`; qualify these values for the deployment.

Activation starts its deadline after acquisition, before candidate preparation. Forward selection, canonical inventory
links, validated byte ranges, adoption and initialization/schema preparation/publication advance a local watermark.
Repeating earlier steps after I/O failure does not renew the budget. Each database gets a publication deadline only
when the coordinator observes a completed local cut beyond its publisher's confirmed cut. Confirmed cut advancement
extends that budget; reaching the local cut clears it. Staged upload bytes and ambiguous writes are not confirmed
publication. Choose a budget that accommodates the largest whole transaction's transfer and reconciliation.

On expiry, the coordinator permanently fences lease acquisition/renewal and publication and clears readiness before
scheduling `RequestFatalRestart` after `RestartDelay`. The delay runs on the injected scheduler, independently of
the stalled worker and its cancellation callbacks. Renewal stays disabled throughout the wait; other nodes can
acquire the lease after its provider-side expiry. Choose the delay to cover the provider lease lifetime and an
opportunity for healthy followers to acquire it. The host must immediately initiate bounded non-graceful termination/restart, must not wait for handlers or
coordinator disposal, and must never reuse the failed session. A bounded best-effort diagnostic flush may precede
termination. Merely calling `StopApplication` is insufficient when providers or handlers ignore cancellation.
The subprocess test host demonstrates an immediate process exit; production logging/termination policy belongs to
the application. Late provider replies cannot restore authority. Graceful shutdown cancels outstanding watchdogs. Once a watchdog has chosen fatal recovery, shutdown or a late
worker response cannot cancel its restart timer or make the coordinator return before the delay elapses.

These deadlines monitor activation and already-observed pending canonical publication. They do not classify application
failures, generate skip markers, watch idle input, or time follower restore. Application execution/commit arbitration
and all event timeouts/skips are application-owned, outside replication scope.
Set optional `ReplicationProgressTimeouts.Maintenance` to a positive no-progress budget for each remote maintenance
lane. It covers snapshot capture, checkpoint prerequisites, PVL placement/protection, KVI publication and cleanup.
Only forward completed steps extend the deadline; retries retain their watermark and completed-checkpoint cleanup
state. Idle intervals and the application-owned leak-event submission callback are excluded. Progress is measured
at whole-file/operation boundaries, so allow enough time for the largest PVL/KVI transfer or serialization. Expiry
uses the same immediate fencing and delayed fatal restart. Input retention remains application-owned. The injected
scheduler must continue servicing deadlines while a worker is blocked, and callbacks must be serialized as usual.

After restart, all required databases must restore before lease contention. Restore can be fast with small databases
or reusable cache, so it is not a guaranteed substitute for the delay. There is no persistent attempt counter,
backoff state file or activation-state storage registration. Crashes outside watchdog recovery are throttled by the
host supervisor's restart policy. Replication does not delay or skip application events.

## Application data in leader.json

`AddBTDBReplication` registers `ReplicationApplicationData`. Any node may read; only an active local leader may write.
The service exposes only the opaque `applicationData` field, not peer credentials or editing of protocol fields:

```csharp
var applicationData = app.Services.GetRequiredService<ReplicationApplicationData>();
var snapshot = await applicationData.ReadAsync(cancellation);
var value = snapshot.Value?.AsObject() ?? new System.Text.Json.Nodes.JsonObject();
value["mySetting"] = "myValue";
var outcome = await applicationData.TryWriteAsync(snapshot, value, cancellation);
```

Pass `null` to clear the value. A snapshot belongs to the service that read it. Each write uses its exact ETag plus the
current lease; a successful update increments leader-record revision and preserves term/session, database names and
all other fields. Concurrent writers cannot silently overwrite one another. New leaders preserve the application data.
Applications may read it during startup before restoring/processing input. Initialization/activating nodes, followers,
draining leaders and stopped/fenced sessions cannot use the write API.

`Applied` confirms the storage effect, not continued leadership. `Rejected` means this attempt was rejected (stale
version/session or no active local leadership). `Ambiguous` means the effect is unresolved: the original write might
still land. Retrying the same snapshot/value keeps the original CAS condition and can reconcile an exact already-landed
intent. Cancellation or an exception after dispatch can also leave an applied write. Do not blindly reread and reapply
an unresolved operation over newer state; the application must resolve its intended semantics. A later `Rejected`
result never proves that an earlier ambiguous attempt had no effect.

JSON updates are independent of database transactions. BTDB does not interpret the payload, enforce its schema,
expire it, observe application handlers, arbitrate event timeouts/commits, or skip events because of it. These policies
belong entirely to the application. The existing activation/publication watchdogs monitor replication work only.

## Operational backup recovery

Scale the cluster to zero, copy the backup into primary Blob storage, then scale up. Startup restores and validates
all required databases before election. This procedure needs no additional backup-import API or forced stream reset.

## Delayed remote cleanup

Pass a positive retention interval (production assumes at least `TimeSpan.FromDays(1)`) as
`ReplicationMaintenance.deletionDelay`. The leader marks obsolete TRL/PVL/KVI in Blob metadata and later deletes only
the unchanged marked version after its deadline. A database whose TRL cut and files are unchanged is not exported again,
but cleanup still runs so marked files reach their deadline. Each database needs its own nonempty Azure prefix. The deadline survives process/leader replacement. The adapter's optional `TimeProvider` supplies UTC;
no Azure lifecycle policy is installed automatically. A new leader rechecks reused PVLs, clears their deletion mark,
and recopies missing files at fresh IDs. Old delayed deletes cannot remove a newly protected version.

Use `CanonicalTrlInventory.DiscoverAsync` on the Azure storage and bind the result with `storage.Bind(inventory)`
for restore, exposing native KVI/PVL and TRLs from the shared namespace. Native KVI references select the required
closure. Do not treat a missing original genesis alone as an empty database: `ResolveRecoveryRootAsync` locates the
oldest retained TRL and rejects a KVI with no TRL history. Native open rejects missing required history. The subprocess
host demonstrates startup without recovery-root metadata.
