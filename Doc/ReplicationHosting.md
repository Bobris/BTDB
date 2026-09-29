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
| `IReplicationNodeHost` | Restore databases, supply fresh candidate identity, report completed application progress, prepare genesis/schema and handle restart, fatal restart, removal and detachment. |
| `IReplicationLeaderStorage` | Confirm exclusive finite lease acquisition/renewal (no grant when ownership is uncertain), transfer it for a prepared handoff, and read/replace the leader document conditionally on lease plus version. |
| `IReplicationScheduler` | Optional: node-local monotonic elapsed time and serialized scheduled callbacks. Defaults to `SystemReplicationScheduler` (see *Clock and lease settings*). |

`AzureLeaderStorage` implements it with a native Blob lease on `leader.json`. Clients are
created and authenticated by the application. Configure SDK retries to zero because replication reconciles conditional
write ambiguity. Keep authority clients separate from data-transfer clients. For example, inside application setup:

```csharp
// application and authorityContainer are supplied by the host.
var leaderStorage = new AzureLeaderStorage(
    authorityContainer.GetBlobClient("cluster/leader.json"),
    TimeSpan.FromSeconds(15),
    """{"format":1,"clusterId":"my-cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """);

builder.Services.AddSingleton<IReplicationNodeHost>(application);
builder.Services.AddSingleton<IReplicationLeaderStorage>(leaderStorage);
builder.Services.AddBTDBReplication(
    new ReplicationNodeOptions("my-cluster", "https://node.example", pollInterval,
        leaseRetryInterval, requestTimeout, confirmationDuration, applicationGeneration),
    maximumClockDriftPpm: 1000, safetyMargin: TimeSpan.FromMilliseconds(250));

var app = builder.Build();
app.MapBTDBReplication();
await app.RunAsync();
```

Intervals, drift and safety margin are host choices requiring deployment qualification.

### Clock and lease settings

`SystemReplicationScheduler` measures lease deadlines with a clock that time synchronization never adjusts and that
keeps running while the process is stopped: `CLOCK_MONOTONIC_RAW` on Linux and macOS, interrupt time
(`Environment.TickCount64`, 10–16 ms resolution) on Windows. Linux `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME`, which
`Stopwatch` uses, are slewed by NTP (chrony by up to 8.3 % while it corrects an offset), so a custom scheduler must not
use them. Qualified on Azure E-series VMs (see [M1Evidence.md](../BTDB.Replication/M1Evidence.md)):

- `maximumClockDriftPpm: 1000` covers the hardware counter against the service clock with a large reserve.
- `safetyMargin` of at least 250 ms: a 15 s Azure lease was observed free as early as 14.997 s after its acquire was
  dispatched and at most 15.06 s after it, and the Windows clock resolution must fit in the margin as well.
- Do not run a node on a host that suspends (laptops, hibernating VMs): the raw clock stops during system suspend. A
  hypervisor freeze of the whole VM may stop every guest clock. Neither can corrupt published history, because every
  durable effect is conditional at the service; a frozen former leader only wastes requests until it fences. The HTTP endpoint must be an HTTPS origin without a path; HTTP is accepted only on
loopback. The adapter maps `POST /_btdb/replication`. TLS certificates, routing and external authentication to Blob
storage belong to the host. Do not log Authorization headers or raw leader JSON.

The Bearer API key is authenticated against the active leader before reading or decoding the request body. The full
term/session identity is checked after decoding and again after awaited operations, including key rotation or fencing.

Each follower step sends the leader one poll: the progress, published cut and latest schema-commit position of every
compared database and of every new database the leader selects, plus a single confirmation grant. For each compared
database the follower also sends the position its comparison resumes from, and the leader returns its complete TRL
bytes from there inline (up to 4 MiB per poll), so a caught-up follower needs no separate range reads, even across TRL
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

The leader may keep publishing, checkpointing and marking files during a restore: the attempt keeps reading its
selected bytes from newer versions that provably contain them and ignores checkpoints published after discovery. It
fails with retryable `IOException` only when a selected file was deleted, so keep the deletion delay well above the
longest restore.

`IReplicationStorage` combines TRL, immutable-file publication and maintenance operations. `IRemoteFileCollection`
remains the read-only inventory contract accepted by `ReplicationFileSet`. A binding preserves its selected TRL
versions and optional authority; create a new binding after acquisition rather than modifying the old one.

The supplied `TransactionLogCapture` belongs to that opened database. `RestoredBase` is the fixed verified startup
file/offset (`BTreeKeyValueDB.ReplicationRestoredPosition`); it is not the follower's moving comparison acknowledgement
or the physical end of an unfinished Blob transaction. Return `{id}.trl` for every term. A zero restored base denotes an unpublished addition, never a missing
published file. Failed restore attempts must release their resources before retry.

`ReplicationFileSet` keeps local operations local. Initialize the remote inventory explicitly; local counts, lookup
and enumeration never mean remote inventory. Production nodes use `OnDiskReplicationFileStorage(directory)`: one
`{id:D8}.{hint}` file per ID in a node-private directory, surviving process restarts. Files being appended keep up to
64 MiB of their newest bytes in memory (plus the current 1 MiB block), and readers never lock. After a crash, a file
may lack its last unflushed block; restore validates cached files against the selected remote inventory and discards
the rest, so reuse the same directory on restart. `InMemoryReplicationFileStorage` is intended for tests. Both implement
`IReplicationFileStorage`.
The application owns the database and backing storage and disposes them only after coordination has stopped.

The application owns the ordered event stream and handler execution. All nodes apply the same inputs in the same
order using `StartWritingTransaction(eventId)`. Local commits never wait for peers or Blob. Publish an atomic
`LeaderTrlProgress` through `GetProgress` only after complete local work and before starting the next application
transaction. After a restore, report the restored cut (its `CommitUlong` and
`BTreeKeyValueDB.ReplicationRestoredPosition`) until the first local transaction: a leader that reports no progress
gives its followers nothing to compare, so they cannot advance or release their local history. This is a completed native cut, not a durability acknowledgement or proof of reader visibility during
virtual batching. `ReportStatus` reports the node role, not a comprehensive readiness/lag signal.

`CreateCandidate` must return fresh session and API-key values for each acquisition, with the configured cluster,
endpoint, generation and complete selected database set. `CaptureInitializationCursorAsync` supplies the predecessor
cursor for a genuinely new database; keep application input for that database stopped until it is initialized.
`PrepareSchemaAsync` uses ordinary ObjectDB startup initialization, must be idempotent across interrupted attempts,
rechecks the supplied authority before writes/commit and updates `GetProgress` with the completed cut, including
genesis or schema-only work. The coordinator publishes genesis/schema before serving as leader.

### Application-owned event execution

A minimal event loop, run identically on every node; `input` is the application's own ordered stream (an event store,
a Kafka partition), and `progress` holds the latest `LeaderTrlProgress` that `GetProgress` returns:

```csharp
using (var read = db.StartReadOnlyTransaction())
    input.Seek(read.GetCommitUlong() + 1); // Resume after the restored or last applied event.
await foreach (var e in input.ReadAsync(cancellation))
{
    using (var tr = await db.StartWritingTransaction(e.Id))
    {
        Apply(tr, e); // Deterministic: every node must write the same bytes for the same event.
        tr.Commit();
    }
    using var read = db.StartReadOnlyTransaction();
    var cut = capture.Completed; // Complete local work, before the next transaction starts.
    progress = new LeaderTrlProgress(read.GetCommitUlong(), cut.FileId, cut.Offset);
}
```

Keep the loop running in every role, including `Activating`: a lease winner adopts published history only after its
own execution reached it. External effects (messages, payments) are not coordinated by replication; drive them from
application state idempotently, for example through an outbox that only the current leader drains.

### Input retention and restarts

- **Retain input** from the event after the oldest published `CommitUlong` any node may restore until that node has
  caught up. A restoring node resumes after the published history and re-executes everything after it: the former
  leader's unpublished tail plus everything that arrived during restore. Size retention above the worst publication lag
  plus the longest restore and restart, and alert on publication lag well before it approaches retention.
- **Restart on request.** `RequestRestart` and `RequestFatalRestart` expect the supervisor
  (for example the Kubernetes restart policy) to start a fresh process; the host stops its HTTP endpoint first. The
  new process restores every required database before it contends for the lease.
- **Keep the node-local directory** across restarts (a `hostPath` or persistent volume, not the container's temporary
  directory), so a restart validates cached files instead of downloading them; a lost directory only makes the next
  restore cold.
- **Keep the deletion delay** (at least a day) far above the longest restore, and the TRL retention of the input
  source above it too.

### Recommended settings

Values qualified by the subprocess tests on live Azure; measure restore and export times for the real database size.

| Setting | Value | Why |
| --- | --- | --- |
| Lease duration (`AzureLeaderStorage`) | 15 s | Azure minimum; takeover after a crash takes one lease plus activation. |
| `maximumClockDriftPpm`, `safetyMargin` | 1000 ppm, 250 ms | See *Clock and lease settings*. |
| `PollInterval` | 50–200 ms | Comparison latency against one small request per follower per interval. |
| `LeaseRetryInterval` | 250 ms | Acquisition retries only; renewals schedule themselves within the lease. |
| `RequestTimeout` | 2 s | Must be below half of the usable lease (validated when the first lease is acquired). |
| `ConfirmationDuration` | 1 s | Planned handoff waits this long; must be below half of the usable lease (validated). |
| Checkpoint interval (`ReplicationMaintenance`) | 1 h | Bounds TRL replay on restore; a checkpoint of 100 GiB uploads at 330–450 MiB/s, so its KVI (20–30 % of the data) takes 1–2 minutes. |
| Deletion delay | 1 day or more | Above the longest restore; late predecessor marks can never become due. |
| `ProgressTimeouts` | activation 15 min (seconds on an upgraded build, see below), publication 2 min, `RestartDelay` 90 s | A cold 100 GiB restore takes 4.4–9 minutes on E8 VMs; restart replication work that stops moving. |

A lease-dependent setting the lease cannot support (a margin consuming the whole lease, a `RequestTimeout` or
`ConfirmationDuration` of half the usable lease or more) stops lease maintenance with `InvalidOperationException` when
the first lease is acquired, instead of silently never holding authority or never issuing grants.

## Upgrades, schema detachment and leak events

Set `PreparedUpgrade` only after the host has validated compatibility, bounded lag with retained replay input, and
every added/removed database requirement. Use a fresh transfer UUID per prepared target and keep it across retries.
Only an offer of a higher generation than the leader's starts a handoff; among equal offers the first observed wins.
The leader drains grants and transfers through `IReplicationLeaderStorage.TransferAsync` (Azure native lease Change); the
target proves ownership by renewal and then activates normally.

An upgraded build whose relations change the persisted schema (for example a new secondary index) must not execute
events before its schema is published, neither as a follower nor while activating: its first writer would persist the
upgrade locally, and the node would diverge and restart (published history stays intact). Start its event loop only
after `PrepareSchemaAsync` ran on it as leader, or when its restored history already contains its schema.

Rolling schema upgrade under continuous input:

1. Start the upgraded build with an activation progress deadline (`ReplicationProgressTimeouts.Activation`, a few
   seconds) and its event loop stopped; it restores and follows without executing.
2. Report `PreparedUpgrade`. The old leader drains, stops publishing and transfers the lease; the upgraded node selects
   its generation, which permanently keeps older builds from leading.
3. It usually lags the now frozen published history and cannot catch up by executing, so its activation deadline
   fences it and requests a fatal restart. Restarted on the same directory, it restores exactly the published history,
   acquires the lease after expiry, publishes its schema in `PrepareSchemaAsync` and starts its event loop from the
   retained input after the published `CommitUlong`.
4. Old nodes detach on the schema commit and keep serving locally; replace them with upgraded nodes, which restore the
   schema commit and follow.

Leadership pauses for about one lease duration plus the restore; input received meanwhile is replayed from retention.

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
the returned maintenance job. Jobs of different databases run concurrently, so one long or failing checkpoint never
delays the others; a shared `publishLeakEvent` callback may therefore be invoked concurrently for different databases.
Local compaction has separate lifetime/cancellation from remote publication.

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

Still pending: qualification with a real application (ObjectDB state after rollback inside virtual batches, input
replay and the schema lifecycle) and release packaging. Public accessibility does not mark replication
production-ready.

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
(`ReplicationNodeRole` numeric value), and `btdb.replication.status.age` (seconds since the last sample), plus the
recovery counters that `ReplicationStatus` also exposes: `btdb.replication.restore.attempts`,
`btdb.replication.restore.failures`, `btdb.replication.restore.duration` (seconds of the completed restore),
`btdb.replication.leader.sessions`, `btdb.replication.leader.sessions.ended` and `btdb.replication.step.failures`.
Useful alerts: not ready for longer than the restore budget, growing restore or step failures (an unreachable leader
or storage), leader sessions ending while the process runs, a status age of more than a few seconds, and publication
lag reported by the application.
Instruments have no database/node/endpoint labels or credential values. Configure collection/export through the host's
normal .NET metrics pipeline. No diagnostic HTTP endpoint is automatically exposed.

## Pending-work deadlines and fatal recovery

Set `ReplicationNodeOptions.ProgressTimeouts` to a `ReplicationProgressTimeouts` with deployment-qualified activation
and publication no-progress budgets plus a fixed `RestartDelay`. All three values must be positive. The default is disabled; replication does not
infer an application handler timeout. When enabled, expiry calls `IReplicationNodeHost.RequestFatalRestart`. For example,
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

## Event log

`BTDB.Replication.EventLog` is an optional, Kafka-like log of opaque records in named topics. It can supply the
application's ordered input and its serializer metadata. Database replication does not depend on it. Each topic has
one owner node, fenced by the ETag of the topic's writable tail blob. Other nodes forward publications to the owner
and follow live records and heartbeats from it; a topic whose owner stays unreachable for `OwnerTimeout` is taken over.
The design and its limits are in
[EventLogImplementationPlan.md](../BTDB.Replication/EventLogImplementationPlan.md).

```csharp
var blobOptions = new BlobClientOptions();
blobOptions.Retry.MaxRetries = 0; // the log resolves ambiguous conditional writes itself
var container = new BlobServiceClient(accountUri, credential, blobOptions).GetBlobContainerClient("eventlog");
builder.Services.AddSingleton<IEventLogStorage>(new AzureEventLogStorage(container, "cluster-1"));
builder.Services.AddBTDBEventLog("https://node-1.internal:8443", apiKey, new EventLogOptions());
var app = builder.Build();
app.MapBTDBEventLog();

var events = app.Services.GetRequiredService<IEventLog>().GetTopic("events");
var offset = await events.PublishAsync(payload);                       // durable, exactly once per call
var end = (await events.GetBoundsAsync()).Next;                        // covers every completed receipt
await foreach (var record in events.ReadAsync(lastApplied + 1, end))   // replay to a captured end
    Apply(record.Offset, record.Payload);
await foreach (var record in events.ReadAsync(end, null, stopping))    // then follow live records
    Apply(record.Offset, record.Payload);
```

- **Endpoint and authentication.** The endpoint is this node's HTTPS origin as peers reach it (HTTP only on
  loopback). Peers call `POST /_btdb/eventlog/v1/{submit,bounds,subscribe}` with the cluster's bearer key.
  Subscriptions are long streamed responses, so disable response buffering and idle timeouts in front of that path.
- **Order and duplicates.** Publications that one process starts on a topic, each after the previous call returned,
  commit in call order. A failure or cancellation after dispatch means an unknown outcome, but a committed record
  exists once.
- **Offsets and replay.** Offsets start at 0 and are contiguous per topic; the application maps them to its event
  IDs. The application persists its applied offset with its own transaction, as with Kafka.
- **Storage.** Splits are at most `SplitCap` (256 KiB) and are sealed once less than `SealFreeSpace` remains. A record
  larger than a split gets its own sealed split, up to `MaxRecordSize`.
- **Merging and cleanup.** The owner merges sealed splits every `MergeInterval` into level-1 and level-2 objects
  (fan-out 16) and deletes covered objects after `DeletionDelay` (default one day). Nothing is deleted before the
  record history is covered, and there is no retention in v1.
- **Benchmarks.** `DBBenchmark eventlog-e2e` measures publish-to-all-nodes latency, and `DBBenchmark eventlog-storage`
  measures the storage primitives on Azure.
