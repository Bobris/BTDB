# Typed event publishing over replication and EventLog

`BTDB.Replication.EventSourcing` supplies the application execution layer above the provider-neutral replication and
EventLog contracts. It consumes one data topic into one ObjectDB and shares a separate Layer2 metadata topic with
all publishers and consumers of that data topic. HTTP and Azure are supplied by the existing adapters; this package
does not depend on either.

## Handler API

```csharp
using BTDB.ODBLayer;
using BTDB.Replication.EventSourcing;

public class Increment : EventWithResult<Incremented>
{
    public int Amount { get; set; }
}

public class Incremented
{
    public bool Accepted { get; set; }
    public ulong EventId { get; set; }
}

public class Entry
{
    [PrimaryKey] public ulong Id { get; set; }
    public int Amount { get; set; }
}

public interface IEntries : IRelation<Entry>;

public class IncrementHandler : IEventHandler<Increment, Incremented>
{
    public Incremented? Validate(Increment ev) => ev.Amount <= 0 ? new() { Accepted = false } : null;

    public Incremented Consume(Increment ev)
    {
        Ambient.Relation<IEntries>().Upsert(new() { Id = ev.EventId, Amount = ev.Amount });
        return new() { Accepted = true, EventId = ev.EventId };
    }
}
```

For events without a result derive from `PureEvent`, implement `IEventHandler<TEvent>` with synchronous `void Validate`
and `void Consume`, and register with `EventDispatcher.Register<TEvent>`. For results register with
`Register<TEvent, TResult>`. Registration is per dispatcher and frozen when a publisher is constructed, so isolated
nodes in the same process do not share a global handler table. Applications with their own generated dispatchers can
implement `IEventDispatcher` instead.

Reference `BTDB.SourceGenerator` at the matching base version and set `AllowUnsafeBlocks` in the application project
for the default BTDB metadata path. The generated `Event` base makes its subclasses eligible for metadata generation;
the generator also supplies relation metadata. Annotate additional nested event payload types with `BTDB.Generate`
when they are not otherwise discovered by the generator.

`PublishAsync(PureEvent)` and `PublishAndWaitAsync(EventWithResult<TResult>)` are deliberately non-async entry points:
validation and its exceptions occur synchronously before they return. Returning a non-null result from validation
rejects publication and immediately returns that result; throwing rejects publication with that exception. Neither
case writes data or metadata. A successful publication waits for durable data-topic acceptance and successful local
consumption; it does not await every replica or canonical database publication.

Validation sees the caller's local read-only snapshot. It is not a distributed precondition: other accepted events
can be consumed between validation and this event. Enforce ordered business invariants in `Consume` too. A handler
may return an application rejection without changing data; the event's commit cursor still advances normally.
Validation does not run again during replay. Do not mutate or reuse the event object until publication completes.

## Data and metadata topics

Use distinct topics, for example `application-events` and `application-metadata`. They must belong to the same
application history; never reuse a metadata topic from an unrelated data stream. All instances use the same type-name
mapping and compatible handlers. `EventPublisherOptions.TypeNameMapper` supports persisted type names and upgrades.

The flow follows Skymamba's streaming Layer2 serializer:

1. At startup, read the metadata topic from offset zero and apply records in order to both `EventSerializer` and
   `EventDeserializer`; continue following new metadata.
2. Serialize the validated event. If Layer2 returns new metadata, append those bytes to the metadata topic and wait
   until that actual offset has been applied locally. Then serialize again.
3. Append only the resulting Layer2 data bytes to the data topic. Serialize and dispatch local publications in one
   lane, releasing it once the data write is dispatched so independent receipts can be batched by EventLog.
4. A consumer missing a descriptor catches up to the metadata end observed after the data record before decoding
   again. An unresolved descriptor at that boundary is an invalid record, rather than an unlimited live wait.

Do not optimistically apply proposed metadata: concurrent publishers may introduce different types or propose the
same descriptors, and their committed metadata-topic order selects the shared Layer2 IDs. The concurrency and delayed
metadata tests exercise this ordering. Existing event types reuse descriptors; metadata is not repeated per event.

Keep the full metadata topic from offset zero, including across database checkpoint restore. Data may be retained
from a later offset only when every supported canonical restore and replay cursor can still reach it. Startup fails
if metadata was pruned or the database cursor is outside retained data bounds. Topics are supplied through
`IEventTopic`; the existing EventLog service owns storage durability, transport and ambiguous-write reconciliation.

The serialized data envelope contains the event and a publication correlation GUID. Consumption can finish before
the data append's receipt reaches the publisher; correlation binds that committed result to the original local caller
in either order. It is not a business deduplication key or a replacement for the ordered data offset. Event IDs are
assigned on consumption from that offset, not by a local counter. `NewPublicationId` and the publication `TimeProvider`
are injectable; the publisher serializes `When` once so every consumer sees the same time.

## ObjectDB, batching and restart

Supply an ObjectDB after canonical restore and relation/schema initialization. Its cursor must follow this mapping:

| Value | Meaning |
| --- | --- |
| Data offset `n` | Event ID `n + 1`. |
| ObjectDB `CommitUlong = 0` | No application event consumed. |
| ObjectDB `CommitUlong = k` | Resume reading data at offset `k`. |

This keeps zero available for genesis and avoids committing the first event with an unchanged cursor. Data and metadata
offsets are independent; metadata never changes the database's application cursor.

Every event uses `StartWritingTransaction(eventId, inBatch: true)`, and commits its state and cursor atomically.
`MaximumBatchSize` bounds each group of buffered events; `FinishTransactionBatchAfterCurrentTransaction` then makes
the native virtual batch visible. Each event retains its own native transaction and rollback in TRL, so nodes can
use different batch sizes without reframing history. Pending result tasks complete after this visibility boundary.
`BufferedEvents` bounds the data-reader queue, and record payloads are copied before advancing EventLog's enumerator.

`StartAsync` captures the data topic's current durable end, catches up from the restored ObjectDB cursor and returns
only after that history is visible. During catch-up `Ambient.IsReplay` is true. Records at or beyond the captured end
are live and have `IsReplay` false on every instance. Cancellation of the `StartAsync` wait does not dispose the
publisher; dispose it to stop its workers.

After restart, create a new publisher over the restored ObjectDB and the same topics. It rebuilds Layer2 metadata
from the complete metadata topic and consumes only the missing data tail. Canonical checkpoint/TRL may lag recent
local commits: replay regenerates that tail from the durable EventLog. Never take the cursor from an unverified local
cache or a process-local counter.

## Ambient and replication integration

`publisher.StartQuery()` or `Ambient.StartQuery(publisher)` creates an async-flow query scope. `Ambient.Relation<T>`
lazily opens a read-only snapshot; nested query scopes for the same publisher reuse it. `TemporaryCloseTransaction`
releases that snapshot so a later query in the same scope sees newly visible commits. `Ambient.PublishEventAsync`
and `PublishAndWaitEventAsync` forward to that scope's publisher. Different publishers and their databases remain
isolated even in one process.

During consumption the wrapper installs the event's writer, `CurrentEvent`, `EventId`, `IsQuery = false` and
`IsReplay`. `Ambient.Transaction` is borrowed: never commit or dispose it. `AllocateId` increments ObjectDB user Ulong
slot 2 inside the transaction; reserve that slot for this allocator. The context closes at the end of the handler.
Publishing from a consume context is rejected because every replica would otherwise emit the same follow-up event.

Use the serialized event's fields and deterministic database state to calculate writes. `IsReplay` can inform
diagnostics or application-owned external-effect policy, but must not change replicated database writes. Live
consumption occurs on all replicas; false does not identify a unique side-effect owner. Application external effects
still need the application's own ownership/idempotency policy.

## Actions after a batch

During `Consume`, register work with `Ambient.RegisterPostCommit(Action)`, following the Skymamba context API:

```csharp
Ambient.Relation<IEntries>().Upsert(new() { Id = ev.EventId, Amount = ev.Amount });
var eventId = ev.EventId;
Ambient.RegisterPostCommit(() =>
{
    // A fresh read-only query scope sees all successful events in this batch.
    var currentRows = Ambient.Relation<IEntries>().Count();
    notifications.Send(eventId, currentRows);
});
```

Actions run synchronously on the consumption worker, in event and registration order, after the complete native
virtual batch is visible and replication progress is updated. They receive a fresh ambient query scope, so no
transaction or snapshot from the handler is reused. Capture event fields needed by the action; `CurrentEvent` is
null in its query scope. The publisher completes local result waits after processing the actions.

Replay registrations are ignored automatically. A rolled-back event discards its registrations; if a later event
in the same batch fails, the successfully committed prefix becomes visible and its actions still run. Registration
from validation or a query is rejected. Action errors do not roll back committed database state: remaining actions
are attempted, local consumption results remain successful, and `Completion` faults to let the host handle recovery.

Actions are process-local and neither persisted nor retried. A crash between commit and execution can lose an action,
and replay intentionally does not recreate it. Live events run actions on every consuming instance; use them for
node-local work or apply the application's external-effect ownership and idempotency policy. They must not block
waiting for another event to be consumed by the same worker. An `Action` is synchronous: do not pass an `async void`
lambda; explicitly schedule and observe asynchronous work in the application when needed.

## Node discovery

Configure an `EventCluster` for each publisher to discover consumers that finished their startup replay:

```csharp
// All instances share this storage prefix and cluster ID. Each running instance has its own node ID.
// Azure example: membershipStorage = new AzureEventLogStorage(container, "application-membership").
var cluster = new EventCluster(nodeId, clusterId, membershipStorage, replicationLeaderStorage);
await using var publisher = new EventPublisher(database, dataTopic, metadataTopic, dispatcher,
    new EventPublisherOptions { Cluster = cluster }, transactionLogCapture);
await publisher.StartAsync(stopping);

var snapshot = publisher.Cluster; // No I/O; periodically refreshed by heartbeats.
var self = snapshot.SelfNodeId;
var leader = snapshot.LeaderNodeId;
var live = snapshot.LiveNodeIds;
using (publisher.StartQuery())
{
    var sameNode = Ambient.Cluster.SelfNodeId;
}
var refreshed = await cluster.RefreshAsync(stopping); // Explicit fresh shared-storage read.
```

Supply the same node ID used in the host's `LeaderCandidate.NodeId`. `LeaderNodeId` comes from the existing
replication leader document, never from data/metadata topic ownership. It is null before a leader is selected and
can name a leader that is still activating or no longer reachable. The snapshot exposes no lease or peer credentials.
Use replication's own authority APIs for operations requiring leadership; membership is discovery, not fencing.

Each instance registers only after its captured startup data end is visible, before `StartAsync` returns. Restoring
or replaying instances remain absent from `LiveNodeIds`, while `SelfNodeId` and the observed leader are available
in replay handlers. A heartbeat keeps the instance registered even while the live topic is idle. Disposal and
consumption failure unregister it; a crash or unsuccessful unregister expires by `NodeTimeout`. Cached snapshots
filter expired entries without requiring another refresh. Heartbeats also refresh the observed leader and members.

`EventClusterOptions` injects time and session-ID creation, with default heartbeat interval 5 seconds, node timeout
30 seconds and per-operation request timeout 10 seconds. Membership uses UTC expiration; synchronize node clocks
and select a timeout that covers clock skew and expected storage delays. Views are sampled, not atomic cluster
censuses or guarantees that another node is currently responsive. A blocked synchronous handler may still have a
heartbeat; discovery identifies its live-consumption mode rather than measuring per-event responsiveness.

Membership uses ordinary conditional objects through the provider-neutral `IEventLogStorage`, in an isolated shared
prefix. It adds no event topic or replicated database writes: the Layer2 data and metadata topics stay separate and
unchanged. Stable node keys are reused after expiration; expired records are filtered, not garbage-collected. A
session ID and ETag protect against a delayed old shutdown removing a replacement session, tested with two consumers
reusing one node ID. A concurrent live duplicate fails startup. Use a separate `EventCluster` per publisher session,
and a separate membership storage prefix for each application history. Membership I/O failures fault the publisher's
`Completion` and stop the worker; hosts retain responsibility for restart policy.

For a replicated DB, pass its `TransactionLogCapture` into the publisher. `Progress` snapshots the completed event ID
and TRL position after each visible batch and can be returned by `IReplicationNodeHost.GetProgress` once the wrapper
is running. Before that, the host reports its verified restored cut. The replication host still owns restore, authority,
genesis, relation initialization, checkpoint maintenance and process restart, as described in
[ReplicationHosting.md](ReplicationHosting.md). Start the input loop after its schemas are ready and keep consuming
while a lagging leader is activating. Do not wait for leader publication before consuming an event.

## Scheduling on the leader

Use the replication host's injected `ReplicationLeadership` service for long-running application scheduling:

```csharp
await leadership.RunWhileLeaderAsync(async leaderCancellation =>
{
    await scheduler.RunAsync(publisher, leaderCancellation);
}, stoppingToken);
```

This is driven by the activated local leader session, independently of `Ambient.Cluster.LeaderNodeId`. It runs once
per session and cancels on fencing, conservative lease expiry, handoff drain or shutdown. The callback executes on
its own worker without inheriting an ambient transaction; open `publisher.StartQuery()` for each query snapshot.
It may await normal event publication/consumption without blocking the publisher's consumption worker. Join the
subscription before disposing the publisher or database. See
[hosting](ReplicationHosting.md#application-work-during-leadership) for cleanup, failure and cancellation semantics.

## Failure and lifetime

A handler failure disposes its writer without committing, preserving earlier successful events in the batch. The
wrapper flushes that successful prefix, faults `Completion`, fails pending local waits and stops its input workers.
It does not skip the failed record or mark it consumed. Restart retries that record from the last successful cursor.
Hosts must observe `Completion` and arrange their restart/error policy; the wrapper does not choose retry, timeout or
skip policies for the application.

Cancellation after publication dispatch can mean the event was stored and may still be consumed; cancellation is
not proof of absence. A new caller retry is a new publication unless the application event contains its own stable
business deduplication key. Pure events and result events follow the same persistence path.

The publisher owns its workers and ambient scopes. It borrows its database, EventLog topics and handler dispatcher.
Dispose it before disposing ObjectDB or EventLog; disposal waits for input workers and propagates a worker fault.
`Completion` remains observable after startup. Observe publication tasks rather than discarding failures.

## Verification

`dotnet test BTDB.Replication.EventSourcing.Test/BTDB.Replication.EventSourcing.Test.csproj -c Release` exercises
synchronous validation, separate-topic descriptor reuse, concurrent publishers, isolated ambient contexts, native
batch visibility, different batch sizes, delayed metadata, consumption before a receipt, restart, cold replay,
rollback and missing retained input. Lower-level EventLog, replication, HTTP and Azure tests remain in their owning
suites. The wrapper's Azure integration test uses the real EventLog service and Azure storage adapter with an isolated
Azurite process (or the existing fixture's explicitly configured live account). These checks support application
integration; they do not close production-application qualification B3.
The suite also checks post-commit ordering and visibility, automatic replay suppression, rolled-back registrations,
action failure, delayed startup membership, heartbeat renewal and leader changes with injected timers, crash expiry,
duplicate IDs, replacement-session shutdown protection, cluster isolation and membership through Azure storage.
