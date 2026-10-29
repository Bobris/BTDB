# BTDB.Replication.EventSourcing

A .NET 10 preview package that connects typed application events to BTDB EventLog and an already restored replicated
ObjectDB. Its API follows the SkillShare shape: synchronous `Validate`, ordered `Consume`, `PureEvent`,
`EventWithResult<TResult>`, `PublishAsync`, `PublishAndWaitAsync`, and an ambient query/transaction context.

Prepared for the next release. After publication:

```sh
dotnet add package BTDB.Replication.EventSourcing --prerelease
dotnet add package BTDB.SourceGenerator
```

Use the same preview version as `BTDB.Replication.Http` and `BTDB.Replication.Azure` when using those adapters.
Use SourceGenerator at the matching base version and enable unsafe blocks in the application project. It generates
metadata for event subclasses and ObjectDB relations used by BTDB's default implementation.

## Compose

```csharp
using BTDB.Replication.EventSourcing;

// database has already finished canonical restore and this generation's relation initialization.
var dispatcher = new EventDispatcher().Register<Increment, Incremented>(new IncrementHandler());
await using var publisher = new EventPublisher(database,
    eventLog.GetTopic("application-events"),
    eventLog.GetTopic("application-metadata"), dispatcher,
    new EventPublisherOptions { MaximumBatchSize = 32 }, transactionLogCapture);
await publisher.StartAsync(stopping);
var result = await publisher.PublishAndWaitAsync(new Increment { Amount = 5 }, stopping);

using (publisher.StartQuery())
{
    var rows = Ambient.Relation<IEntries>();
}
```

`Validate` runs on the caller's stack against a local read-only snapshot before publishing to either topic.
`Consume` runs on every instance in data-topic order. `StartAsync` catches up from the ObjectDB commit cursor, then
follows live events. `Ambient.IsReplay` is true for history captured at startup and false for later live events on
every instance. Each event has its own transaction inside a bounded native BTDB virtual batch; local publication
results complete after the batch becomes visible to queries.

In a handler, `Ambient.RegisterPostCommit(Action)` schedules synchronous work after the entire batch becomes visible,
with a fresh query scope. Replay ignores it automatically and rollback discards it. Actions run locally on all live
instances and are not persisted or retried.

Set `EventPublisherOptions.Cluster` to an `EventCluster(nodeId, replicationPresence)` to read `Ambient.Cluster` or
`publisher.Cluster`: `SelfNodeId`, `LeaderNodeId`, `LiveNodeIds` and `Settling`. Consumers announce themselves after
replay to the selected replication leader through the node's `ReplicationPresence`, withdraw on shutdown and expire
after a crash. Membership adds no shared-storage requests and no event topic; use the host's node ID.

Layer2 uses two separate topics: metadata is applied in topic order to both serializer
and deserializer before dependent data is written or consumed. Metadata is retained from offset zero; data replay
starts at the database cursor. The publisher owns its workers, but the application owns the database, schemas, input
retention, replication host and external effects. Observe `Completion` and stop the publisher before disposing its
database or EventLog service.

See the [integration guide](https://github.com/Bobris/BTDB/blob/master/Doc/ReplicationEventSourcing.md) for handlers,
cursor semantics, replication progress, restart and failure behavior. Production application qualification remains open.
