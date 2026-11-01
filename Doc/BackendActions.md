# Backend actions

`BTDB.BackendActions` runs long, resource-limited background work for applications built on
[`BTDB.Replication.EventSourcing`](ReplicationEventSourcing.md). Application events enqueue and cancel actions in
their own transactions. The replication leader schedules them, any node executes them, and an ordered event hands
each outcome back to the application on every replica. The design, its trade-offs and the remaining milestones are
in [the implementation plan](BackendActionsPlan.md).

The package depends only on `BTDB.Replication.EventSourcing`. It is not production-qualified: failover is tested in
process and in separate node processes over Azurite
([process qualification](../BTDB.BackendActions.Process.Test/README.md)), not yet on live Azure.

## Define an action

```csharp
using BTDB;
using BTDB.BackendActions;

[BackendAction(Cpu = 100, MaxConcurrent = 4, ConcurrencyGroup = "reports", SingleRunningPerTenant = true)]
public class GenerateReport : ActionInput<ReportResult>
{
    public ulong ReportId { get; set; }
}

[Generate] // results travel in events, so the source generator must know them
public class ReportResult
{
    public string BlobName { get; set; } = "";
}

public class GenerateReportHandler : IActionHandler<GenerateReport, ReportResult>
{
    public async Task<ReportResult> ExecuteAsync(GenerateReport input, IActionContext context)
    {
        using (context.StartQuery()) { /* read-only snapshot that contains the event that started this attempt */ }
        await Task.Delay(100, context.Cancellation);
        return new() { BlobName = "report-" + input.ReportId };
    }

    public void OnFinished(GenerateReport input, ActionOutcome<ReportResult> outcome)
    {
        // Inside the consume of the finishing event, on every replica, once per outcome. Deterministic, like any
        // consume. outcome.Kind is Completed, Partial, Failed, Cancelled or Interrupted.
    }
}
```

* Inputs and results are copied, so use plain collections (`List<T>`, `Dictionary<TKey, TValue>`, `HashSet<T>`). An
  input property typed `IDictionary`, `IOrderedDictionary` or `IOrderedSet` is stored as a lazy database collection,
  which the handler cannot read after the worker's query ends.
* `ActionInput<TResult>` carries `TenantId` (0 means none; it scopes fairness, `SingleRunningPerTenant` and the
  per-tenant queries) and `ActionId`, assigned on enqueue. Input classes are stored in ObjectDB rows and events; the
  `[Generate]` on `IActionInput` makes the source generator cover them.
* `[BackendAction]`: `Cpu` (100 is one core, at least 10 is accounted), `MaxConcurrent` across the cluster (0 is
  unlimited) for the type or its `ConcurrencyGroup`, `SingleRunningPerTenant`, and `Name`, by default the input type's
  full name. Rows store the name, so set it before renaming an input type. After a leader change, attempts that may
  still run count toward both limits until their worker reports them or their lease passes.
* `IActionInputUnificator.GetUniqueKey()` returns a `byte[]` key; empty means the input never merges. An input with
  the key of a queued action that has not started merges into it. Compose a key of several values by writing them into
  one array, for example with `BinaryPrimitives`. `IActionInputAggregator.TryAggregate(other)` merges the new input's
  content; when it returns false, the queued action stops aggregating and the new input starts another action. Once
  an action starts, a new input with its key creates a new action, so a request never receives a result computed
  before it was made.
* `IActionWithBeforeStart<TInput>.OnBeforeStart(input)` runs in the consume that records a start. An exception
  finishes the attempt as `Failed` with code `BeforeStartFailed`; writes made before it stay, so validate first.
* Failures: throw `BackendActionException(code, message, details)`; any other exception finishes the attempt as
  `Failed` with code `Unexpected`.
* An exception from `OnFinished` does not undo the outcome: the action still finishes, the handler's writes before it
  stay, and `BackendActions.OnFinishedFailed(id, kind, exception)`, when set, reports it inside the consume.

## Register and host

Every node registers every action type, because outcomes are handled on every replica. `BTDB.SourceGenerator`
registers each concrete public or internal class implementing `IActionHandler<TInput, TResult>` with its input's
`[BackendAction]` and schedule values, without reflection; `RegisterGenerated` adds them all. Handlers come from the
given factory, for example the BTDB IOC container, or from their parameterless constructor. Two handlers of one input
are an error. `Register(handler)` registers one handler by hand; it uses the generated values when they exist and
reads the attributes otherwise.

The package works under NativeAOT; `AotActionsSmoke/` publishes and runs a NativeAOT app with actions, and CI runs it.

```csharp
var actions = new BackendActions().RegisterGenerated(type => container.Resolve(type));
actions.RegisterTypes(objectDb);                     // input types for polymorphic storage
await objectDb.InitializeRelations([.. applicationRelations, .. BackendActions.RelationTypes]);
actions.RegisterEvents(dispatcher);                  // before the EventPublisher is constructed
// ... construct the publisher and await publisher.StartAsync(stopping) ...
var host = new BackendActionsHost(actions, publisher,
    app.Services.GetRequiredService<ReplicationLeadership>(),
    app.Services.GetRequiredService<ReplicationLeaderChannel>(),
    new BackendActionsOptions { NodeId = nodeId, ApplicationGeneration = applicationGeneration });
var running = host.RunAsync(stopping);               // join it before disposing the publisher
```

`ReplicationLeadership` and `ReplicationLeaderChannel` come from `AddBTDBReplication`; see
[requests to the leader](ReplicationHosting.md#requests-to-the-leader). `NodeId` must be stable across restarts of the
node, and `ApplicationGeneration` equal to the node's `ReplicationNodeOptions.ApplicationGeneration`. Other options:
`WorkerCpu` (the node's capacity, by default 100 per processor), `RunWorker`, `Planner` (lease, worker timeout,
attempt limit, capacity shares, CPU overrides), the poll, plan, delayed and schedule intervals, and `TimeProvider`,
which should be the publisher's.

## Enqueue and cancel

```csharp
// Inside any event handler's Consume: in its transaction, with a deterministic ID and no extra event.
var id = actions.Enqueue(new GenerateReport { ReportId = 7 },
    new ActionEnqueueOptions { Priority = ActionPriority.Low, StartAt = ev.When.AddHours(1) });
actions.Cancel(id);

// Elsewhere, for example in an API endpoint: publishes an event and waits for it to be applied.
var other = await actions.EnqueueAsync(publisher, new GenerateReport { ReportId = 8 });
var found = await actions.CancelAsync(publisher, other);
```

`QualifiedActionId(TenantId, ActionId)` identifies an action. An input that merges returns the ID of the action it
merged into and allocates no ID of its own. Priorities are `RealTime`, `Normal`, `Low` and `Idle`. A start time later than the enqueuing event's
time keeps the action delayed. Cancelling removes a delayed action without an outcome, finishes a queued one as
`Cancelled` at once, and marks a started one; its worker or the coordinator then finishes it as `Cancelled`.

## Execution

The coordinator runs on the replication leader. It starts actions by priority, then enqueue order within a tenant,
serving tenants with the least recent CPU use first. A worker takes an action when its cost fits the priority's share
of the worker's capacity (`RealTime` 100 %, `Normal` 80 %, `Low` and `Idle` 60 %); an idle worker takes any single
action. A worker of an older `ApplicationGeneration` than the coordinator gets no new actions.

Execution is at least once: `ExecuteAsync` can run again after a lost worker, an expired lease (2 min without a
report) or a leader change, so external effects need idempotency. `OnFinished` runs exactly once per outcome: results
of a superseded attempt are ignored. After `MaxAttempts` (5) lost attempts the coordinator finishes the action as
`Interrupted` with code `AttemptsExhausted`. A stopping worker publishes nothing for its running attempts and tells the
coordinator in a last exchange, so they run again elsewhere at once.

`IActionContext` offers:

* `Cancellation` and `CancellationReason`: `Cancelled`, `Timeout` (after `CancelAfter(delay)`, finishing as
  `Cancelled` with code `Timeout`) or `Stopped` (superseded or the worker stops; nothing is published).
* `SubmitPartialAsync(result)`: a `Partial` outcome, applied before the call returns, so parts keep their order. Use it
  for results larger than one event (`EventPublisherOptions.MaximumEventSize`, 50 MiB by default): return the data in
  parts and a small final result, and accumulate the parts in `OnFinished`. `outcome.Attempt` tells which attempt a
  part belongs to; a new attempt starts again from its first part.
* `AdjustResources(cpuDelta)`: changes the CPU the coordinator accounts to this attempt, never below 10.
* `StartQuery()`: a snapshot of the worker's database.

## Schedules

`[RepeatedlyScheduled(minutes)]`, `[DailyScheduled(minuteOfDay)]` and `[MonthlyScheduled(day, minuteOfDay)]` on an
input class with a parameterless constructor start the type once per passed occurrence, in UTC. The first check after
a type appears only records its latest occurrence, and missed occurrences are not caught up. A handler implementing
`IScheduledActionGate.CanStartScheduled(now)` can skip an occurrence. To start a scheduled type by hand, enqueue it.

## Monitoring

In any query or consume scope, on any node:

```csharp
using (publisher.StartQuery())
{
    var all = actions.ListActions();              // delayed, queued and started actions
    var tenant = actions.ListActions(tenantId);
    var one = actions.FindAction(id);
    var busy = actions.HasStartedActions(tenantId);
    var merged = actions.FindByUniqueKey(tenantId, name, key);
    var suspended = actions.SuspendedActions;
    var paused = actions.IsTenantSuspended(tenantId); // or actions.SuspendedTenants
}
var overview = await host.GetOverviewAsync(stopping); // from the leader: workers, waiting and running per priority
await actions.SuspendAsync(publisher, [name]);         // start no new attempts of these types
await actions.SuspendTenantsAsync(publisher, [tenantId]); // nor of these tenants' actions
```

Suspension stops new attempts only: running attempts go on, and inputs keep queueing, merging and becoming due.
Resume with `suspended: false`. Inside a consume, for example of an application event that deactivates a tenant,
`actions.SuspendTenants([tenantId])` suspends in that event's transaction. Tenant 0 holds the actions without a
tenant, scheduled ones included. Suspended tenants are few by design: they are stored in one row.

Meter `BTDB.BackendActions` counts each worker's started and finished attempts (`btdb.backend_actions.attempts.*`,
finished by `kind`), records the start delay and attempt duration in seconds, and counts failed exchanges. The node
whose coordinator runs also reports the gauges `btdb.backend_actions.waiting`, `running` and `workers`.

## Migrating from another action system

Import the old system's actions once, at the same quiesced barrier as the application's event-store cutover, before
any new coordinator starts:

1. In the old system, stop starting actions, let running ones drain for a bounded time, then stop the old workers.
   Actions still running are left without a result and run again after the import, as after a crash.
2. Quiesce the publishers and read the old action tables from a snapshot at the barrier.
3. Import, in one of two ways:
   * The old tables are in the application's ObjectDB: publish one application event whose consume reads them and
     calls `actions.Import(rows)`, `actions.ImportScheduledRuns(lastRuns)` and, for suspended tenants,
     `actions.SuspendTenants(ids)`. Every replica imports the same rows.
   * The old tables are elsewhere: an export tool calls `actions.ImportAsync(publisher, rows, lastRuns, suspended,
     suspendedTenants)`,
     in batches below the publisher's `MaximumEventSize`, in ascending old order.
4. Verify queued and delayed counts per action name and tenant and the scheduled last runs (`ListActions`), then start
   the cluster, so the coordinator loads the imported rows. Keep the old tables until the validation window ends.

An `ImportedAction` keeps its `TenantId` and `ActionId` and carries the input (a registered type), priority, old
order, queue time, run attempts, `StartAt` for a delayed action, `IsCancelled` and `IsAggregable`. The import queues
the actions in their old relative order after any already queued, keeps delayed ones delayed, finishes cancelled ones
at once as `Cancelled` through their handler, points each unification key at its latest aggregable action, and raises
the ID allocator above every imported ID. An ID that already exists fails the whole event. An imported action's next
attempt is its run attempts plus one.

## Testing handlers

`BTDB.BackendActions.Testing.TestActionContext` runs `ExecuteAsync` without a cluster. It records partial results,
resource adjustments and the requested timeout, and `Cancel(reason)` cancels it. Test `OnFinished` through an
`EventPublisher`, like any other consume.
