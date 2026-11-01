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
    public async Task<ActionResult<ReportResult>> ExecuteAsync(GenerateReport input, IActionContext context)
    {
        using (context.StartQuery()) { /* read-only snapshot that contains the event that started this attempt */ }
        if (context.Cancellation.IsCancellationRequested) // or let an awaited call throw OperationCanceledException
            return context.CancellationReason == ActionCancellationReason.Interrupted
                ? ActionResult.Interrupted : ActionResult.Cancelled;
        await Task.Delay(100, context.Cancellation);
        return new ReportResult { BlobName = "report-" + input.ReportId }; // converts to ActionResult<ReportResult>
    }

    public void OnFinished(GenerateReport input, ActionOutcome<ReportResult> outcome)
    {
        // Inside the consume of the finishing event, on every replica, once per outcome. Deterministic, like any
        // consume. outcome.Kind is Completed, Failed, Cancelled or Interrupted.
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
* `IActionInputUnificator.GetUniqueKey()` returns a `byte[]?` key; null means the input never merges, and an empty
  key merges with other empty keys. An input with the key of a queued action that has not started merges into it.
  Compose a key of several values by writing them into one array, for example with `BinaryPrimitives`. `IActionInputAggregator.TryAggregate(other)` merges the new input's
  content; when it returns false, the queued action stops aggregating and the new input starts another action. Once
  an action starts, a new input with its key creates a new action, so a request never receives a result computed
  before it was made.
* `IActionWithBeforeStart<TInput>.OnBeforeStart(input)` runs in the consume that records a start. An exception
  finishes the attempt as `Failed` with code `BeforeStartFailed`; writes made before it stay, so validate first.
* Failures: throw `BackendActionException(code, message, details)`; any other exception finishes the attempt as
  `Failed` with code `Unexpected`. An outcome larger than `EventPublisherOptions.MaximumEventSize` finishes it as
  `Failed` with code `OutcomeTooLarge`, without its result.
* An exception from `OnFinished` does not undo the outcome: the action still finishes, the handler's writes before it
  stay, and `BackendActions.OnFinishedFailed(id, kind, exception)`, when set, reports it inside the consume. The
  finishing action's unification key is already free there, so an input `OnFinished` enqueues starts a new action.

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

// Elsewhere, for example in an API endpoint: inside a query scope, publishes an event with the scope's publisher
// and waits for it to be applied; later reads of the scope see it.
using (publisher.StartQuery())
{
    var other = await actions.EnqueueAsync(new GenerateReport { ReportId = 8 });
    var found = await actions.CancelAsync(other);
}
```

The async methods (`EnqueueAsync`, `CancelAsync`, `SuspendAsync`, `SuspendTenantsAsync`, `ImportAsync`) take no
publisher: inside a query scope they publish with the scope's publisher, inside a consume they apply in its
transaction like their synchronous counterparts, and with no ambient scope they throw.

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
of a superseded attempt are ignored. A worker that cannot publish an attempt's outcome reports the attempt as lost
and never runs it again, so the coordinator records a new attempt at once. After `MaxAttempts` (5) lost attempts the
coordinator finishes the action as `Interrupted` with code `AttemptsExhausted`. A stopping worker interrupts its
running attempts and publishes the results of those that still return one. For the others it publishes
`ActionInterrupted`, which queues the action again at once; an interruption is not the action's fault, so it does not
count toward `MaxAttempts` (`QueuedAction.InterruptedAttempts` counts them). A crash, a lost worker or an expired lease
still counts.

`IActionContext` offers:

* `Cancellation` and `CancellationReason`: `Cancelled`, `Timeout` (after `CancelAfter(delay)`) or `Interrupted`
  (superseded or the worker stops). `ExecuteAsync` returns an `ActionResult<TResult>`: a result, which converts
  implicitly, `ActionResult.Cancelled` or `ActionResult.Interrupted`. The handler acknowledges a cancellation by
  returning one of the latter, without an exception, or by throwing `OperationCanceledException`. The attempt then
  finishes as `Cancelled`, with code `Timeout` for a timeout, or, when interrupted, publishes nothing and runs again
  without using an attempt. A handler may also return `Cancelled` on its own, which finishes the attempt as
  `Cancelled`, or `Interrupted`, which runs it again as a lost attempt that counts toward `MaxAttempts`. A result
  completes the attempt whatever the reason; a failure while interrupted publishes nothing, since the interruption may have caused
  it. A synchronous handler returns `Task.FromResult<ActionResult<TResult>>(result)`.
* `SubmitPartialAsync(ev)`: publishes an event of the application and returns after this node consumed it, so parts
  keep their order and a slow consumer slows the action down. It does nothing else: the application's own event
  handler consumes the part. It is often used to commit partial progress: each consumed part is durable on every
  replica, so a long action can record the work it has done, and a later attempt can resume from the last committed
  part instead of starting over. It also returns results larger than one event
  (`EventPublisherOptions.MaximumEventSize`, 50 MiB by default): publish the data in parts and return a small final
  result. Put `context.Id` and `context.Attempt` in the event; a new attempt starts from its first part unless it
  resumes from the committed progress, and a superseded attempt's parts can still arrive, so the consume can skip a
  part unless `FindAction(id)` is `Started` with `RunAttempts` equal to its attempt.
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
using (publisher.StartQuery())
{
    await actions.SuspendAsync([name]);            // start no new attempts of these types
    await actions.SuspendTenantsAsync([tenantId]); // nor of these tenants' actions
}
```

Suspension stops new attempts only: running attempts go on, and inputs keep queueing, merging and becoming due.
Resume with `suspended: false`. Inside a consume, for example of an application event that deactivates a tenant,
`actions.SuspendTenants([tenantId])` or `actions.Suspend([name])` suspends in that event's transaction. Tenant 0 holds the actions without a
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
   * The old tables are elsewhere: an export tool calls `actions.ImportAsync(rows, lastRuns, suspended,
     suspendedTenants)` in a query scope, in batches below the publisher's `MaximumEventSize`, in ascending old
     order.
4. Verify queued and delayed counts per action name and tenant and the scheduled last runs (`ListActions`), then start
   the cluster, so the coordinator loads the imported rows. Keep the old tables until the validation window ends.

An `ImportedAction` keeps its `TenantId` and `ActionId` and carries the input (a registered type), priority, old
order, queue time, run attempts, `StartAt` for a delayed action, `IsCancelled` and `IsAggregable`. The import queues
the actions in their old relative order after any already queued, keeps delayed ones delayed, finishes cancelled ones
at once as `Cancelled` through their handler, points each unification key at its latest aggregable action, and raises
the ID allocator above every imported ID. An ID that already exists fails the whole event. An imported action's next
attempt is its run attempts plus one.

## Testing handlers

`BTDB.BackendActions.Testing.TestActionContext` runs `ExecuteAsync` without a cluster. It records the events submitted
as partial results, resource adjustments and the requested timeout, and `Cancel(reason)` cancels it. Test `OnFinished` through an
`EventPublisher`, like any other consume.
