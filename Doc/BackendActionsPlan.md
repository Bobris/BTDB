# BTDB.BackendActions — implementation plan

Status: milestones A0–A8 are implemented (revised 2026-11-01). Still open: qualification on live Azure, process runs
with partitions, restarts on the same directory and upgrades (A6), the import rehearsal (A7), and the cluster tests
of section 9 for a lost exchange response, a deposed leader's late `ActionsStarted` and a fault-injecting transport,
and the import test of section 11 that restarts every node.

Decided in review: a `ulong TenantId` scope, a single package, a recorded start for every action with no opt-out
(section 5), partial results as the way to return results larger than one event, and migration by draining running
actions and importing the rest with their IDs (section 11). No open questions remain.

`BTDB.BackendActions` runs long, asynchronous, resource-limited work ("backend actions") on the nodes of a BTDB
replication cluster. Application events enqueue and cancel actions. The replication leader schedules them, any live
node executes them, and an ordered result event hands each outcome back to the application. The design follows an
existing in-house BackendActions implementation that has run in production for years. It keeps that implementation's
developer API and planning policy, and changes how coordination is carried.

## 1. Goal and the main design change

The reference implementation needs three channels:

1. An event-sourced store for durable action state (queued, delayed and current actions).
2. A separate broadcast log, the coordination stream, for heartbeats, assignments, cancellations and timeouts. Every
   node consumes it and keeps a full replica of the coordinator.
3. A pluggable leader election that picks the node issuing commands.

The coordination stream causes most of the complexity. It is not ordered against the event stream: a cancellation can
overtake an assignment, and an assignment can reach a worker before the input row it refers to. Workers mitigate this
with waits and timeouts of up to 60 s and 30 min, repeated cancellations, and self-published removal events. Every new
coordination message has to be checked against these races by hand.

This library has no coordination log:

* **Durable state and every decision that must survive failover** goes through the application's existing ordered
  event stream (`BTDB.Replication.EventSourcing`) into the replicated ObjectDB, in the same transactions as business
  data.
* **Ephemeral coordination** (heartbeats, assignment delivery, cancellation delivery, resource usage) travels over
  HTTP between workers and the current leader. It is never persisted. A new leader rebuilds it from the database and
  from the workers' next exchanges.
* **Leader election** is `ReplicationLeadership.RunWhileLeaderAsync`: the coordinator runs exactly while this node is
  the activated replication leader and is cancelled on fencing, lease expiry, handoff or shutdown.
* **Node liveness** comes from worker exchanges, with `EventCluster` / `ReplicationPresence` as an extra signal.

Each HTTP message names the event IDs it depends on. A receiver acts only once its local database has consumed them,
so the ordering races of the old design cannot occur. Section 5 explains why this also fences a deposed leader.

Non-goals for v1:

* Cross-cluster or cross-database actions.
* Sandboxing: actions run in-process, like in the reference implementation.
* Durable history of finished actions. Finished rows are deleted, and history lives in the event stream, logs and
  metrics.
* A generic workflow engine with steps, compensation or DAGs.

## 2. What the reference implementation does (summary)

Only the parts that shape this plan.

| Area | Reference behavior | Kept / changed |
| --- | --- | --- |
| Handler | `ExecuteAsync(input, ct)` off-transaction; `Handle/Fail/Interrupt/CancelConsume` inside the result event's transaction; optional before-start consume. | Kept, with one outcome callback (section 3). |
| Execution attribute | CPU cost, maximum concurrent count per cluster, named concurrency group, at most one running per tenant, awaited partial/final results. | Kept. |
| Input capabilities | Unification by unique key (also into a running action), aggregation into a queued input, forced start, before-execute initializers. | Kept, except that unification stops once the action starts (section 4). |
| Submission | From a consume (nested, transactional) or a query (event); priority RealTime/Normal/Low/Idle; delayed start date; cancel by ID. | Kept. A consume writes rows directly instead of publishing a nested event. |
| Scheduling | Daily, monthly and repeated attributes evaluated every 5 s on the leader; optimistic `LastRun` check in a validator. | Kept. |
| Action IDs | Pre-allocated 100k ranges in a DB singleton, refilled by events, for determinism in consume. | Replaced by `Ambient.AllocateId()`, which is already deterministic in consume. |
| Resources | One CPU dimension; overrides through `IOverrideActionResources`; runtime adjustment through heartbeats; 10 CPU floor. | Kept. The worker sends its own capacity, which the reference does not (it assumes identical nodes). |
| Planning | Tenant fairness by decayed consumption (×0.95 per second); order (Priority, Order, ID); group limits; capacity share per priority (0.6/0.8/1.0); round-robin workers; 2 min lease; 5 attempts, then cancel. | Ported. The admission check also counts the action's own cost. |
| Failover | Every node mirrors the coordinator from the coordination stream. A new leader publishes "leader elected", waits, reloads rows and protects running actions for one lease. | No mirrors. The new leader reloads from the DB after a barrier event and learns running attempts from worker exchanges. |
| Worker | Waits for input events, closes aggregation with an awaited event, publishes run-attempt and before-start events, executes, and publishes the finished event with throttling. | Collapsed into one coordinator-published `ActionsStarted` event plus one worker-published `ActionFinished` event. |
| Duplicates | At-least-once execution. Duplicate finished events are only logged as "no matching action". | Execution stays at-least-once. Result handling becomes exactly-once per attempt (section 5). |
| Results | Partial results published as events, optionally awaited; one final result. | Kept, and made the documented way to return results larger than one event (section 3). |
| Monitoring | In-memory coordinator state: overview, queued/running/delayed lists, per-tenant running queries; metrics on the leader. | Mostly DB queries, which work on any node, plus a leader overview over HTTP. |
| Tests | In-memory stream, fault injection, manipulable timer, deterministic strategies; approval tests of event, DB, input and API surfaces. | Kept, including the approval tests. |

## 3. Developer API (draft)

```csharp
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
    // Off-transaction, on the assigned worker. Honors context.Cancellation.
    public async Task<ReportResult> ExecuteAsync(GenerateReport input, IActionContext context)
    {
        using (context.StartQuery()) { /* local read-only snapshot, causally after the start */ }
        await context.SubmitPartialAsync(new ReportProgress { ReportId = input.ReportId }); // an application event
        return new ReportResult { BlobName = "report-" + input.ReportId };
    }

    // Inside the ActionFinished event's consume transaction, on every replica. Deterministic, like any Consume.
    public void OnFinished(GenerateReport input, ActionOutcome<ReportResult> outcome)
    {
        // outcome.Kind: Completed | Failed | Cancelled | Interrupted; outcome.Result / outcome.Error
    }
}
```

* `ActionInput<TResult>` derives from `IActionInput`. It carries `ActionId`, an optional `ulong TenantId` and an
  optional per-input override of the concurrency group. `TenantId` replaces the reference "company" one to one: it
  scopes fairness, `SingleRunningPerTenant` and the per-tenant queries. 0 means no tenant, and an application without
  tenants never sets it.
* An action is identified by `QualifiedActionId(TenantId, ActionId)`, as in the reference. `Enqueue` returns it and
  `Cancel` takes it. New IDs come from `Ambient.AllocateId()` and are unique on their own; imported IDs (section 11)
  are unique only together with their tenant, so lookups always use both.
* The capability interfaces keep their names: `IActionInputUnificator.GetUniqueKey()`,
  `IActionInputAggregator.TryAggregate(other)`, and the optional
  `IActionWithBeforeStart.OnBeforeStart(input)`, which runs in the consume of `ActionsStarted`. A before-start
  exception finishes the attempt as `Failed`.
* The action name is `[BackendAction(Name = ...)]`, by default the input type's full name. Rows and unification keys
  store it, so set it before renaming an input type. It is used for limits, schedules and metrics.
* Errors are an `ActionError` with Code, Message and Details. `BackendActionException(code, message, details)` sets
  them, and any other exception becomes `Code = "Unexpected"`. There is no ambient error object.
* Cancellation: `context.Cancellation` is cancelled for every `context.CancellationReason`: `Cancelled` (the action
  was cancelled), `Timeout` (`context.CancelAfter(delay)` elapsed) and `Stopped` (the attempt was superseded or the
  worker stops). An `OperationCanceledException` thrown after it finishes the attempt as `Cancelled`, with the error
  code `Timeout` for a timeout, not as `Failed`; a stopped attempt publishes nothing. The first reason wins. There is
  no default timeout, as in the reference.
* Resources: `context.AdjustResources(cpuDelta)` changes the CPU the coordinator accounts to the running attempt,
  never below 10, with the worker's next exchange; a new attempt starts from the type's cost again.
  `ActionPlannerOptions.CpuOverride(name, cost)` replaces the reference's `IOverrideActionResources`, for example
  from configuration; it is evaluated on the leader.
* Scheduling attributes on the input class: `[RepeatedlyScheduled(minutes)]` (counted from midnight UTC),
  `[DailyScheduled(minuteOfDay)]` and `[MonthlyScheduled(day, minuteOfDay)]` (the month's last day when it is
  shorter), all UTC. A handler may implement `IScheduledActionGate.CanStartScheduled(now)` to skip an occurrence. The
  coordinator checks every `ScheduleInterval` (5 s) and starts each passed occurrence once, with a new input from the
  type's parameterless constructor; the first check of a type only records its latest occurrence, so deploying a type
  never starts it at once. Missed occurrences are not caught up: one start covers all of them. The reference's
  `IActionInputForceable` is dropped: starting a scheduled type by hand is an ordinary `EnqueueAsync`.

Submission:

```csharp
// actions is the application's BackendActions instance (no process-global state).
// Inside any event handler's Consume: transactional with the business change, deterministic ID, no extra event.
var id = actions.Enqueue(new GenerateReport { ReportId = 7 },
    new ActionEnqueueOptions { Priority = ActionPriority.Low, StartAt = ev.When.AddHours(1) });
actions.Cancel(id);

// From a query scope, for example in an API endpoint or scheduler: publishes ActionEnqueued / ActionCancelRequested
// with the scope's publisher.
using (publisher.StartQuery())
{
    var id2 = await actions.EnqueueAsync(new GenerateReport { ReportId = 8 }, cancellation: cancellation);
}
```

`EnqueueAsync` and `CancelAsync` use `Ambient.PublishAndWaitEventAsync`, so the returned ID is the one the consume
assigned. Called inside a consume, they apply in its transaction instead.
An input that unifies or aggregates returns the ID of the action it merged into.

### Partial results, progress and large results

`SubmitPartialAsync` is often used to commit partial progress: a long action publishes what it has done so far, each
part is durable once consumed, and a later attempt resumes from the last committed part instead of starting over. The
same mechanism returns results larger than one event.

A result travels in an `ActionFinished` event and is therefore limited by `EventPublisherOptions.MaximumEventSize`
(50 MiB by default). There is no blob-reference mechanism in v1. A handler with more data publishes it in parts, as
events of the application:

```csharp
public async Task<ExportResult> ExecuteAsync(ExportRows input, IActionContext context)
{
    foreach (var chunk in ReadInChunks(input, maximumBytes: 4 * 1024 * 1024))
        await context.SubmitPartialAsync(new ExportRowsPart
        {
            TenantId = context.Id.TenantId, ActionId = context.Id.ActionId, Attempt = context.Attempt, Rows = chunk
        });
    return new ExportResult { Completed = true }; // small final result
}

// The application's IEventHandler<ExportRowsPart>.Consume appends ev.Rows to its relation keyed by the action.
public void OnFinished(ExportRows input, ActionOutcome<ExportResult> outcome)
{
    // Completed: mark the export done. Failed/Cancelled/Interrupted: delete the appended rows or keep them.
}
```

* `SubmitPartialAsync(ev)` is a thin wrapper of `EventPublisher.PublishAsync` with the attempt's cancellation: it
  returns after the local consume, so a slow consumer applies back-pressure and parts keep their order. BackendActions
  neither inspects nor consumes the event.
* A part larger than `MaximumEventSize` fails the call with `EventTooLargeException` before anything is written;
  the handler can split it further or let the attempt fail.
* The application's handler consumes each part in its own consume transaction. The application owns the accumulated
  state and decides what a later failure or cancellation means for it.
* A new attempt starts from the beginning, after the coordinator restarts the action, or resumes from what the
  application already stored. A superseded attempt's parts may still arrive after the new attempt started, so the
  consume skips a part unless `FindAction(id)` is `Started` with `RunAttempts` equal to the part's attempt.

Hosting:

```csharp
var actions = new BackendActions().RegisterGenerated(type => container.Resolve(type)); // or Register(handler)
actions.RegisterTypes(objectDb);                        // input types stored polymorphically in action rows
await objectDb.InitializeRelations([.. applicationRelations, .. BackendActions.RelationTypes]);
actions.RegisterEvents(dispatcher);                     // the library's own event handlers, before the publisher
builder.Services.AddBTDBReplication(...);
// after the publisher started; join RunAsync before disposing the publisher
var host = new BackendActionsHost(actions, publisher, app.Services.GetRequiredService<ReplicationLeadership>(),
    app.Services.GetRequiredService<ReplicationLeaderChannel>(), new BackendActionsOptions { NodeId = nodeId });
var running = host.RunAsync(stopping);
```

The library registers its own events (section 4) with the application's `EventDispatcher`, so they share the data
topic, the ObjectDB and its batching. Relations are added to the application's `InitializeRelations` list.

### 3.1 Source-generated registration (A8, done)

Hand registration reflects at startup: `Register` reads `[BackendAction]` and the schedule attributes, checks for a
parameterless constructor and creates scheduled inputs with `Activator.CreateInstance`. `BTDB.SourceGenerator` now
finds every concrete, non-generic public or internal class implementing `IActionHandler<TInput, TResult>` (a class
whose concrete base already implements the same interface is left to that base) and emits a module initializer per
handler. It adds one `GeneratedAction<TInput, TResult>` per handled input to `GeneratedActions`, with the input's
name, CPU, limits, group and schedule (the attribute rebuilt from its arguments), a typed `new TInput()` for scheduled
starts and a typed `new THandler()` when the handler has an accessible parameterless constructor.
`actions.RegisterGenerated(handlerFactory, include)` registers them all; handlers come from the factory (for example
the IOC container) or their parameterless constructor. `Register(handler)` stays for tests and special cases and uses
the generated values when they exist. The generator pipeline is separate from the main one and emits a file only per
handler, so it stays incremental. Tests: `BackendActionsGeneratorTests` (Verify snapshots) and
`ActionGenerationTest`; the process qualification nodes register through it.

The package is marked AOT-compatible; the analyzer's only finding was the reflection fallback of `Register`, whose
input type parameter is now annotated to keep the parameterless constructor. `AotActionsSmoke/` is the NativeAOT
smoke app: it runs the host on an in-process single-node cluster (the HTTP replication stack is not part of it) and
covers import, generated and hand registration, enqueue, merging, partial results, failure, cancellation and the
overview. `run.sh` fails on trim or AOT warnings in BackendActions and EventSourcing; CI runs it as
`aot-actions-smoke`. It found four BTDB gaps, all fixed: trimming removed the properties of `[Generate]` types in
library assemblies (`ClassMetadata.Type` and relation registration are now annotated, so trimming keeps them), the
inline-object creator emitted IL, the event serializer created enum and struct array types at runtime, and the
poison-event journal used reflection-based JSON. A second pass added a payload of every field shape, relations with lazy collections,
singletons and upgrades to the smoke. It found `RawData.MethodTable` decoding CoreCLR's type flags under NativeAOT and
`Singleton`/`New` emitting their field initializer; both are fixed.

## 4. Persistent state and events

The application's ObjectDB holds these relations. Only consumes write them.

| Relation | Key | Fields |
| --- | --- | --- |
| `IQueuedActions` | PK (TenantId, ActionId); `Order` from the relation's `AllocateId` counter | Name, Priority, Order, Input, Flags (Aggregable, Cancelled, Started in one byte), RunAttempts, WorkerSession, StartEventId, LatestInputEventId, QueuedAt |
| `IDelayedActions` | PK (TenantId, ActionId); SK StartAt | Name, Priority, Input, QueuedAt |
| `IActionInputIndexes` | PK (TenantId, Name, UniqueKey) | ActionId of the queued action that has not started |
| `IScheduledActionRuns` | PK Name | LastRun: the latest occurrence started, skipped or recorded |
| `IBackendActionsStates`, one row with Id 0 | PK Id | CoordinatorSession, suspended action names, suspended tenants (few, so a list in the row) |

Whether an input unifies or aggregates comes from its type, so rows store no flags for it. Secondary keys for
monitoring are added with monitoring (A5) when its queries need them.

Events, all ordinary `BTDB.Replication.EventSourcing` events:

| Event | Published by | Consume |
| --- | --- | --- |
| `ActionEnqueued{Input, Options}` | `EnqueueAsync` | Same code path as `BackendActions.Enqueue` inside a consume. |
| `ActionCancelRequested{ActionId}` | `CancelAsync` | Delayed: delete. Not started: call `OnFinished(Cancelled)` and delete in this transaction (the input is at hand, so no deferred removal is needed). Started: set `IsCancelled`, and the coordinator finishes it. |
| `CoordinatorActivated{Session}` | New leader's coordinator | Store `CoordinatorSession`. This is the barrier and fencing epoch (section 5). |
| `ActionsStarted{Session, [ActionId, Attempt, WorkerSession]}` | Coordinator, one event per planning round | Reject the whole event if `Session` is not current. Per action: skip it if it is gone, cancelled, or `Attempt` is not `RunAttempts + 1`; otherwise set `IsStarted`, `RunAttempts = Attempt`, close aggregation, remove the index entry and run `OnBeforeStart`. A higher attempt supersedes a running one. |
| `ActionFinished{ActionId, Attempt, Outcome, Result/Error}` | Worker, or the coordinator for abandoned attempts | Ignore it unless the action is started and `RunAttempts == Attempt`. Call `OnFinished` and delete the row. |
| `DelayedActionsDue{[ActionId]}` | Coordinator, about every 10 s | Move due rows to `IQueuedActions`, or unify them into existing rows. |
| `ScheduledActionStarted{Name, ExpectedLastRun, NewLastRun, Start}` | Coordinator scheduler | Only if `LastRun == ExpectedLastRun` (an optimistic check in consume, not in a validator): set `LastRun = NewLastRun` and, when `Start`, enqueue a new input. |
| `BackendActionsSettingsChanged{...}` | Admin API | Update the singleton. |

Aggregation, unification and running actions: the reference closes aggregation with an awaited event published by
the worker, but keeps unifying new requests into an action that is already running. Here the `ActionsStarted` consume
removes the action's index entry in the same transaction that records the attempt. A later input with the same key
therefore creates a new queued action, which later requests unify into. A request never receives a result computed
before it was made. This is a deliberate change for unificators: a request that arrives while its twin runs now
causes one more run.

Every node's consume registers `Ambient.RegisterPostCommit` notifications ("action queued", "action cancelled",
"action finished"). Only the leader's coordinator subscribes to them; on other nodes they cost nothing. Replay does not
run post-commit actions, which is fine because the coordinator reloads from the database after activation.

## 5. Coordination without a coordination log

### Coordinator lifetime

```
RunWhileLeaderAsync(leaderCancellation =>
  1. subscribe to the post-commit notifications (before loading, the reference's invariant)
  2. session = new Guid; await PublishAndWaitAsync(CoordinatorActivated(session))
     -> the local DB now contains every event published before the barrier
  3. load IQueuedActions / IDelayedActions in one StartQuery() snapshot
  4. rows with Started != null are "possibly running": protect them for one lease (default 2 min) or until their
     worker reports
  5. serve worker exchanges, plan, run the delayed and recurring schedulers, until leaderCancellation)
```

`CoordinatorActivated` is the reference's "leader elected" event used as a fencing epoch. A deposed leader may still
publish `ActionsStarted` before it notices its cancellation. Its old session no longer matches the stored one, so the
consume rejects the event on every replica in the same way. The replication lease already guarantees at most one
active coordinator; this check also covers effects already in flight.

### Attempts and exactly-once handling

An attempt is the action's start count: `ActionsStarted` names `RunAttempts + 1`, its consume records it, and
`ActionFinished` must name the recorded value. The count is durable, so a superseded attempt stays rejected across
coordinator sessions and the attempt limit survives failover. The coordinator assigns an action only after the
`ActionsStarted` event that records the attempt has been consumed locally. The assignment it delivers to the worker
carries that event ID. The worker waits until its local `LastAppliedEventId` reaches it, which takes milliseconds
because workers are live consumers, and then reads the input from its own snapshot. Several guarantees follow:

* The worker always sees the input and every business change committed before the start. No 30 min wait is needed.
* `ActionFinished` from a stale attempt (lag, partition, a reassigned lease) is ignored by the consume. `OnFinished`
  runs exactly once per final outcome, which closes the reference's duplicate-handling gap.
* Execution stays at-least-once. `ExecuteAsync` may run twice for one action, and external effects need the
  application's idempotency, as documented for `RunWhileLeaderAsync`.

### Why every start is recorded

An earlier revision let an action type skip `ActionsStarted` and start without an event round trip. It was dropped
from v1 after a survey of the reference implementation's 451 action handlers:

* The reference already publishes one event per started action (the run-attempt counter), plus an awaited event when
  it closes aggregation or runs a before-start hook. One batched `ActionsStarted` per planning round costs less.
* The saving is one event commit and apply, about 10–30 ms (Standard_GZRS commit p50 8.7 ms, p99 19 ms, EventLog plan
  section 16). The reference's own start-delay alerts begin at 5 s for real-time priority.
* 43% of the handlers use partial results, aggregation or before-start, which need a recorded start anyway. Most
  real-time actions send messages, where a duplicate is worse than a delay.
* Without it, attempt counts would live only in the leader's memory. An action that crashes the leader's process
  would reset its count on every failover and retry forever. A new leader would also have to wait for workers to
  report before starting any such action, which costs more than the steady-state saving. And a request unified into
  a running action would get a result computed before the request was made.

If A2 measurements show a workload where start latency matters, the opt-out can come back as an additive option.

### HTTP exchange (pull model)

Workers initiate every request; the leader never calls workers. This mirrors replication's follower poll. Workers need
no authenticated inbound endpoint, and the leader needs no worker addresses.

The exchange is a `ReplicationLeaderChannel` request on the channel `btdb-backend-actions`, a long poll held for up
to about 10 s, or until there is a command for this worker.

* **Request** (JSON): `WorkerSession` (fresh per process), `NodeId`, CPU capacity, application generation,
  Draining, the attempts it runs with their CPU adjustments and whether it already ends them (`Cancelling`), and the
  attempts whose `ActionFinished` it published and applied locally, until acknowledged.
* **Response:** `CoordinatorSession`, the acknowledged finished attempts, and commands: `Start{ActionId, Attempt,
  StartEventId}`, `Cancel{Attempt}` and `Forget{Attempt}` (a stale attempt: stop it and drop its result).

Commands carry no sequence numbers. The coordinator derives them from each report: `Start` for an attempt assigned
to the worker that it does not report, `Cancel` for a reported attempt of a cancelled action that the worker does not
already end, `Forget` for a reported
attempt that is superseded or unknown. A lost response therefore costs one round trip, and a worker ignores a `Start`
for an attempt it already runs or finished. A finished report stops the coordinator from starting the attempt again
until its `ActionFinished` is applied on the leader; if that never happens within a lease, the action runs again. A
worker whose outcome publication fails reports the attempt in `Lost` until acknowledged and ignores its `Start`; the
coordinator frees the worker, so the next plan records a new attempt, or the action finishes as `Interrupted` after
its last one. An outcome over `MaximumEventSize` is published instead as `Failed` with code `OutcomeTooLarge`. The
coordinator answers at once when it has commands or acknowledgements, and otherwise holds the request until news for
this worker or the poll timeout (default 10 s). A worker reports a finished attempt at once by cancelling its current
long poll.

The exchange itself is the heartbeat. A worker without an exchange for the worker timeout (default 30 s) is expired,
and its attempts are lost at once. A new `WorkerSession` from the same `NodeId` (a process restart) ends the attempts
of the old session without waiting for their leases. `ActionsStarted` records the worker's node in the row, so this
holds also for attempts a previous coordinator started. The failover test of a restarted node shows the difference:
without the node, the new coordinator waits a whole lease (2 min by default) before starting the attempt again.
Expiring a worker whose node left a non-settling `EventCluster.LiveNodeIds` is not implemented: the worker timeout
already bounds the wait, and no test showed a need for a second signal.

A stopping worker cancels its attempts and publishes no outcome for them, so they run again elsewhere, as after a
crash. Its last exchange, with `Draining` and no running attempts, removes the worker at once, so the reruns need not
wait for the worker timeout; when that exchange fails, the timeout still ends them. An `Interrupted` outcome comes
only from the coordinator, for exhausted attempts.

Lease expiry (no report of a running attempt) makes the action runnable again. The next `ActionsStarted` supersedes
the old attempt. After `MaxAttempts` (default 5), the coordinator publishes `ActionFinished(Interrupted, "attempts
exhausted")` for the current attempt. Cancellation of an action with a dead worker is finished the same way, as
`Cancelled`.

The leader is also a worker: the channel calls the leader's own handler directly, with the same messages.

### Failover walk-through

1. The leader dies or hands off. The old coordinator is cancelled; workers keep executing.
2. Workers' exchanges fail and they re-resolve the leader. Finished results are published to the event stream, which
   needs no coordinator, so work completed during the gap is not lost.
3. The new leader activates, publishes `CoordinatorActivated` and reloads. Rows that are started but unreported are
   protected for one lease.
4. Workers reconnect and report their running attempts. The coordinator adopts those, rebuilds resource usage, and
   reassigns the rest after protection ends.

The same path covers a graceful handoff during a rolling upgrade. Each worker reports its application generation
(`BackendActionsOptions.ApplicationGeneration`, the node's `ReplicationNodeOptions.ApplicationGeneration`), and the
coordinator starts nothing new on a worker older than its own generation. Attempts an older worker already runs
finish normally, and after the handoff to an upgraded leader new actions run only on upgraded nodes. Workers list no
action types: every node registers every type anyway, because `OnFinished` runs on every replica.

## 6. Prerequisites in existing packages

* **R1, an application channel to the leader.** Done as `ReplicationLeaderChannel` in `BTDB.Replication`, carried
  by `BTDB.Replication.Http`; see [hosting](ReplicationHosting.md#requests-to-the-leader). Every node registers named
  handlers. They run only during the active leader session that runs `RunWhileLeaderAsync` callbacks, with a token
  cancelled when it ends. Followers send over their existing authenticated peer session, so no new credential, route
  or leader discovery exists, and the leader calls its own handler directly. Presence could not carry this: it is one
  polled value of at most 4 KiB per node, with no request and response.
* **R2, presence sharing (optional).** `ReplicationPresence` holds one value per node, and `EventCluster` claims it.
  BackendActions only reads `EventCluster.Current` and needs no slot. If a later feature needs its own announcement,
  extend presence to named slots then, not now.
* **R3, a publisher hook for library handlers.** No change needed: `EventDispatcher.Register` is public, so the
  library registers its handlers on the application's dispatcher before the publisher is constructed, and the
  publisher's `TypeNameMapper` maps the library's event and input types like the application's.

## 7. Packages and code layout

One package, `BTDB.BackendActions`, depends only on `BTDB.Replication.EventSourcing`. It contains contracts,
attributes, relations, events and consumes, the coordinator and planner, the worker and executor, the scheduler,
monitoring queries and metrics. It needs no ASP.NET or HTTP code: the coordinator and worker exchange messages only
through `ReplicationLeaderChannel`, which `AddBTDBReplication` registers and the replication peer endpoint carries.
The application constructs `BackendActionsHost` from that channel and `ReplicationLeadership`, so there are no DI
extensions either. It is versioned and released with the rest of the family (Releaser project list).

Test projects: `BTDB.BackendActions.Test` (unit, planner, and multi-node failover over an in-process fake of the
leader sessions and channel) and `BTDB.BackendActions.Process.Test` (A6: subprocess cluster with real replication and
HTTP: process kill, failover, handoff).

## 8. Monitoring and operations

Implemented in A5; usage is in [BackendActions.md](BackendActions.md#monitoring).

* **Any node, from the local DB:** `ListActions(tenantId?)` (delayed, queued and started), `FindAction`,
  `HasStartedActions(tenantId)`, `FindByUniqueKey`, `SuspendedActions`, `SuspendedTenants` and
  `IsTenantSuspended`, in the caller's query or consume scope.
  They need no leader and no secondary keys: tenant lists use the primary key prefix, and callers page with LINQ.
* **Leader overview:** `BackendActionsHost.GetOverviewAsync` asks the active coordinator over the channel
  `btdb-backend-actions-overview`: workers with session, node, generation, capacity, CPU in use, readiness, time since
  their exchange and attempts; per priority the waiting, running and protected actions; the suspended types and
  tenants.
* **Metrics** (meter `BTDB.BackendActions`, one per host, no node or action labels): attempts started and finished
  (by `kind`), start delay and attempt duration, failed exchanges; the gauges waiting, running and workers come only
  from the node whose coordinator runs.
* **Suspension:** in a query scope, `SuspendAsync(names, suspended)` and `SuspendTenantsAsync(tenantIds, suspended)`
  publish `ActionsSuspended`; inside a consume they, `Suspend` and `SuspendTenants` apply in its transaction. Suspended types and the actions of
  suspended tenants start no new attempts; running ones go on and inputs keep queueing. The planner skips a suspended
  tenant as a whole in its fairness loop.
* **Dropped:** capacity shares as an admin event (they stay leader configuration in `ActionPlannerOptions`, like the
  CPU override) and runtime tracing (the library has no logging dependency; the overview and metrics cover it).

## 9. Testing strategy

* **Planner unit tests:** port the reference coordinator test scenarios (priority, fairness, groups, single running
  per tenant, resources and adjustments, leases, attempt exhaustion) to a pure `ActionPlanner` with a fake
  `TimeProvider` and a deterministic worker strategy.
* **Consume tests:** enqueue, unify, aggregate, delayed, cancel in each state, stale attempt or session rejection,
  before-start failure; partial results' ordering through the host. Verify that replay and different batch sizes give identical TRL bytes.
* **In-process cluster:** the existing replication and EventSourcing test infrastructure with 1–3 nodes and an
  in-memory EventLog. Cover leader failover mid-run, worker kill, a lost exchange response, a deposed leader's late
  `ActionsStarted`, handoff, and an upgrade where the new action type is unsupported by old workers. Add a fault
  injecting transport.
* **Process tests:** kill -9 the worker or the leader under load. Assert that every action finishes exactly once in
  `OnFinished`, and that no action is lost (the reference's failover verification scenario).
* **Approval tests:** public API, event list, relation schemas and the exchange JSON, as Verify baselines in
  `BTDB.BackendActions.Test/ApprovalTest.*.verified.txt`. Done.
* **Application test helpers:** `Testing.TestActionContext` runs `ExecuteAsync` without a cluster and records parts,
  adjustments and timeouts. `OnFinished` is a consume and is tested through an `EventPublisher`, so no handler base
  class is needed.

## 10. Milestones

| ID | Content | Done when |
| --- | --- | --- |
| A0 | R1 in the existing packages, with tests and docs; R3 needed no change. Done. | Followers and the leader reach the active leader's handler, and a held request ends with its session. |
| A1 | Contracts, relations, events and consumes; enqueue, unify, aggregate, delayed, cancel; `TenantId`. Done. | Consume tests pass, including determinism under replay. |
| A2 | `ActionPlanner` port, coordinator lifetime, channel messages, worker and executor on one node. Done. | A single-node app runs actions end to end; the planner suite passes. |
| A3 | Multi-node failover, attempt fencing, settling, rolling upgrades (by application generation since A4). Done; the exchange needs no HTTP code of its own. | Failover tests pass in process. |
| A4 | Recurring scheduling, partial results with back-pressure and the large-result guidance, resource adjustment, before-start, cancellation reasons, timeouts. Done. | Feature parity with the reference API surface. |
| A5 | Monitoring, metrics, test helpers, `Doc/BackendActions.md`, changelog, release wiring. Done. | The package packs, and docs cover hosting, and the API. |
| A6 | Process qualification. Done: healthy, killed-worker and killed-leader runs in `BTDB.BackendActions.Process.Test`; it found and fixed an `OnDiskReplicationFileStorage` bug (a transaction could not read its own unflushed values). Partitions, restarts on the same directory and upgrades are not covered yet. | Kill and failover runs show exactly-once `OnFinished` and no lost actions. |
| A7 | Import API (`Import`, `ActionsImported`, scheduled runs, settings) and the cutover runbook (section 11). Done, except the rehearsal, which needs the old system's tables and its export tool, outside this repository. | Import tests pass, and a rehearsal against a copy of the old tables matches the counts. |
| A8 | Source-generated registration (section 3.1) and the NativeAOT smoke run (`AotActionsSmoke/`). Done. | Applications register every action without reflection; SourceGenerator snapshot tests and an AOT smoke run pass. |

## 11. Migration from the reference implementation

Decided: drain running actions, then import the rest with their IDs. The switch happens at the same quiesced barrier
as the application's event-store cutover ([EventLog plan, section 13](../BTDB.Replication/EventLogImplementationPlan.md#13-migrating-an-existing-kafka-deployment)).
The old coordination topic is not migrated. There is nothing in it to keep: all durable action state is in the old
action tables.

### Cutover steps

1. **Stop starting actions.** In the old system, suspend every action type so its coordinator assigns nothing new.
   Inputs keep queueing until the event-store barrier stops publishers.
2. **Drain running actions** for a bounded budget chosen per deployment, at least the longest common action. Their
   finished events land in the old history and remove their rows as usual.
3. **Stop the rest without a result.** Actions still running when the budget ends are stopped by shutting down the old
   workers. They publish nothing, so their rows stay in the old tables with their run attempts. This is the same
   situation as a crashed worker in the old system, and the action runs again after import. Handlers that already
   needed to tolerate a crash need nothing new.
4. **Quiesce publishers** and capture the barrier (EventLog plan, step 1). Export from a read-only snapshot of the old
   action tables at that barrier.
5. **Import** before the new cluster starts the coordinator (below).
6. **Verify** queued and delayed counts per action name and tenant, the scheduled last runs, and that the ID allocator
   is above every imported ID. Then switch producers and consumers together.
7. **Leave the old tables in place** for the validation window. Rollback follows the EventLog plan: a reverse copy or
   forward recovery, never simply switching back. Drop the old tables in a later release.

### Import API

Implemented in A7; the runbook is in [BackendActions.md](BackendActions.md#migrating-from-another-action-system).

`BackendActions.Import(IEnumerable<ImportedAction>)` can be called from any consume; `ImportAsync(...)` in a query
scope publishes the event. The `ActionsImported` event uses
the same code, so an application can choose either way in:

* **The old tables are in the same ObjectDB:** publish one application-defined migration event. Its consume reads the
  old relations and calls `BackendActions.Import`. There is no payload, and the import is deterministic on every
  replica.
* **The old tables are elsewhere**, for example in a separate action database: an export tool reads them at the barrier
  and publishes `ActionsImported` events batched below `MaximumEventSize`. The tool appends these events to the new
  data topic right after the imported history, before any node starts.

Either way, the import events come before the new coordinator's `CoordinatorActivated` barrier. The coordinator
therefore loads imported rows like any others, and no import gate is needed.

`ImportedAction` contains:

* `TenantId` and `ActionId`, both kept as they are.
* The input, already converted to the new input type. Usually this is the same class, mapped through
  `TypeNameMapper`.
* `Priority`, the old `Order` (only to keep the relative order), `QueuedAt` and `RunAttempts`.
* `StartAt` for delayed actions.
* `IsCancelled`.

The consume does the following:

* It assigns new `Order` values in the old relative order. Imported actions therefore queue before any new ones.
* It recomputes the unification and aggregation index from the inputs, and keeps aggregation open as in the old row.
* An imported row with `IsCancelled` was not running after the drain, so the consume finishes it at once with
  `OnFinished(Cancelled)`.
* It raises the `Ambient.AllocateId` allocator above the largest imported `ActionId` with `Ambient.ReserveIdsUpTo`,
  which never lowers it, before any row, so an ID that a cancelled row's `OnFinished` allocates never collides with a
  later imported row.

`ImportScheduledRuns(name → LastRun)` and the event's suspended names and tenants carry the remaining replicated state, so
scheduled actions neither fire twice nor skip a run. Capacity shares and per-worker capacity are leader and worker
configuration (`ActionPlannerOptions`, `BackendActionsOptions.WorkerCpu`), not replicated state, so they move with the
deployment's configuration instead of an import. `ImportedAction.IsAggregable` carries whether the old action still
accepted aggregated inputs; the index points at the latest aggregable action of each key. An existing ID fails the
whole import event.

An imported action with `RunAttempts > 0` keeps that count, so attempt exhaustion still applies. Its first new
attempt is `RunAttempts + 1`, which `ActionOutcome.Attempt` shows. A partial-result
handler that already received parts before the cutover sees a restart, exactly as after a crash in the old system.

### Tests

* Import consume determinism, order preservation, index recomputation, allocator raise, cancelled rows, and delayed
  and scheduled state. Run each with both ways in.
* A cluster test that imports, restarts all nodes and replays the import, then runs every imported action exactly
  once.
