using System.Text.Json;
using BTDB.ODBLayer;
using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

public sealed class BackendActionsOptions
{
    /// <summary>This node's stable identity across restarts, such as its replication node ID; a new worker session
    /// from the same node ends the previous process's attempts at once.</summary>
    public required string NodeId { get; set; }
    /// <summary>CPU units this node's worker offers; 100 is one core.</summary>
    public int WorkerCpu { get; set; } = Environment.ProcessorCount * 100;
    /// <summary>False runs no worker on this node; it still coordinates while it leads.</summary>
    public bool RunWorker { get; set; } = true;
    public ActionPlannerOptions Planner { get; set; } = new();
    /// <summary>How long the coordinator holds a worker's exchange without news; well below the worker timeout.</summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(10);
    /// <summary>How long a worker waits for the leader beyond the poll timeout, and between failed exchanges.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    /// <summary>How often the coordinator queues delayed actions that became due.</summary>
    public TimeSpan DelayedInterval { get; set; } = TimeSpan.FromSeconds(10);
    /// <summary>The longest the coordinator waits before rechecking leases without any change.</summary>
    public TimeSpan PlanInterval { get; set; } = TimeSpan.FromSeconds(1);
    /// <summary>Wall clock for due delayed actions, the same as the publisher's, and monotonic time for leases.</summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
}

/// <summary>
/// One leader session's scheduler. It publishes <see cref="CoordinatorActivated"/> and loads the actions only after that
/// barrier is applied, then keeps its planner in sync with committed rows through <see cref="ActionChanges"/>, records
/// the starts it plans in one <see cref="ActionsStarted"/> per round and answers worker exchanges with the commands the
/// planner derives from their reports.
/// </summary>
internal sealed class ActionCoordinator(BackendActions actions, EventPublisher publisher, BackendActionsOptions options)
{
    readonly object _lock = new();
    readonly ActionPlanner _planner = new(options.Planner);
    readonly TimeProvider _time = options.TimeProvider;
    readonly long _started = options.TimeProvider.GetTimestamp();
    TaskCompletionSource _workersWake = Signal();
    TaskCompletionSource _loopWake = Signal();
    bool _ready, _closed;

    internal Guid Session { get; } = Guid.NewGuid();

    static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    TimeSpan Now => _time.GetElapsedTime(_started);

    internal async Task RunAsync(CancellationToken cancellation)
    {
        using var changes = actions.Changes.Subscribe();
        try
        {
            await publisher.PublishAsync(new CoordinatorActivated { Session = Session }, cancellation).ConfigureAwait(false);
            actions.Changes.Drain(); // the full load below covers every earlier change
            Load();
            lock (_lock) _ready = true;
            var nextDelayed = TimeSpan.Zero;
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();
                Task wake;
                lock (_lock) wake = _loopWake.Task;
                var changed = actions.Changes.Next;
                Apply(actions.Changes.Drain());
                List<Abandonment> abandoned;
                List<Assignment> assignments;
                lock (_lock)
                {
                    if (_loopWake.Task.IsCompleted) _loopWake = Signal();
                    abandoned = _planner.Expire(Now);
                    assignments = _planner.Plan(Now);
                }
                foreach (var abandonment in abandoned)
                    await publisher.PublishAsync(new ActionFinished
                    {
                        TenantId = abandonment.Id.TenantId, ActionId = abandonment.Id.ActionId,
                        Attempt = abandonment.Attempt, Kind = abandonment.Kind,
                        Error = new() { Code = abandonment.Reason, Message = "The coordinator finished the action." }
                    }, cancellation).ConfigureAwait(false);
                if (assignments.Count != 0)
                    await publisher.PublishAsync(new ActionsStarted
                    {
                        Session = Session,
                        Actions = assignments.Select(a => new StartedAction
                        {
                            TenantId = a.Id.TenantId, ActionId = a.Id.ActionId, Attempt = a.Attempt, WorkerSession = a.Worker
                        }).ToList()
                    }, cancellation).ConfigureAwait(false);
                if (Now >= nextDelayed)
                {
                    nextDelayed = Now + options.DelayedInterval;
                    await QueueDueAsync(cancellation).ConfigureAwait(false);
                }
                if (abandoned.Count != 0 || assignments.Count != 0) continue; // their changes arrive promptly
                await Task.WhenAny(changed, wake, Task.Delay(options.PlanInterval, _time, cancellation)).ConfigureAwait(false);
            }
        }
        finally
        {
            lock (_lock)
            {
                _closed = true;
                _workersWake.TrySetResult();
            }
        }
    }

    void Load()
    {
        using var query = publisher.StartQuery();
        var now = Now;
        lock (_lock)
            foreach (var row in Ambient.Relation<IQueuedActions>())
                _planner.Update(new(row.TenantId, row.ActionId), row, actions.TypeOf(row.Name), now);
    }

    void Apply(HashSet<QualifiedActionId> changed)
    {
        if (changed.Count == 0) return;
        using (publisher.StartQuery())
        {
            var queued = Ambient.Relation<IQueuedActions>();
            var now = Now;
            lock (_lock)
                foreach (var id in changed)
                {
                    var row = queued.FindByIdOrDefault(id.TenantId, id.ActionId);
                    _planner.Update(id, row, row == null ? null : actions.TypeOf(row.Name), now);
                }
        }
        WakeWorkers();
    }

    async Task QueueDueAsync(CancellationToken cancellation)
    {
        List<ActionKey> due;
        using (publisher.StartQuery())
            due = Ambient.Relation<IDelayedActions>().ListByStartAt(new(EnumerationOrder.Ascending, DateTime.MinValue,
                    KeyProposition.Included, _time.GetUtcNow().UtcDateTime, KeyProposition.Included))
                .Select(d => new ActionKey { TenantId = d.TenantId, ActionId = d.ActionId }).ToList();
        if (due.Count != 0)
            await publisher.PublishAsync(new DelayedActionsDue { Actions = due }, cancellation).ConfigureAwait(false);
    }

    void WakeWorkers()
    {
        TaskCompletionSource wake;
        lock (_lock)
        {
            wake = _workersWake;
            _workersWake = Signal();
        }
        wake.TrySetResult();
    }

    /// <summary>A worker's long poll: answers at once with commands or acknowledgements, otherwise when the planner
    /// has news for it or after the poll timeout.</summary>
    internal async ValueTask<ReadOnlyMemory<byte>> ExchangeAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellation)
    {
        ExchangeRequest request;
        try { request = ExchangeJson.DecodeRequest(bytes); }
        catch (JsonException error) { throw new IOException("Invalid backend action exchange.", error); }
        var running = request.Running.Select(a => a.ToAttempt()).ToList();
        var finished = request.Finished.Select(a => a.ToAttempt()).ToList();
        var deadline = Now + options.PollTimeout;
        var response = new ExchangeResponse { CoordinatorSession = Session, Acknowledged = request.Finished };
        while (true)
        {
            Task wake;
            lock (_lock)
            {
                if (_closed || !_ready) throw new IOException("The backend action coordinator is not running.");
                var commands = _planner.Exchange(request.WorkerSession, request.NodeId, request.Cpu, request.Names,
                    !request.Draining, running, finished, Now);
                finished = [];
                response.Commands = commands.Select(c => new CommandMessage
                {
                    Kind = c.Kind, TenantId = c.Id.TenantId, ActionId = c.Id.ActionId, Attempt = c.Attempt,
                    StartEventId = c.StartEventId
                }).ToList();
                wake = _workersWake.Task;
                _loopWake.TrySetResult(); // a new or freed worker may take actions
            }
            var remaining = deadline - Now;
            if (response.Commands.Count != 0 || response.Acknowledged.Count != 0 || remaining <= TimeSpan.Zero)
                return ExchangeJson.Encode(response);
            await Task.WhenAny(wake, Task.Delay(remaining, _time, cancellation)).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
        }
    }
}
