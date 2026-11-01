using System.Text.Json;
using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

/// <summary>
/// This node's executor. It long-polls the active leader's coordinator with the attempts it runs and has finished, and
/// carries out the returned commands. An attempt starts only after this node consumed the event that recorded it, so
/// the input and every change before the start are visible. Its outcome is published as <see cref="ActionFinished"/>
/// and reported as finished until the coordinator acknowledges it. Stopping the worker interrupts running attempts
/// without an outcome, so another node runs them again.
/// </summary>
internal sealed class ActionWorker(BackendActions actions, EventPublisher publisher, IBackendActionsCluster cluster,
    BackendActionsOptions options, BackendActionsMetrics? metrics = null)
{
    readonly BackendActions _actions = actions;
    readonly BackendActionsOptions _options = options;
    readonly BackendActionsMetrics? _metrics = metrics;
    readonly EventPublisher _publisher = publisher;
    readonly object _lock = new();
    readonly Dictionary<QualifiedActionId, Execution> _running = new();
    readonly Dictionary<QualifiedActionId, ulong> _finished = new();
    /// <summary>Superseded executions still ending; the worker waits for them when it stops.</summary>
    readonly HashSet<Execution> _forgotten = new();
    CancellationTokenSource? _poll;

    internal Guid Session { get; } = Guid.NewGuid();

    internal async Task RunAsync(CancellationToken stopping)
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                using var poll = CancellationTokenSource.CreateLinkedTokenSource(stopping);
                poll.CancelAfter(_options.PollTimeout + _options.RetryDelay + TimeSpan.FromSeconds(5));
                lock (_lock) _poll = poll;
                try
                {
                    var reply = await cluster.SendAsync(ExchangeJson.Channel, ExchangeJson.Encode(Request()), poll.Token)
                        .ConfigureAwait(false);
                    Apply(ExchangeJson.DecodeResponse(reply));
                }
                catch (OperationCanceledException) when (!stopping.IsCancellationRequested)
                {
                    // An attempt finished and must be reported now, or the leader did not answer in time.
                }
                catch (Exception error) when (error is IOException or JsonException)
                {
                    _metrics?.ExchangeFailed();
                    await Task.Delay(_options.RetryDelay, _options.TimeProvider, stopping).ConfigureAwait(false);
                }
                finally
                {
                    lock (_lock) _poll = null;
                }
            }
        }
        finally
        {
            Task[] executions;
            lock (_lock)
            {
                foreach (var execution in _running.Values) execution.Stop(forget: false);
                executions = _running.Values.Concat(_forgotten).Select(e => e.Task).ToArray();
            }
            await Task.WhenAll(executions).ConfigureAwait(false);
            await DrainAsync().ConfigureAwait(false);
        }
    }

    // Tells the coordinator at once that this worker's attempts end, instead of after the worker timeout, and reports
    // the results published meanwhile. Best effort: a missed drain only delays the reruns.
    async Task DrainAsync()
    {
        using var timeout = new CancellationTokenSource(_options.RetryDelay + TimeSpan.FromSeconds(5));
        try
        {
            await cluster.SendAsync(ExchangeJson.Channel, ExchangeJson.Encode(Request(draining: true)), timeout.Token)
                .ConfigureAwait(false);
        }
        catch (Exception error) when (error is IOException or OperationCanceledException) { }
    }

    ExchangeRequest Request(bool draining = false)
    {
        lock (_lock)
            return new()
            {
                WorkerSession = Session, NodeId = _options.NodeId, Cpu = _options.WorkerCpu,
                Generation = _options.ApplicationGeneration, Draining = draining,
                Running = draining ? [] : _running.Values.Select(e => AttemptMessage.From(e.Id, e.Attempt, e.CpuDelta,
                    e.CancellationReason != ActionCancellationReason.None)).ToList(),
                Finished = _finished.Select(f => AttemptMessage.From(f.Key, f.Value)).ToList()
            };
    }

    void Apply(ExchangeResponse response)
    {
        lock (_lock)
        {
            foreach (var acknowledged in response.Acknowledged)
                if (_finished.TryGetValue(new(acknowledged.TenantId, acknowledged.ActionId), out var attempt) &&
                    attempt == acknowledged.Attempt)
                    _finished.Remove(new(acknowledged.TenantId, acknowledged.ActionId));
            foreach (var command in response.Commands)
            {
                var id = new QualifiedActionId(command.TenantId, command.ActionId);
                _running.TryGetValue(id, out var execution);
                switch (command.Kind)
                {
                    case WorkerCommandKind.Start:
                        if (execution != null || _finished.TryGetValue(id, out var done) && done == command.Attempt) break;
                        execution = new(this, id, command.Attempt);
                        _running.Add(id, execution);
                        execution.Task = execution.RunAsync(command.StartEventId);
                        break;
                    case WorkerCommandKind.Cancel:
                        if (execution?.Attempt == command.Attempt) execution.Cancel();
                        break;
                    case WorkerCommandKind.Forget:
                        if (execution?.Attempt != command.Attempt) break;
                        execution.Stop(forget: true);
                        _running.Remove(id);
                        _forgotten.Add(execution);
                        break;
                }
            }
        }
    }

    void Completed(Execution execution, bool published)
    {
        lock (_lock)
        {
            if (_forgotten.Remove(execution)) return;
            if (!_running.TryGetValue(execution.Id, out var current) || current != execution) return;
            _running.Remove(execution.Id);
            if (published) _finished[execution.Id] = execution.Attempt;
            _poll?.Cancel(); // report now instead of after the long poll
        }
    }

    sealed class Execution(ActionWorker worker, QualifiedActionId id, ulong attempt) : IActionContext
    {
        readonly CancellationTokenSource _cancel = new();
        int _reason, _cpuDelta;
        volatile bool _forgotten;
        ITimer? _timeout;

        public QualifiedActionId Id => id;
        public ulong Attempt => attempt;
        public CancellationToken Cancellation => _cancel.Token;
        public ActionCancellationReason CancellationReason => (ActionCancellationReason)Volatile.Read(ref _reason);
        internal int CpuDelta => Volatile.Read(ref _cpuDelta);
        public Ambient.Scope StartQuery() => worker._publisher.StartQuery();
        internal Task Task { get; set; } = Task.CompletedTask;

        public ValueTask SubmitPartialAsync(PureEvent ev) => worker._publisher.PublishAsync(ev, _cancel.Token);

        public void AdjustResources(int cpuDelta) => Interlocked.Add(ref _cpuDelta, cpuDelta);

        public void CancelAfter(TimeSpan delay)
        {
            var timer = worker._options.TimeProvider.CreateTimer(_ => End(ActionCancellationReason.Timeout), null, delay,
                Timeout.InfiniteTimeSpan);
            Interlocked.Exchange(ref _timeout, timer)?.Dispose();
        }

        internal void Cancel() => End(ActionCancellationReason.Cancelled);

        internal void Stop(bool forget)
        {
            _forgotten |= forget;
            End(ActionCancellationReason.Stopped);
        }

        // The first reason decides the outcome.
        void End(ActionCancellationReason reason)
        {
            Interlocked.CompareExchange(ref _reason, (int)reason, 0);
            _cancel.Cancel();
        }

        internal async Task RunAsync(ulong startEventId)
        {
            await Task.Yield(); // never run the handler under the worker's lock
            var published = false;
            try
            {
                while (worker._publisher.LastAppliedEventId < startEventId)
                    await Task.Delay(1, _cancel.Token).ConfigureAwait(false);
                IActionInput input;
                DateTime queuedAt;
                ActionType? type;
                using (StartQuery())
                {
                    var row = Ambient.Relation<IQueuedActions>().FindByIdOrDefault(id.TenantId, id.ActionId);
                    if (row is not { IsStarted: true } || row.RunAttempts != attempt) return; // superseded meanwhile
                    input = (IActionInput)row.Input!;
                    queuedAt = row.QueuedAt;
                    type = worker._actions.TypeOf(row.Name);
                    if (row.IsCancelled) Cancel(); // cancelled before it ran: finish as Cancelled without the handler
                }
                if (type == null) return;
                var started = worker._options.TimeProvider.GetTimestamp();
                worker._metrics?.Started(attempt, worker._options.TimeProvider.GetUtcNow().UtcDateTime - queuedAt);
                var finished = new ActionFinished { TenantId = id.TenantId, ActionId = id.ActionId, Attempt = attempt };
                try
                {
                    _cancel.Token.ThrowIfCancellationRequested();
                    finished.Result = await type.Execute(input, this).ConfigureAwait(false);
                    finished.Kind = ActionOutcomeKind.Completed;
                }
                catch (OperationCanceledException) when (_cancel.IsCancellationRequested)
                {
                    if (CancellationReason == ActionCancellationReason.Stopped) return;
                    finished.Kind = ActionOutcomeKind.Cancelled;
                    if (CancellationReason == ActionCancellationReason.Timeout)
                        finished.Error = new() { Code = "Timeout", Message = "The action timed out." };
                }
                catch (BackendActionException error)
                {
                    finished.Kind = ActionOutcomeKind.Failed;
                    finished.Error = new() { Code = error.Code, Message = error.Message, Details = error.Details };
                }
                catch (Exception error)
                {
                    finished.Kind = ActionOutcomeKind.Failed;
                    finished.Error = new() { Code = "Unexpected", Message = error.Message, Details = error.ToString() };
                }
                if (_forgotten || CancellationReason == ActionCancellationReason.Stopped) return;
                await worker._publisher.PublishAsync(finished).ConfigureAwait(false);
                published = true;
                worker._metrics?.Finished(finished.Kind, worker._options.TimeProvider.GetElapsedTime(started));
            }
            catch (OperationCanceledException) when (_cancel.IsCancellationRequested) { }
            catch (Exception)
            {
                // Publication failed: the attempt's lease runs out and the coordinator starts it again.
            }
            finally
            {
                Interlocked.Exchange(ref _timeout, null)?.Dispose();
                worker.Completed(this, published);
            }
        }
    }
}
