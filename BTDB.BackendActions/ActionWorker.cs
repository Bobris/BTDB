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
    BackendActionsOptions options)
{
    readonly BackendActions _actions = actions;
    readonly EventPublisher _publisher = publisher;
    readonly object _lock = new();
    readonly Dictionary<QualifiedActionId, Execution> _running = new();
    readonly Dictionary<QualifiedActionId, ulong> _finished = new();
    CancellationTokenSource? _poll;

    internal Guid Session { get; } = Guid.NewGuid();

    internal async Task RunAsync(CancellationToken stopping)
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                using var poll = CancellationTokenSource.CreateLinkedTokenSource(stopping);
                poll.CancelAfter(options.PollTimeout + options.RetryDelay + TimeSpan.FromSeconds(5));
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
                    await Task.Delay(options.RetryDelay, options.TimeProvider, stopping).ConfigureAwait(false);
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
                executions = _running.Values.Select(e => e.Task).ToArray();
            }
            await Task.WhenAll(executions).ConfigureAwait(false);
        }
    }

    ExchangeRequest Request()
    {
        lock (_lock)
            return new()
            {
                WorkerSession = Session, NodeId = options.NodeId, Cpu = options.WorkerCpu,
                Names = _actions.Types.Select(t => t.Name).ToList(),
                Running = _running.Values.Select(e => AttemptMessage.From(e.Id, e.Attempt)).ToList(),
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
                        break;
                }
            }
        }
    }

    void Completed(Execution execution, bool published)
    {
        lock (_lock)
        {
            if (!_running.TryGetValue(execution.Id, out var current) || current != execution) return;
            _running.Remove(execution.Id);
            if (published) _finished[execution.Id] = execution.Attempt;
            _poll?.Cancel(); // report now instead of after the long poll
        }
    }

    sealed class Execution(ActionWorker worker, QualifiedActionId id, ulong attempt) : IActionContext
    {
        readonly CancellationTokenSource _cancel = new();
        volatile bool _cancelled, _stopped, _forgotten;

        public QualifiedActionId Id => id;
        public ulong Attempt => attempt;
        public CancellationToken Cancellation => _cancel.Token;
        public Ambient.Scope StartQuery() => worker._publisher.StartQuery();
        internal Task Task { get; set; } = Task.CompletedTask;

        internal void Cancel()
        {
            _cancelled = true;
            _cancel.Cancel();
        }

        internal void Stop(bool forget)
        {
            _stopped = true;
            _forgotten |= forget;
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
                ActionType? type;
                using (StartQuery())
                {
                    var row = Ambient.Relation<IQueuedActions>().FindByIdOrDefault(id.TenantId, id.ActionId);
                    if (row is not { IsStarted: true } || row.RunAttempts != attempt) return; // superseded meanwhile
                    input = (IActionInput)row.Input!;
                    type = worker._actions.TypeOf(row.Name);
                }
                if (type == null) return;
                var finished = new ActionFinished { TenantId = id.TenantId, ActionId = id.ActionId, Attempt = attempt };
                try
                {
                    finished.Result = await type.Execute(input, this).ConfigureAwait(false);
                    finished.Kind = ActionOutcomeKind.Completed;
                }
                catch (OperationCanceledException) when (_cancel.IsCancellationRequested)
                {
                    if (!_cancelled || _stopped) return;
                    finished.Kind = ActionOutcomeKind.Cancelled;
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
                if (_forgotten || _stopped && !_cancelled) return;
                await worker._publisher.PublishAsync(finished).ConfigureAwait(false);
                published = true;
            }
            catch (OperationCanceledException) when (_cancel.IsCancellationRequested) { }
            catch (Exception)
            {
                // Publication failed: the attempt's lease runs out and the coordinator starts it again.
            }
            finally
            {
                worker.Completed(this, published);
            }
        }
    }
}
