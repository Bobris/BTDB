using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions.Testing;

/// <summary>
/// An <see cref="IActionContext"/> for unit tests of <c>ExecuteAsync</c>, without a cluster. It records the events
/// submitted as partial results, resource adjustments and the requested timeout; <see cref="Cancel"/> cancels with a
/// chosen reason. Test <c>OnFinished</c> and the partial results' handlers through an EventPublisher, like any consume.
/// </summary>
public sealed class TestActionContext(QualifiedActionId id = default, ulong attempt = 1, EventPublisher? publisher = null)
    : IActionContext
{
    readonly CancellationTokenSource _cancel = new();
    readonly List<PureEvent> _partials = new();
    ActionCancellationReason _reason;

    public QualifiedActionId Id => id;
    public ulong Attempt => attempt;
    public CancellationToken Cancellation => _cancel.Token;
    public ActionCancellationReason CancellationReason => _reason;
    public IReadOnlyList<PureEvent> Partials => _partials;
    public int CpuDelta { get; private set; }
    public TimeSpan? Timeout { get; private set; }

    /// <summary>Needs the publisher passed to the constructor.</summary>
    public Ambient.Scope StartQuery() => (publisher ?? throw new InvalidOperationException(
        "Pass a publisher to the TestActionContext to open queries.")).StartQuery();

    public ValueTask SubmitPartialAsync(PureEvent ev)
    {
        ArgumentNullException.ThrowIfNull(ev);
        _cancel.Token.ThrowIfCancellationRequested();
        _partials.Add(ev);
        return ValueTask.CompletedTask;
    }

    public void AdjustResources(int cpuDelta) => CpuDelta += cpuDelta;

    /// <summary>Records the delay; call <see cref="Cancel"/> with Timeout to simulate it elapsing.</summary>
    public void CancelAfter(TimeSpan delay) => Timeout = delay;

    public void Cancel(ActionCancellationReason reason = ActionCancellationReason.Cancelled)
    {
        if (_reason == ActionCancellationReason.None) _reason = reason;
        _cancel.Cancel();
    }
}
