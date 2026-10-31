using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions.Testing;

/// <summary>
/// An <see cref="IActionContext"/> for unit tests of <c>ExecuteAsync</c>, without a cluster. It records partial
/// results, resource adjustments and the requested timeout; <see cref="Cancel"/> cancels with a chosen reason. With
/// <paramref name="resultType"/> it rejects partial results of another type, as a worker does. Test <c>OnFinished</c>
/// through an EventPublisher, like any consume.
/// </summary>
public sealed class TestActionContext(QualifiedActionId id = default, ulong attempt = 1, EventPublisher? publisher = null,
    Type? resultType = null) : IActionContext
{
    readonly CancellationTokenSource _cancel = new();
    readonly List<object> _partials = new();
    ActionCancellationReason _reason;

    public QualifiedActionId Id => id;
    public ulong Attempt => attempt;
    public CancellationToken Cancellation => _cancel.Token;
    public ActionCancellationReason CancellationReason => _reason;
    public IReadOnlyList<object> Partials => _partials;
    public int CpuDelta { get; private set; }
    public TimeSpan? Timeout { get; private set; }

    /// <summary>Needs the publisher passed to the constructor.</summary>
    public Ambient.Scope StartQuery() => (publisher ?? throw new InvalidOperationException(
        "Pass a publisher to the TestActionContext to open queries.")).StartQuery();

    public Task SubmitPartialAsync(object result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (resultType != null && !resultType.IsInstanceOfType(result))
            throw new ArgumentException($"A partial result must be a {resultType.FullName}.", nameof(result));
        _cancel.Token.ThrowIfCancellationRequested();
        _partials.Add(result);
        return Task.CompletedTask;
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
