using System.Diagnostics.Metrics;

namespace BTDB.BackendActions;

/// <summary>
/// Instruments of meter <c>BTDB.BackendActions</c>, one meter per host. Every node counts its worker's attempts; only
/// the node whose coordinator runs reports the queue gauges, so a sum across nodes is the cluster's. Instruments carry
/// no node or action labels.
/// </summary>
internal sealed class BackendActionsMetrics : IDisposable
{
    internal const string MeterName = "BTDB.BackendActions";
    readonly Meter _meter = new(MeterName);
    internal Meter Meter => _meter;
    readonly Counter<long> _started;
    readonly Counter<long> _finished;
    readonly Histogram<double> _startDelay;
    readonly Histogram<double> _duration;
    readonly Counter<long> _exchangeFailures;

    internal BackendActionsMetrics(Func<ActionsOverview?> overview)
    {
        _started = _meter.CreateCounter<long>("btdb.backend_actions.attempts.started", description:
            "Attempts this node's worker started.");
        _finished = _meter.CreateCounter<long>("btdb.backend_actions.attempts.finished", description:
            "Attempts this node's worker finished, by outcome kind.");
        _startDelay = _meter.CreateHistogram<double>("btdb.backend_actions.start_delay", "s",
            "From queueing to the start of the first attempt.");
        _duration = _meter.CreateHistogram<double>("btdb.backend_actions.attempt.duration", "s",
            "Execution time of finished attempts.");
        _exchangeFailures = _meter.CreateCounter<long>("btdb.backend_actions.exchange.failures", description:
            "Worker exchanges with the leader that failed.");
        _meter.CreateObservableGauge("btdb.backend_actions.waiting",
            () => Measure(overview(), o => o.Priorities.Sum(p => p.Waiting)), description: "Actions waiting to start.");
        _meter.CreateObservableGauge("btdb.backend_actions.running",
            () => Measure(overview(), o => o.Priorities.Sum(p => p.Running)), description: "Actions held by a worker.");
        _meter.CreateObservableGauge("btdb.backend_actions.workers",
            () => Measure(overview(), o => o.Workers.Count), description: "Workers known to the coordinator.");
    }

    static IEnumerable<Measurement<int>> Measure(ActionsOverview? overview, Func<ActionsOverview, int> value) =>
        overview == null ? [] : [new(value(overview))];

    internal void Started(ulong attempt, TimeSpan sinceQueued)
    {
        _started.Add(1);
        if (attempt == 1) _startDelay.Record(Math.Max(0, sinceQueued.TotalSeconds));
    }

    internal void Finished(ActionOutcomeKind kind, TimeSpan duration)
    {
        _finished.Add(1, new KeyValuePair<string, object?>("kind", kind.ToString()));
        _duration.Record(duration.TotalSeconds);
    }

    internal void ExchangeFailed() => _exchangeFailures.Add(1);

    public void Dispose() => _meter.Dispose();
}
