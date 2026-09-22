using System;
using System.Diagnostics.Metrics;

namespace BTDB.Replication.Http;

/// <summary>Host-scoped instruments contain no database, node, endpoint or credential labels.</summary>
internal sealed class ReplicationMetrics : IDisposable
{
    readonly Meter _meter;

    public ReplicationMetrics(IMeterFactory factory, ReplicationStatus status, IReplicationScheduler clock)
    {
        _meter = factory.Create("BTDB.Replication");
        _meter.CreateObservableGauge("btdb.replication.ready", () => status.Current.Ready ? 1 : 0,
            description: "Local replication readiness; not publication authority or Blob durability.");
        _meter.CreateObservableGauge("btdb.replication.role", () => (int)status.Current.Role,
            description: "Sampled ReplicationNodeRole enum value.");
        _meter.CreateObservableGauge("btdb.replication.status.age", () =>
            Math.Max(0, (clock.Elapsed - status.Current.SampledAt).TotalSeconds), "s",
            "Age of the last coordinator status sample; a blocked operation may prevent updates.");
    }

    public void Dispose() => _meter.Dispose();
}
