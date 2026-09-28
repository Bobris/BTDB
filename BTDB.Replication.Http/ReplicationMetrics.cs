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
        _meter.CreateObservableCounter("btdb.replication.restore.attempts", () => status.RestoreAttempts,
            description: "Restore attempts of this node, including retries.");
        _meter.CreateObservableCounter("btdb.replication.restore.failures", () => status.RestoreFailures,
            description: "Restore attempts that failed with retryable I/O.");
        _meter.CreateObservableGauge("btdb.replication.restore.duration", () =>
            status.RestoreDuration is { } duration ? new[] { new Measurement<double>(duration.TotalSeconds) } : [], "s",
            "Duration of this node's completed restore, including retried attempts.");
        _meter.CreateObservableCounter("btdb.replication.leader.sessions", () => status.LeaderSessions,
            description: "Leader sessions this node activated.");
        _meter.CreateObservableCounter("btdb.replication.leader.sessions.ended", () => status.LostLeaderSessions,
            description: "Activated leader sessions that ended while the node kept running.");
        _meter.CreateObservableCounter("btdb.replication.step.failures", () => status.FailedSteps,
            description: "Replication steps that failed with I/O or a timeout, such as an unreachable leader.");
    }

    public void Dispose() => _meter.Dispose();
}
