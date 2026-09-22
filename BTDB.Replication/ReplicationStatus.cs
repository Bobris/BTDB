using System;
using System.Collections.Generic;
using System.Threading;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>Sampled completed cuts, not reader-visible roots or client durability acknowledgements.
/// Compared is historical byte equality, not a live confirmation grant. Null means unavailable.</summary>
public sealed record ReplicationDatabaseStatus(string Name, LeaderTrlProgress? LocalCommitted,
    LeaderTrlProgress? Compared, TransactionLogPosition? Published, bool Removed, bool Detached);

/// <summary>Credential-free local status. SampledAt uses the injected node-local monotonic clock.
/// Ready means restored and available for ordinary local work, not caught up or authorized to publish.</summary>
public sealed record ReplicationNodeStatus(ReplicationNodeRole Role, bool Ready, TimeSpan SampledAt,
    IReadOnlyList<ReplicationDatabaseStatus> Databases);

/// <summary>Thread-safe sampled diagnostics. No storage or peer I/O occurs on reads.</summary>
public sealed class ReplicationStatus
{
    ReplicationNodeStatus _current = new(ReplicationNodeRole.Restoring, false, TimeSpan.Zero,
        Array.Empty<ReplicationDatabaseStatus>());
    int _stopping;

    public ReplicationNodeStatus Current
    {
        get
        {
            var current = Volatile.Read(ref _current);
            return current.Ready && Volatile.Read(ref _stopping) != 0 ? current with { Ready = false } : current;
        }
    }

    internal void Update(ReplicationNodeStatus status) => Volatile.Write(ref _current, status);
    // Sticky: late coordinator callbacks cannot make a stopping host ready again.
    internal void Stop() => Interlocked.Exchange(ref _stopping, 1);
}
