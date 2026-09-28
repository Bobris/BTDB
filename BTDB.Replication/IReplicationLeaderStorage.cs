using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>Provider-confirmed lease. Handle is a credential and is deliberately excluded from ToString.</summary>
public sealed record LeaseGrant(string Handle, TimeSpan GuaranteedDuration)
{
    public override string ToString() => $"LeaseGrant {{ GuaranteedDuration = {GuaranteedDuration} }}";
}

/// <summary>A version-bound leader document. Json includes peer credentials; do not log or serialize it as diagnostics.</summary>
public sealed record LeaderRecord(string Token, string Json)
{
    public override string ToString() => nameof(LeaderRecord);
}
public enum LeaderWriteOutcome { Applied, Rejected, Ambiguous }

/// <summary>
/// The cluster leader document and its finite lease; record writes are bound to a handle from the same lease.
/// Null means ownership was not confirmed (including an ambiguous response).
/// Acquisition must establish exclusive ownership, with a fresh handle that old requests cannot use to renew
/// or release the new lease. A possibly landed acquire is reconciled by the adapter before returning a grant.
/// Renewal is bound to exactly the supplied handle. Durations are conservative bounds measured from dispatch.
/// </summary>
public interface IReplicationLeaderStorage
{
    ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation);
    ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation);
    /// <summary>Planned handoff: change the lease to the proposed handle, confirmed by the target's renewal.</summary>
    ValueTask TransferAsync(string currentHandle, string proposedHandle, CancellationToken cancellation);

    ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation);
    ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string expectedToken, string json,
        CancellationToken cancellation);
}
