using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>Provider-confirmed lease. Handle is a credential and is deliberately excluded from ToString.</summary>
public sealed record LeaseGrant(string Handle, TimeSpan GuaranteedDuration)
{
    public override string ToString() => $"LeaseGrant {{ GuaranteedDuration = {GuaranteedDuration} }}";
}

/// <summary>
/// Provider lease operations. Null means ownership was not confirmed (including an ambiguous response).
/// Acquisition must establish exclusive ownership, with a fresh handle that old requests cannot use to renew
/// or release the new lease. A possibly landed acquire is reconciled by the adapter before returning a grant.
/// Renewal is bound to exactly the supplied handle. Durations are conservative bounds measured from dispatch.
/// </summary>
public interface IReplicationLeaseStorage
{
    ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation);
    ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation);
}

public interface IReplicationLeaseTransferStorage
{
    ValueTask TransferAsync(string currentHandle, string proposedHandle, CancellationToken cancellation);
}

/// <summary>
/// Node lifetime lease acquisition/renewal. Maintenance is serialized by the owner; authority/handle snapshots
/// are synchronized with independent activation and publication lanes. Acquiring a lease does not activate publication: each new authority
/// still requires term selection and canonical history validation. Old authority objects stay fenced forever.
/// </summary>
internal sealed class LeaseSessionController(IReplicationLeaseStorage storage, IReplicationScheduler clock,
    int maximumClockDriftPpm, TimeSpan safetyMargin)
{
    readonly object _stateLock = new();
    LeaseAuthority? _authority;
    string? _handle;
    volatile bool _closed;
    bool _ineligible;
    string? _proposedHandle;
    TimeSpan? _requestTimeout, _confirmationDuration;

    public void ProposeTransfer(string handle)
    {
        lock (_stateLock) if (!_ineligible && !_closed) _proposedHandle = handle;
    }

    public async ValueTask TransferAsync(string proposedHandle, CancellationToken cancellation)
    {
        if (storage is not IReplicationLeaseTransferStorage transfer)
            throw new NotSupportedException("The lease provider does not support transfer.");
        string handle;
        lock (_stateLock)
        {
            if (Current == null) throw new InvalidOperationException("Transfer requires live authority.");
            handle = _handle!;
            Disqualify(); // Stop renewal and publication even if the response is lost.
        }
        await transfer.TransferAsync(handle, proposedHandle, cancellation).ConfigureAwait(false);
    }

    // Permanent for this node lifetime. A delayed acquisition/renewal cannot restore eligibility.
    public void Disqualify()
    {
        lock (_stateLock)
        {
            _ineligible = true;
            _authority?.Fence();
        }
    }

    public LeaseAuthority? Current { get { lock (_stateLock) return _authority is { IsValid: true } ? _authority : null; } }

    internal string GetHandle(LeaseAuthority authority)
    {
        lock (_stateLock) return ReferenceEquals(Current, authority)
            ? _handle! : throw new InvalidOperationException("The lease session is no longer current.");
    }

    /// <summary>
    /// Owns maintenance until cancellation or a non-I/O failure. Do not call MaintainAsync concurrently.
    /// The caller observes this task and activates newly acquired authority separately. I/O failures retry;
    /// application/provider contract errors propagate. No renewal waits on database work or publication.
    /// </summary>
    public async Task RunAsync(TimeSpan retryInterval, Action<LeaseAuthority?> observed,
        CancellationToken cancellation, TimeSpan? requestTimeout = null, TimeSpan? confirmationDuration = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(retryInterval.Ticks);
        if (requestTimeout is { } timeoutDuration)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(timeoutDuration.Ticks);
        if (confirmationDuration is { } confirmation)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(confirmation.Ticks);
        _requestTimeout = requestTimeout;
        _confirmationDuration = confirmationDuration;
        ArgumentNullException.ThrowIfNull(observed);
        try
        {
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();
                if (_closed) return;
                var previousDeadline = Current?.Deadline;
                var failed = false;
                using (var request = CancellationTokenSource.CreateLinkedTokenSource(cancellation))
                using (var timeout = requestTimeout is { } duration
                           ? clock.Schedule(duration, () => ReplicationTimeouts.Cancel(request), "lease request timeout") : null)
                {
                    try { await MaintainAsync(request.Token).ConfigureAwait(false); }
                    catch (InvalidOperationException) when (_closed) { return; }
                    catch (IOException) { failed = true; }
                    catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { failed = true; }
                }
                cancellation.ThrowIfCancellationRequested();
                var current = Current;
                // Healthy sessions renew at least halfway through their remaining conservative lifetime. A failed
                // renewal retries no later than that either, so one transient error cannot outlast the lease; halving
                // the remaining time bounds the attempts before expiry. An unconfirmed renewal keeps the retry interval.
                var delay = retryInterval;
                if (current != null && (failed || previousDeadline == null || current.Deadline > previousDeadline.Value))
                    delay = TimeSpan.FromTicks(Math.Min(delay.Ticks,
                        Math.Max(1, (current.Deadline - clock.Elapsed).Ticks / 2)));
                var ready = new TaskCompletionSource();
                using var scheduled = clock.Schedule(delay, () => ready.TrySetResult(), "lease maintenance");
                using var registration = cancellation.Register(() => ready.TrySetCanceled(cancellation));
                observed(current);
                await ready.Task.ConfigureAwait(false);
            }
        }
        finally { Close(); }
    }

    /// <summary>
    /// Call again after an outage. Unconfirmed renewal preserves only the previously proven deadline;
    /// after expiry the next attempt acquires a new session. Exceptions propagate for owner retry scheduling.
    /// No application work or database lifetime is controlled here.
    /// </summary>
    public async ValueTask<LeaseAuthority?> MaintainAsync(CancellationToken cancellation = default)
    {
        if (_closed) throw new InvalidOperationException("Lease acquisition is closed for this node session.");
        cancellation.ThrowIfCancellationRequested();
        lock (_stateLock) if (_ineligible) return null;
        // Expiry, transfer or disqualification can fence the session between these two checks.
        if (Current is { } current && current.TryBeginRequest(out var request))
        {
            var duration = await storage.RenewAsync(_handle!, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (duration is { } confirmed) current.AcceptSuccess(request, confirmed);
            return Current;
        }

        // Never revive the previous object: publishers and readers may still hold references to it.
        lock (_stateLock)
        {
            _authority?.Fence();
            _authority = null;
            _handle = null;
            // A concurrent transfer or disqualification must not dispatch an acquisition nobody would renew.
            if (_ineligible) return null;
        }
        var candidate = new LeaseAuthority(clock, maximumClockDriftPpm, safetyMargin);
        var acquire = candidate.BeginRequest();
        string? proposed;
        lock (_stateLock) proposed = _proposedHandle;
        LeaseGrant? grant = null;
        if (proposed != null && await storage.RenewAsync(proposed, cancellation).ConfigureAwait(false) is { } transferred)
            grant = new(proposed, transferred);
        grant ??= await storage.AcquireAsync(cancellation).ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        if (grant != null) Validate(candidate, grant.GuaranteedDuration);
        lock (_stateLock)
        {
            if (_closed || _ineligible || grant == null || !candidate.AcceptSuccess(acquire, grant.GuaranteedDuration)) return null;
            // A transferred handle is consumed; a later reacquisition must not renew it again.
            if (grant.Handle == _proposedHandle) _proposedHandle = null;
            _handle = grant.Handle;
            _authority = candidate;
            return candidate;
        }
    }

    // A configuration the provider's lease cannot support would otherwise never hold authority, lose it to one slow
    // renewal, or never issue a confirmation grant (so detached nodes restart). Fail the node instead of stalling.
    void Validate(LeaseAuthority candidate, TimeSpan guaranteed)
    {
        var usable = candidate.UsableDuration(guaranteed);
        if (usable <= TimeSpan.Zero)
            throw new InvalidOperationException($"A {guaranteed.TotalSeconds:0.###} s lease leaves no authority after " +
                                                $"the {maximumClockDriftPpm} ppm drift bound and {safetyMargin.TotalMilliseconds:0} ms safety margin.");
        if (_requestTimeout is { } timeout && timeout.Ticks * 2 >= usable.Ticks)
            throw new InvalidOperationException($"RequestTimeout {timeout.TotalSeconds:0.###} s must be below half of " +
                                                $"the {usable.TotalSeconds:0.###} s usable lease authority.");
        if (_confirmationDuration is { } confirmation && candidate.BoundPeerWindow(confirmation).Ticks * 2 >= usable.Ticks)
            throw new InvalidOperationException($"ConfirmationDuration {confirmation.TotalSeconds:0.###} s must be below " +
                                                $"half of the {usable.TotalSeconds:0.###} s usable lease authority.");
    }

    public void Close()
    {
        lock (_stateLock)
        {
            _closed = true;
            _authority?.Fence();
        }
    }
}
