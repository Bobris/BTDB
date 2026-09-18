using System;

namespace BTDB.Replication;

/// <summary>
/// Conservative local lease deadline. The supplied monotonic clock must include process/OS pauses and remain
/// within the configured rate bound. This is not a lease client or proof that the leader record was selected.
/// </summary>
internal sealed class LeaseAuthority
{
    const long Scale = 1_000_000;
    readonly IReplicationScheduler _clock;
    readonly long _slowRate;
    readonly long _fastRate;
    readonly long _margin;
    long _request;
    long _requestStarted;
    long _deadline;
    bool _held;
    bool _fenced;

    public LeaseAuthority(IReplicationScheduler clock, int maximumClockDriftPpm, TimeSpan safetyMargin)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maximumClockDriftPpm);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(maximumClockDriftPpm, Scale);
        ArgumentOutOfRangeException.ThrowIfNegative(safetyMargin.Ticks);
        _clock = clock;
        _slowRate = Scale - maximumClockDriftPpm;
        _fastRate = Scale + maximumClockDriftPpm;
        _margin = safetyMargin.Ticks;
    }

    public bool IsValid
    {
        get
        {
            if (_held && _clock.Elapsed.Ticks >= _deadline) Fence();
            return _held && !_fenced;
        }
    }

    public bool IsFenced => _fenced;
    public TimeSpan Deadline => TimeSpan.FromTicks(_deadline);

    /// <summary>Call immediately before dispatch, not when the acquire/renew response arrives.</summary>
    public long BeginRequest()
    {
        _ = IsValid;
        if (_fenced) throw new InvalidOperationException("A fenced session cannot renew its way back into authority.");
        _requestStarted = _clock.Elapsed.Ticks;
        return checked(++_request);
    }

    public bool AcceptSuccess(long request, TimeSpan guaranteedLeaseDuration)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(guaranteedLeaseDuration.Ticks);
        _ = IsValid;
        if (_fenced || request == 0 || request != _request) return false;
        var duration = (long)((Int128)guaranteedLeaseDuration.Ticks * _slowRate / Scale) - _margin;
        if (duration <= 0 || (Int128)_requestStarted + duration > long.MaxValue)
        {
            Fence();
            return false;
        }
        var deadline = _requestStarted + duration;
        if (_clock.Elapsed.Ticks >= deadline)
        {
            Fence();
            return false;
        }
        _held = true;
        _deadline = deadline;
        return true;
    }

    // Ambiguous acquire supplies no authority. Ambiguous renewal leaves only the previously proven deadline.
    public void Fence()
    {
        _fenced = true;
        _held = false;
    }

    /// <summary>
    /// Upper bound, in this clock's ticks, on a peer window measured by another clock with the same rate bound.
    /// Used both to fit a grant inside authority and to wait out grants before an early lease transfer.
    /// </summary>
    public TimeSpan BoundPeerWindow(TimeSpan peerDuration)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(peerDuration.Ticks);
        var numerator = (Int128)peerDuration.Ticks * _fastRate;
        return TimeSpan.FromTicks(checked((long)((numerator + _slowRate - 1) / _slowRate)));
    }

    public bool CanGrant(TimeSpan peerDuration) => IsValid &&
        (Int128)_clock.Elapsed.Ticks + BoundPeerWindow(peerDuration).Ticks < _deadline;
}
