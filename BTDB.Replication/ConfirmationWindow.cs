using System;

namespace BTDB.Replication;

/// <summary>One follower session's challenge window; challenge IDs are never reused within this object.</summary>
internal sealed class ConfirmationWindow(IReplicationScheduler clock)
{
    long _challenge;
    long _deadline;
    bool _accepted;
    bool _closed;

    public long BeginChallenge(TimeSpan duration)
    {
        if (_closed) throw new InvalidOperationException("The leader session is closed.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(duration.Ticks);
        _deadline = checked(clock.Elapsed.Ticks + duration.Ticks);
        _accepted = false;
        return checked(++_challenge);
    }

    // The transport/role owner must validate database, leader term and connection before calling this method.
    public bool Accept(long challenge)
    {
        if (_closed || challenge == 0 || challenge != _challenge || clock.Elapsed.Ticks >= _deadline) return false;
        _accepted = true;
        return true;
    }

    public bool IsValid => !_closed && _accepted && clock.Elapsed.Ticks < _deadline;

    public void Close()
    {
        _closed = true;
        _accepted = false;
    }
}

/// <summary>One leader session's conservative drain bound; no per-follower revocation protocol is needed.</summary>
internal sealed class ConfirmationGrants(IReplicationScheduler clock, LeaseAuthority authority)
{
    long _drainUntil;
    bool _draining;

    public bool TryIssue(TimeSpan peerDuration)
    {
        if (_draining || !authority.CanGrant(peerDuration)) return false;
        _drainUntil = Math.Max(_drainUntil, checked(clock.Elapsed.Ticks + authority.BoundPeerWindow(peerDuration).Ticks));
        return true;
    }

    public void BeginDrain() => _draining = true;
    public bool IsDrained => _draining && clock.Elapsed.Ticks >= _drainUntil;
}
