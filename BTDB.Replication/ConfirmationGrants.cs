using System;

namespace BTDB.Replication;

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
