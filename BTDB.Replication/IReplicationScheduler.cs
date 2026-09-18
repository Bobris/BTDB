using System;

namespace BTDB.Replication;

/// <summary>Node-scoped monotonic scheduling. Callbacks must be serialized by the implementation.</summary>
internal interface IReplicationScheduler
{
    TimeSpan Elapsed { get; }

    /// <summary>
    /// Queue a callback, including for zero delay; never invoke it inline.
    /// Disposal prevents a queued callback from starting, but does not undo an external operation it dispatched.
    /// </summary>
    IDisposable Schedule(TimeSpan delay, Action callback, string description);
}
