namespace BTDB.Replication;

/// <summary>Injected entropy; deterministic implementations can reproduce IDs and backoff choices.</summary>
internal interface IReplicationRandom
{
    ulong NextUInt64();
}
