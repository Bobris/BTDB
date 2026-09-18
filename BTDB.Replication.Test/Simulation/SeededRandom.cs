using System;

namespace BTDB.Replication.Test.Simulation;

// SplitMix64: a fixed algorithm, independent of System.Random/runtime versions.
internal sealed class SeededRandom(ulong seed) : IReplicationRandom
{
    ulong _state = seed;

    public ulong NextUInt64()
    {
        unchecked
        {
            var value = _state += 0x9E3779B97F4A7C15ul;
            value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9ul;
            value = (value ^ (value >> 27)) * 0x94D049BB133111EBul;
            return value ^ (value >> 31);
        }
    }

    public TimeSpan Backoff(TimeSpan maximum)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maximum.Ticks);
        return TimeSpan.FromTicks((long)(NextUInt64() % ((ulong)maximum.Ticks + 1)));
    }
}
