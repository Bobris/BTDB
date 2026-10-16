using System;
using System.Collections.Generic;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// Read logic over a stack of components, newest first (ImplementationPlan.md, sections 6 and 7.3): existence, rank,
/// index-only rank bounds, select, and seeks that resolve each candidate key by the newest point or masking tombstone.
sealed class LsmView
{
    readonly ILsmComponent[] _components;

    public LsmView(ILsmComponent[] components)
    {
        _components = components;
    }

    public ReadOnlySpan<ILsmComponent> Components => _components;

    public long Count => CountFrom(0);

    public long CountFrom(int first)
    {
        long count = 0;
        for (var i = first; i < _components.Length; i++)
            count += _components[i].DeltaSum - _components[i].Tombstones.CoveredSum;
        return count;
    }

    /// Number of live keys lower than key.
    public long Rank(ReadOnlySpan<byte> key) => RankFrom(0, key);

    // rank_C(x) = points_C(<x) - covered_C(fragments ending at or before x) + rank_older(x'), where x' is the start of
    // the fragment containing x, or x itself (ImplementationPlan.md, section 6.3).
    public long RankFrom(int first, ReadOnlySpan<byte> key)
    {
        long rank = 0;
        var current = key;
        for (var i = first; i < _components.Length; i++)
        {
            var component = _components[i];
            rank += component.PointDeltaBefore(current);
            var fragment = component.Tombstones.Find(current, out var coveredBefore);
            rank -= coveredBefore;
            if (fragment >= 0) current = component.Tombstones[fragment].Start;
        }

        return rank;
    }

    public (long Min, long Max) RankBounds(ReadOnlySpan<byte> key)
    {
        long min = 0, max = 0;
        var current = key;
        foreach (var component in _components)
        {
            var (pointMin, pointMax) = component.PointDeltaBounds(current);
            var fragment = component.Tombstones.Find(current, out var coveredBefore);
            min += pointMin - coveredBefore;
            max += pointMax - coveredBefore;
            if (fragment >= 0) current = component.Tombstones[fragment].Start;
        }

        return (min, max);
    }

    public bool Exists(ReadOnlySpan<byte> key) => ExistsFrom(0, key);

    public bool ExistsFrom(int first, ReadOnlySpan<byte> key) => TryGetFrom(first, key, out _);

    public bool TryGet(ReadOnlySpan<byte> key, out LsmValue value) => TryGetFrom(0, key, out value);

    bool TryGetFrom(int first, ReadOnlySpan<byte> key, out LsmValue value)
    {
        for (var i = first; i < _components.Length; i++)
        {
            if (_components[i].TryGet(key, out var entry))
            {
                value = entry.LsmValue;
                return entry.IsSet;
            }

            if (_components[i].Tombstones.Covers(key)) break;
        }

        value = default;
        return false;
    }

    /// First live key greater than (or equal to, when inclusive) key.
    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out byte[] liveKey, out LsmValue value)
    {
        var iterator = new MergingIterator(this);
        var found = iterator.SeekGE(key, inclusive);
        liveKey = found ? iterator.Key : [];
        value = found ? iterator.Value : default;
        return found;
    }

    /// Last live key lower than (or equal to, when inclusive) key; unbounded means no upper bound.
    public bool SeekLE(ReadOnlySpan<byte> key, bool unbounded, bool inclusive, out byte[] liveKey,
        out LsmValue value)
    {
        var iterator = new MergingIterator(this);
        var found = iterator.SeekLE(key, unbounded, inclusive);
        liveKey = found ? iterator.Key : [];
        value = found ? iterator.Value : default;
        return found;
    }

    /// Live key with the given rank; index must be in [0, Count).
    public byte[] Select(long index, int selectFinalBlocks = 2)
    {
        byte[]? low = null;
        byte[]? high = null;
        while (true)
        {
            ILsmComponent? best = null;
            int bestFrom = 0, bestTo = 0;
            foreach (var component in _components)
            {
                var from = low == null ? 0 : component.CountBlocksBefore(low, true);
                var to = high == null ? component.BlockCount : component.CountBlocksBefore(high, false);
                if (to - from > Math.Max(bestTo - bestFrom, selectFinalBlocks))
                {
                    best = component;
                    bestFrom = from;
                    bestTo = to;
                }
            }

            if (best == null) break;
            var pivot = best.BlockFirstKey((bestFrom + bestTo) >>> 1);
            var (minRank, maxRank) = RankBounds(pivot);
            var below = maxRank <= index || minRank <= index && Rank(pivot) <= index;
            if (below) low = pivot;
            else high = pivot;
        }

        var skip = index - (low == null ? 0 : Rank(low));
        var iterator = new MergingIterator(this);
        var found = iterator.SeekGE(low ?? [], true);
        for (; found && skip > 0; skip--) found = iterator.Next();
        if (!found) throw new InvalidOperationException("Select did not find a live key; counts are inconsistent.");
        return iterator.Key;
    }
}
