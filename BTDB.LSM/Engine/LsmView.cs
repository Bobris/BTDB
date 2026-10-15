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

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlyMemory<byte> value) => TryGetFrom(0, key, out value);

    bool TryGetFrom(int first, ReadOnlySpan<byte> key, out ReadOnlyMemory<byte> value)
    {
        for (var i = first; i < _components.Length; i++)
        {
            if (_components[i].TryGet(key, out var entry))
            {
                value = entry.Value;
                return entry.IsSet;
            }

            if (_components[i].Tombstones.Covers(key)) break;
        }

        value = default;
        return false;
    }

    /// First live key greater than (or equal to, when inclusive) key.
    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out byte[] liveKey, out ReadOnlyMemory<byte> value)
    {
        var count = _components.Length;
        var bounds = new byte[count][];
        var boundInclusive = new bool[count];
        var candidates = new LsmEntry[count];
        var has = new bool[count];
        var stale = new bool[count];
        var start = key.ToArray();
        for (var i = 0; i < count; i++)
        {
            bounds[i] = start;
            boundInclusive[i] = inclusive;
            stale[i] = true;
        }

        while (true)
        {
            byte[]? best = null;
            for (var i = 0; i < count; i++)
            {
                if (stale[i])
                {
                    has[i] = _components[i].SeekGE(bounds[i], boundInclusive[i], out candidates[i]);
                    stale[i] = false;
                }

                if (has[i] && (best == null || LsmKeys.Compare(candidates[i].Key, best) < 0)) best = candidates[i].Key;
            }

            if (best == null)
            {
                liveKey = [];
                value = default;
                return false;
            }

            var masked = -1;
            for (var i = 0; i < count; i++)
            {
                if (has[i] && LsmKeys.Compare(candidates[i].Key, best) == 0)
                {
                    if (candidates[i].IsSet)
                    {
                        liveKey = best;
                        value = candidates[i].Value;
                        return true;
                    }

                    break;
                }

                if (_components[i].Tombstones.Find(best, out _) is >= 0 and var fragment)
                {
                    masked = i;
                    var end = _components[i].Tombstones[fragment].End;
                    // Everything older than the masking component is hidden up to the fragment end.
                    for (var j = i + 1; j < count; j++)
                    {
                        if (LsmKeys.Compare(bounds[j], end) < 0)
                        {
                            bounds[j] = end;
                            boundInclusive[j] = true;
                            stale[j] = true;
                        }
                    }

                    break;
                }
            }

            for (var i = 0; i < (masked >= 0 ? masked + 1 : count); i++)
            {
                if (!has[i] || LsmKeys.Compare(candidates[i].Key, best) != 0) continue;
                bounds[i] = best;
                boundInclusive[i] = false;
                stale[i] = true;
            }
        }
    }

    /// Last live key lower than (or equal to, when inclusive) key; a null key means no upper bound.
    public bool SeekLE(ReadOnlySpan<byte> key, bool unbounded, bool inclusive, out byte[] liveKey,
        out ReadOnlyMemory<byte> value)
    {
        var count = _components.Length;
        var bounds = new byte[]?[count];
        var boundInclusive = new bool[count];
        var candidates = new LsmEntry[count];
        var has = new bool[count];
        var stale = new bool[count];
        var start = unbounded ? null : key.ToArray();
        for (var i = 0; i < count; i++)
        {
            bounds[i] = start;
            boundInclusive[i] = inclusive;
            stale[i] = true;
        }

        while (true)
        {
            byte[]? best = null;
            for (var i = 0; i < count; i++)
            {
                if (stale[i])
                {
                    has[i] = bounds[i] == null
                        ? _components[i].SeekLast(out candidates[i])
                        : _components[i].SeekLE(bounds[i], boundInclusive[i], out candidates[i]);
                    stale[i] = false;
                }

                if (has[i] && (best == null || LsmKeys.Compare(candidates[i].Key, best) > 0)) best = candidates[i].Key;
            }

            if (best == null)
            {
                liveKey = [];
                value = default;
                return false;
            }

            var masked = -1;
            for (var i = 0; i < count; i++)
            {
                if (has[i] && LsmKeys.Compare(candidates[i].Key, best) == 0)
                {
                    if (candidates[i].IsSet)
                    {
                        liveKey = best;
                        value = candidates[i].Value;
                        return true;
                    }

                    break;
                }

                if (_components[i].Tombstones.Find(best, out _) is >= 0 and var fragment)
                {
                    masked = i;
                    var fragmentStart = _components[i].Tombstones[fragment].Start;
                    for (var j = i + 1; j < count; j++)
                    {
                        if (bounds[j] == null || LsmKeys.Compare(bounds[j], fragmentStart) > 0 ||
                            boundInclusive[j] && LsmKeys.Compare(bounds[j], fragmentStart) == 0)
                        {
                            bounds[j] = fragmentStart;
                            boundInclusive[j] = false;
                            stale[j] = true;
                        }
                    }

                    break;
                }
            }

            for (var i = 0; i < (masked >= 0 ? masked + 1 : count); i++)
            {
                if (!has[i] || LsmKeys.Compare(candidates[i].Key, best) != 0) continue;
                bounds[i] = best;
                boundInclusive[i] = false;
                stale[i] = true;
            }
        }
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
        if (!SeekGE(low ?? [], true, out var key, out _))
            throw new InvalidOperationException("Select did not find a live key; counts are inconsistent.");
        while (skip > 0)
        {
            if (!SeekGE(key, false, out key, out _))
                throw new InvalidOperationException("Select did not find a live key; counts are inconsistent.");
            skip--;
        }

        return key;
    }
}
