using System;
using System.Collections.Generic;

namespace BTDB.LSM.Counting;

sealed class CountedStackOptions
{
    public int BlockSize { get; init; } = 16;
    public int RestartInterval { get; init; } = 4;

    /// Ranges with at most this many live keys are erased with point deletes instead of a range tombstone.
    public int PointDeleteRangeLimit { get; init; } = 64;

    /// Select stops bisecting once no component has more than this many block boundaries inside the interval.
    public int SelectFinalBlocks { get; init; } = 2;
}

/// Phase L0 prototype of the counted LSM model (ImplementationPlan.md, section 6): a mutable memtable over a stack of
/// immutable components, newest first, with exact rank, select and count. It models the table layout and counts its
/// accesses, but keeps everything in managed memory and has no values, persistence or concurrency.
sealed class CountedStack
{
    readonly CountedStackOptions _options;
    readonly List<CountedComponent> _immutables = [];
    readonly List<CountedPoint> _memPoints = [];
    readonly List<CountedTombstone> _memTombstones = [];
    CountedComponent? _memFrozen;

    public CountedStack(CountedStackOptions options)
    {
        _options = options;
    }

    public CountedStats Stats { get; } = new();
    public IReadOnlyList<CountedComponent> Immutables => _immutables;
    public int MemtableEntryCount => _memPoints.Count + _memTombstones.Count;

    CountedComponent? Memtable
    {
        get
        {
            if (_memPoints.Count == 0 && _memTombstones.Count == 0) return null;
            return _memFrozen ??= new([.. _memPoints], [.. _memTombstones], _options.BlockSize,
                _options.RestartInterval);
        }
    }

    public long Count
    {
        get
        {
            long count = 0;
            var mem = Memtable;
            if (mem != null) count += mem.DeltaSum - mem.CoveredSum;
            foreach (var component in _immutables) count += component.DeltaSum - component.CoveredSum;
            return count;
        }
    }

    /// Number of live keys lower than key.
    public long Rank(byte[] key) => RankCore(key, true, 0);

    long RankCore(byte[] key, bool includeMemtable, int firstImmutable)
    {
        Stats.RankCalls++;
        long rank = 0;
        var current = key;
        if (includeMemtable)
        {
            var mem = Memtable;
            if (mem != null) rank += RankStep(mem, ref current);
        }

        for (var i = firstImmutable; i < _immutables.Count; i++) rank += RankStep(_immutables[i], ref current);
        return rank;
    }

    // rank_C(x) = points_C(<x) - covered_C(fragments ending at or before x) + rank_below(x'), where x' is the start
    // of the fragment containing x, or x itself. The key passed to older components therefore only decreases.
    long RankStep(CountedComponent component, ref byte[] current)
    {
        var rank = component.PointDeltaBefore(current, Stats);
        var fragment = component.FindTombstone(current, out var coveredBefore);
        rank -= coveredBefore;
        if (fragment >= 0) current = component.Tombstones[fragment].Start;
        return rank;
    }

    public bool Exists(byte[] key) => ExistsCore(key, true, 0);

    bool ExistsCore(byte[] key, bool includeMemtable, int firstImmutable)
    {
        if (includeMemtable)
        {
            var index = MemPointIndex(key);
            if (index >= 0) return _memPoints[index].IsSet;
            if (MemCovers(key)) return false;
        }

        for (var i = firstImmutable; i < _immutables.Count; i++)
        {
            if (ExistsStep(_immutables[i], key) is { } result) return result;
        }

        return false;
    }

    bool? ExistsStep(CountedComponent component, byte[] key)
    {
        if (component.TryGetPoint(key, Stats, out var point)) return point.IsSet;
        if (component.Covers(key)) return false;
        return null;
    }

    /// Live key with rank index (0-based).
    public byte[] Select(long index)
    {
        if (index < 0 || index >= Count) throw new ArgumentOutOfRangeException(nameof(index));
        Stats.SelectCalls++;
        var components = AllComponents();
        byte[]? low = null;
        byte[]? high = null;
        while (true)
        {
            CountedComponent? best = null;
            int bestFrom = 0, bestTo = 0;
            foreach (var component in components)
            {
                var from = low == null ? 0 : component.CountBlocksBefore(low, true);
                var to = high == null ? component.BlockCount : component.CountBlocksBefore(high, false);
                if (to - from > Math.Max(bestTo - bestFrom, _options.SelectFinalBlocks))
                {
                    best = component;
                    bestFrom = from;
                    bestTo = to;
                }
            }

            if (best == null) break;
            var pivot = best.BlockFirstKey((bestFrom + bestTo) >>> 1);
            Stats.SelectPivots++;
            // Index-only bounds decide most pivots; only a pivot near the target needs data blocks.
            var (minRank, maxRank) = RankBounds(pivot);
            bool below;
            if (maxRank <= index) below = true;
            else if (minRank > index) below = false;
            else
            {
                Stats.SelectExactPivots++;
                below = Rank(pivot) <= index;
            }

            if (below) low = pivot;
            else high = pivot;
        }

        var skip = index - (low == null ? 0 : Rank(low));
        foreach (var key in LiveKeysFrom(low))
        {
            if (skip == 0) return key;
            skip--;
            Stats.SelectSteps++;
        }

        throw new InvalidOperationException("Select did not find a live key; counts are inconsistent.");
    }

    /// Lower and upper bound of Rank(key) computed from index data only, without reading data blocks.
    public (long Min, long Max) RankBounds(byte[] key)
    {
        long min = 0, max = 0;
        var current = key;
        foreach (var component in AllComponents())
        {
            var (pointMin, pointMax) = component.PointDeltaBounds(current, Stats);
            var fragment = component.FindTombstone(current, out var coveredBefore);
            min += pointMin - coveredBefore;
            max += pointMax - coveredBefore;
            if (fragment >= 0) current = component.Tombstones[fragment].Start;
        }

        return (min, max);
    }

    List<CountedComponent> AllComponents()
    {
        var result = new List<CountedComponent>(_immutables.Count + 1);
        var mem = Memtable;
        if (mem != null) result.Add(mem);
        result.AddRange(_immutables);
        return result;
    }

    /// Live keys in ascending order starting at key (inclusive), or at the beginning for null. A merging iterator:
    /// for each key the newest point decides, unless a newer component's tombstone masks it first. Every component
    /// block entered counts as one block read.
    public IEnumerable<byte[]> LiveKeysFrom(byte[]? key)
    {
        var components = AllComponents();
        var positions = new int[components.Count];
        var lastBlocks = new int[components.Count];
        for (var i = 0; i < components.Count; i++)
        {
            Stats.IndexReads++;
            positions[i] = key == null ? 0 : components[i].LowerBound(key);
            lastBlocks[i] = -1;
        }

        while (true)
        {
            byte[]? next = null;
            for (var i = 0; i < components.Count; i++)
            {
                var points = components[i].Points;
                if (positions[i] >= points.Length) continue;
                var block = components[i].BlockOf(positions[i]);
                if (block != lastBlocks[i])
                {
                    lastBlocks[i] = block;
                    Stats.BlockReads++;
                }

                if (next == null || CountedKey.Compare(points[positions[i]].Key, next) < 0)
                    next = points[positions[i]].Key;
            }

            if (next == null) yield break;
            bool? live = null;
            for (var i = 0; i < components.Count; i++)
            {
                var points = components[i].Points;
                var atKey = positions[i] < points.Length && CountedKey.Compare(points[positions[i]].Key, next) == 0;
                if (live == null)
                {
                    if (atKey) live = points[positions[i]].IsSet;
                    else if (components[i].Covers(next)) live = false;
                }

                if (atKey) positions[i]++;
            }

            if (live == true) yield return next;
        }
    }

    /// Creates or updates key; returns true when the key was created.
    public bool Set(byte[] key)
    {
        var created = !Exists(key);
        var index = MemPointIndex(key);
        if (index >= 0)
        {
            _memPoints[index] = _memPoints[index] with { IsSet = true };
        }
        else
        {
            var existsBelow = !MemCovers(key) && ExistsCore(key, false, 0);
            _memPoints.Insert(~index, new(key, true, existsBelow));
        }

        _memFrozen = null;
        return created;
    }

    /// Erases an existing key; returns false when the key does not exist.
    public bool Delete(byte[] key)
    {
        if (!Exists(key)) return false;
        var index = MemPointIndex(key);
        if (index >= 0)
        {
            if (_memPoints[index].ExistsBelow) _memPoints[index] = _memPoints[index] with { IsSet = false };
            else _memPoints.RemoveAt(index);
        }
        else
        {
            // A live key without a memtable point cannot be covered by a memtable tombstone.
            _memPoints.Insert(~index, new(key, false, true));
        }

        _memFrozen = null;
        return true;
    }

    /// Erases all keys in [start, end); returns the number of erased keys.
    public long DeleteRange(byte[] start, byte[] end)
    {
        if (CountedKey.Compare(start, end) >= 0) return 0;
        var count = Rank(end) - Rank(start);
        if (count == 0) return 0;
        if (count <= _options.PointDeleteRangeLimit)
        {
            var keys = new List<byte[]>();
            foreach (var key in LiveKeysFrom(start))
            {
                if (CountedKey.Compare(key, end) >= 0) break;
                keys.Add(key);
            }

            foreach (var key in keys) Delete(key);
            return count;
        }

        // Memtable points inside the range are replaced by the tombstone.
        var firstPoint = LowerBoundMemPoints(start);
        var endPoint = LowerBoundMemPoints(end);
        _memPoints.RemoveRange(firstPoint, endPoint - firstPoint);

        // Merge with overlapping or touching memtable fragments into one span.
        var spanStart = start;
        var spanEnd = end;
        var removeFrom = _memTombstones.Count;
        var removeTo = 0;
        for (var i = 0; i < _memTombstones.Count; i++)
        {
            var fragment = _memTombstones[i];
            if (CountedKey.Compare(fragment.End, start) < 0 || CountedKey.Compare(fragment.Start, end) > 0) continue;
            if (CountedKey.Compare(fragment.Start, spanStart) < 0) spanStart = fragment.Start;
            if (CountedKey.Compare(fragment.End, spanEnd) > 0) spanEnd = fragment.End;
            removeFrom = Math.Min(removeFrom, i);
            removeTo = i + 1;
        }

        var insertAt = removeFrom;
        if (removeTo > removeFrom) _memTombstones.RemoveRange(removeFrom, removeTo - removeFrom);
        else insertAt = LowerBoundMemTombstones(start);
        var coveredBelow = RankCore(spanEnd, false, 0) - RankCore(spanStart, false, 0);
        if (coveredBelow > 0) _memTombstones.Insert(insertAt, new(spanStart, spanEnd, coveredBelow));
        _memFrozen = null;
        return count;
    }

    /// Turns the memtable into the newest immutable component.
    public void Flush()
    {
        var mem = Memtable;
        if (mem == null) return;
        _immutables.Insert(0, mem);
        _memPoints.Clear();
        _memTombstones.Clear();
        _memFrozen = null;
    }

    /// Merges immutable components first..last (inclusive, first is the newest) into one component.
    public void Compact(int first, int last)
    {
        if (first < 0 || last >= _immutables.Count || first > last) throw new ArgumentOutOfRangeException(nameof(first));
        var run = _immutables.GetRange(first, last - first + 1);

        var spans = new List<(byte[] Start, byte[] End)>();
        var fragments = new List<CountedTombstone>();
        foreach (var component in run) fragments.AddRange(component.Tombstones);
        fragments.Sort((a, b) => CountedKey.Compare(a.Start, b.Start));
        foreach (var fragment in fragments)
        {
            if (spans.Count > 0 && CountedKey.Compare(fragment.Start, spans[^1].End) <= 0)
            {
                if (CountedKey.Compare(fragment.End, spans[^1].End) > 0) spans[^1] = (spans[^1].Start, fragment.End);
            }
            else spans.Add((fragment.Start, fragment.End));
        }

        var keys = new List<byte[]>();
        foreach (var component in run)
        foreach (var point in component.Points)
            keys.Add(point.Key);
        keys.Sort(CountedKey.Comparer);

        var points = new List<CountedPoint>();
        var spanIndex = 0;
        for (var k = 0; k < keys.Count; k++)
        {
            var key = keys[k];
            if (k > 0 && CountedKey.Compare(keys[k - 1], key) == 0) continue;
            while (spanIndex < spans.Count && CountedKey.Compare(spans[spanIndex].End, key) <= 0) spanIndex++;
            var covered = spanIndex < spans.Count && CountedKey.Compare(spans[spanIndex].Start, key) <= 0;

            // Newest decision wins: a point, or a fragment masking everything older.
            var isSet = false;
            foreach (var component in run)
            {
                if (component.TryGetPoint(key, null, out var point))
                {
                    isSet = point.IsSet;
                    break;
                }

                if (component.Covers(key)) break;
            }

            // Uncovered: the oldest point's ExistsBelow already describes the state below the run.
            var existsBelow = false;
            if (!covered)
            {
                for (var i = run.Count - 1; i >= 0; i--)
                {
                    if (!run[i].TryGetPoint(key, null, out var point)) continue;
                    existsBelow = point.ExistsBelow;
                    break;
                }
            }

            if (isSet || existsBelow) points.Add(new(key, isSet, existsBelow));
        }

        var tombstones = new List<CountedTombstone>();
        foreach (var (start, end) in spans)
        {
            var coveredBelow = RankCore(end, false, last + 1) - RankCore(start, false, last + 1);
            if (coveredBelow > 0) tombstones.Add(new(start, end, coveredBelow));
        }

        _immutables.RemoveRange(first, run.Count);
        var merged = new CountedComponent([.. points], [.. tombstones], _options.BlockSize, _options.RestartInterval);
        if (!merged.IsEmpty) _immutables.Insert(first, merged);
    }

    int MemPointIndex(byte[] key)
    {
        int lo = 0, hi = _memPoints.Count;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = CountedKey.Compare(_memPoints[mid].Key, key);
            if (cmp == 0) return mid;
            if (cmp < 0) lo = mid + 1;
            else hi = mid;
        }

        return ~lo;
    }

    int LowerBoundMemPoints(byte[] key)
    {
        var index = MemPointIndex(key);
        return index >= 0 ? index : ~index;
    }

    int LowerBoundMemTombstones(byte[] key)
    {
        int lo = 0, hi = _memTombstones.Count;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (CountedKey.Compare(_memTombstones[mid].Start, key) < 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }

    bool MemCovers(byte[] key)
    {
        foreach (var fragment in _memTombstones)
        {
            if (CountedKey.Compare(fragment.Start, key) <= 0 && CountedKey.Compare(key, fragment.End) < 0)
                return true;
        }

        return false;
    }
}
