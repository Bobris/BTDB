using System;
using System.Collections.Generic;

namespace BTDB.LSM.Test.Counting;

/// One point entry. ExistsBelow says whether the key exists in the state below this entry's component with the
/// component's own range tombstones applied, so the rank delta is derived and never stored (ImplementationPlan.md, 6.1).
readonly record struct CountedPoint(byte[] Key, bool IsSet, bool ExistsBelow)
{
    public int Delta => (IsSet ? 1 : 0) - (ExistsBelow ? 1 : 0);
}

/// Range tombstone fragment [Start, End) masking everything below its component. CoveredBelow is the number of keys
/// in the fragment that exist in the state below the component (ImplementationPlan.md, 6.3).
readonly record struct CountedTombstone(byte[] Start, byte[] End, long CoveredBelow);

/// Cost counters approximating storage access of the table layout: an index read is a lookup in a pinned or cached
/// index, a block read is a data block access that may miss the cache.
sealed class CountedStats
{
    public long IndexReads;
    public long BlockReads;
    public long RankCalls;
    public long SelectCalls;
    public long SelectPivots;
    public long SelectExactPivots;
    public long SelectSteps;

    public CountedStats Clone() => (CountedStats)MemberwiseClone();
}

static class CountedKey
{
    public static int Compare(byte[] a, byte[] b) => a.AsSpan().SequenceCompareTo(b);

    public static readonly IComparer<byte[]> Comparer = Comparer<byte[]>.Create(Compare);
}

/// Immutable sorted run with the prefix sums a table stores: the cumulative delta before every restart point (and so
/// before every block, as kept in the index) and the cumulative CoveredBelow before every tombstone fragment.
sealed class CountedComponent
{
    readonly CountedPoint[] _points;
    readonly int _blockSize;
    readonly int _restartInterval;
    readonly long[] _restartDeltaBefore;
    readonly int[] _blockMinPrefix;
    readonly int[] _blockMaxPrefix;
    readonly CountedTombstone[] _tombstones;
    readonly long[] _coveredBefore;

    public CountedComponent(CountedPoint[] points, CountedTombstone[] tombstones, int blockSize, int restartInterval)
    {
        if (blockSize <= 0 || restartInterval <= 0 || blockSize % restartInterval != 0)
            throw new ArgumentException("Block size must be a positive multiple of the restart interval.");
        _points = points;
        _tombstones = tombstones;
        _blockSize = blockSize;
        _restartInterval = restartInterval;
        _restartDeltaBefore = new long[(points.Length + restartInterval - 1) / restartInterval];
        long sum = 0;
        for (var i = 0; i < points.Length; i++)
        {
            if (i % restartInterval == 0) _restartDeltaBefore[i / restartInterval] = sum;
            sum += points[i].Delta;
        }

        DeltaSum = sum;
        // Index entries also keep the extremes of the running delta inside each block, so a rank inside a block can be
        // bounded without reading it.
        _blockMinPrefix = new int[BlockCount];
        _blockMaxPrefix = new int[BlockCount];
        for (var block = 0; block < BlockCount; block++)
        {
            int running = 0, min = 0, max = 0;
            for (var i = block * blockSize; i < Math.Min(points.Length, (block + 1) * blockSize); i++)
            {
                running += points[i].Delta;
                min = Math.Min(min, running);
                max = Math.Max(max, running);
            }

            _blockMinPrefix[block] = min;
            _blockMaxPrefix[block] = max;
        }

        _coveredBefore = new long[tombstones.Length + 1];
        for (var i = 0; i < tombstones.Length; i++)
            _coveredBefore[i + 1] = _coveredBefore[i] + tombstones[i].CoveredBelow;
        CoveredSum = _coveredBefore[^1];
    }

    public long DeltaSum { get; }
    public long CoveredSum { get; }
    public ReadOnlySpan<CountedPoint> Points => _points;
    public ReadOnlySpan<CountedTombstone> Tombstones => _tombstones;
    public int BlockCount => (_points.Length + _blockSize - 1) / _blockSize;
    public bool IsEmpty => _points.Length == 0 && _tombstones.Length == 0;

    public byte[] BlockFirstKey(int block) => _points[block * _blockSize].Key;

    /// Number of blocks whose first key is lower than key (strict) or lower or equal (inclusive).
    public int CountBlocksBefore(byte[] key, bool inclusive)
    {
        int lo = 0, hi = BlockCount;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = CountedKey.Compare(BlockFirstKey(mid), key);
            if (cmp < 0 || inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }

    /// Sum of point deltas for keys lower than key, using the index and at most one data block.
    public long PointDeltaBefore(byte[] key, CountedStats? stats)
    {
        if (stats != null) stats.IndexReads++;
        var blocks = CountBlocksBefore(key, false);
        if (blocks == 0) return 0;
        if (stats != null) stats.BlockReads++;
        var block = blocks - 1;
        var firstRestart = block * (_blockSize / _restartInterval);
        var endRestart = Math.Min(firstRestart + _blockSize / _restartInterval, _restartDeltaBefore.Length);
        // The block's first key is lower than key, so the first restart qualifies.
        int lo = firstRestart + 1, hi = endRestart;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (CountedKey.Compare(_points[mid * _restartInterval].Key, key) < 0) lo = mid + 1;
            else hi = mid;
        }

        var restart = lo - 1;
        var sum = _restartDeltaBefore[restart];
        for (var i = restart * _restartInterval; i < _points.Length && CountedKey.Compare(_points[i].Key, key) < 0; i++)
            sum += _points[i].Delta;
        return sum;
    }

    public int BlockOf(int pointIndex) => pointIndex / _blockSize;

    /// Bounds of PointDeltaBefore(key) from index data only: exact when key is a block's first key, otherwise the
    /// block's starting sum plus the block's running-delta extremes.
    public (long Min, long Max) PointDeltaBounds(byte[] key, CountedStats? stats)
    {
        if (stats != null) stats.IndexReads++;
        var blocks = CountBlocksBefore(key, true);
        if (blocks == 0) return (0, 0);
        var block = blocks - 1;
        var before = _restartDeltaBefore[block * (_blockSize / _restartInterval)];
        if (CountedKey.Compare(BlockFirstKey(block), key) == 0) return (before, before);
        return (before + _blockMinPrefix[block], before + _blockMaxPrefix[block]);
    }

    /// Index of the first point whose key is greater than or equal to key.
    public int LowerBound(byte[] key)
    {
        int lo = 0, hi = _points.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (CountedKey.Compare(_points[mid].Key, key) < 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }

    public bool TryGetPoint(byte[] key, CountedStats? stats, out CountedPoint point)
    {
        if (stats != null)
        {
            stats.IndexReads++;
            stats.BlockReads++;
        }

        var index = LowerBound(key);
        if (index < _points.Length && CountedKey.Compare(_points[index].Key, key) == 0)
        {
            point = _points[index];
            return true;
        }

        point = default;
        return false;
    }

    /// Returns the index of the fragment containing key, or -1. coveredBefore is the CoveredBelow sum of all
    /// fragments that end at or before key.
    public int FindTombstone(byte[] key, out long coveredBefore)
    {
        int lo = 0, hi = _tombstones.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (CountedKey.Compare(_tombstones[mid].Start, key) <= 0) lo = mid + 1;
            else hi = mid;
        }

        var candidate = lo - 1;
        if (candidate >= 0 && CountedKey.Compare(key, _tombstones[candidate].End) < 0)
        {
            coveredBefore = _coveredBefore[candidate];
            return candidate;
        }

        coveredBefore = _coveredBefore[lo];
        return -1;
    }

    public bool Covers(byte[] key) => FindTombstone(key, out _) >= 0;
}
