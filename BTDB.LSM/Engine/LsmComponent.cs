using System;
using System.Collections.Generic;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// One point entry of a component. ExistsBelow says whether the key exists in the state below the component with the
/// component's own tombstones applied, so the rank delta is derived (ImplementationPlan.md, section 6.1).
readonly struct LsmEntry(
    byte[] key,
    bool isSet,
    bool existsBelow,
    ReadOnlyMemory<byte> value,
    bool isSeparated = false)
{
    public byte[] Key { get; } = key;
    public bool IsSet { get; } = isSet;
    public bool ExistsBelow { get; } = existsBelow;

    /// The value, or the encoded handle of a separated value when IsSeparated.
    public ReadOnlyMemory<byte> Value { get; } = value;

    public bool IsSeparated { get; } = isSeparated;
    public int Delta => (IsSet ? 1 : 0) - (ExistsBelow ? 1 : 0);
    public LsmValue LsmValue => new(Value, IsSeparated);
}

/// A value as stored: inline bytes, or the handle of a value in a value file (ImplementationPlan.md, section 10).
readonly record struct LsmValue(ReadOnlyMemory<byte> Data, bool IsSeparated);

/// Range tombstone fragment [Start, End) masking everything older than its component. CoveredBelow counts the keys in
/// the fragment that exist below the component (ImplementationPlan.md, section 6.3).
readonly record struct LsmTombstone(byte[] Start, byte[] End, long CoveredBelow);

/// Sorted, non-overlapping tombstone fragments with prefix sums of CoveredBelow.
sealed class TombstoneSet
{
    public static readonly TombstoneSet Empty = new([]);

    readonly LsmTombstone[] _items;
    readonly long[] _coveredBefore;

    public TombstoneSet(LsmTombstone[] items)
    {
        _items = items;
        _coveredBefore = new long[items.Length + 1];
        for (var i = 0; i < items.Length; i++) _coveredBefore[i + 1] = _coveredBefore[i] + items[i].CoveredBelow;
    }

    public int Count => _items.Length;
    public ReadOnlySpan<LsmTombstone> Items => _items;
    public LsmTombstone this[int index] => _items[index];
    public LsmTombstone this[Index index] => _items[index];
    public long CoveredSum => _coveredBefore[^1];

    /// Returns the index of the fragment containing key, or -1. coveredBefore sums CoveredBelow of all fragments that
    /// end at or before key.
    public int Find(ReadOnlySpan<byte> key, out long coveredBefore)
    {
        int lo = 0, hi = _items.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (LsmKeys.Compare(_items[mid].Start, key) <= 0) lo = mid + 1;
            else hi = mid;
        }

        var candidate = lo - 1;
        if (candidate >= 0 && LsmKeys.Compare(key, _items[candidate].End) < 0)
        {
            coveredBefore = _coveredBefore[candidate];
            return candidate;
        }

        coveredBefore = _coveredBefore[lo];
        return -1;
    }

    public bool Covers(ReadOnlySpan<byte> key) => Find(key, out _) >= 0;
}

/// A sorted run in the LSM stack: a memtable or a table. Components are ordered newest first; a newer component's
/// points override older ones and its tombstones mask everything older.
interface ILsmComponent
{
    long EntryCount { get; }
    long DeltaSum { get; }
    TombstoneSet Tombstones { get; }

    /// Sum of point deltas for keys lower than key (exact).
    long PointDeltaBefore(ReadOnlySpan<byte> key);

    /// Bounds of PointDeltaBefore(key) computed without reading data blocks.
    (long Min, long Max) PointDeltaBounds(ReadOnlySpan<byte> key);

    bool TryGet(ReadOnlySpan<byte> key, out LsmEntry entry);

    /// First entry with key greater than (or equal to, when inclusive) key.
    bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry);

    /// Last entry with key lower than (or equal to, when inclusive) key.
    bool SeekLE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry);

    bool SeekLast(out LsmEntry entry);

    /// Block structure used as select pivots.
    int BlockCount { get; }

    byte[] BlockFirstKey(int block);

    /// Number of blocks whose first key is lower than key (or lower or equal when inclusive).
    int CountBlocksBefore(ReadOnlySpan<byte> key, bool inclusive);

    /// True for components backed by storage, whose lookups may miss the cache.
    bool IsStored { get; }

    /// Starts an asynchronous read of the data block a seek to key would read first, unless it is cached or already
    /// being read, so a seek that misses in several components waits for their reads once (ImplementationPlan.md,
    /// section 12.2).
    void Prefetch(ReadOnlySpan<byte> key);

    /// Adds keys inside blocks [fromBlock, toBlock) at most one restart interval apart, used as select pivots
    /// finer than blocks.
    void AddFinePivots(int fromBlock, int toBlock, List<byte[]> pivots);

    /// All points in ascending key order.
    IEnumerable<LsmEntry> Enumerate();

    IComponentCursor CreateCursor();
}
