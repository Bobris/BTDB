using System;
using System.Collections.Generic;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// One level L1..Ln: non-overlapping tables sorted by key, presented as a single component (ImplementationPlan.md,
/// section 13). Every table's tombstones lie inside its [Lower, Upper) span, so the level's tombstone set is the
/// concatenation of the tables' sets and prefix sums over tables give level-wide rank and block positions.
sealed class LevelComponent : ILsmComponent
{
    readonly TableReader[] _tables;
    readonly long[] _deltaBefore;
    readonly int[] _blocksBefore;

    public LevelComponent(TableReader[] tables)
    {
        _tables = tables;
        _deltaBefore = new long[tables.Length + 1];
        _blocksBefore = new int[tables.Length + 1];
        var tombstones = new List<LsmTombstone>();
        long entryCount = 0;
        for (var i = 0; i < tables.Length; i++)
        {
            _deltaBefore[i + 1] = _deltaBefore[i] + tables[i].DeltaSum;
            _blocksBefore[i + 1] = _blocksBefore[i] + tables[i].BlockCount;
            entryCount += tables[i].EntryCount;
            tombstones.AddRange(tables[i].Tombstones.Items);
            if (i > 0 && LsmKeys.Compare(tables[i - 1].Upper, tables[i].Lower) > 0)
                throw new InvalidOperationException("Tables of a level must not overlap.");
        }

        EntryCount = entryCount;
        Tombstones = new([.. tombstones]);
    }

    public IReadOnlyList<TableReader> Tables => _tables;
    public long EntryCount { get; }
    public long DeltaSum => _deltaBefore[^1];
    public TombstoneSet Tombstones { get; }
    public int BlockCount => _blocksBefore[^1];

    /// Index of the first table whose span ends after key; all tables before it lie entirely below key.
    internal int FirstEndingAfter(ReadOnlySpan<byte> key)
    {
        int lo = 0, hi = _tables.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (LsmKeys.Compare(_tables[mid].Upper, key) <= 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }

    /// Index of the last table whose span starts at or before key, or -1.
    internal int LastStartingAtOrBefore(ReadOnlySpan<byte> key)
    {
        int lo = 0, hi = _tables.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (LsmKeys.Compare(_tables[mid].Lower, key) <= 0) lo = mid + 1;
            else hi = mid;
        }

        return lo - 1;
    }

    public long PointDeltaBefore(ReadOnlySpan<byte> key)
    {
        var table = FirstEndingAfter(key);
        if (table == _tables.Length) return DeltaSum;
        var result = _deltaBefore[table];
        if (LsmKeys.Compare(_tables[table].Lower, key) < 0) result += _tables[table].PointDeltaBefore(key);
        return result;
    }

    public (long Min, long Max) PointDeltaBounds(ReadOnlySpan<byte> key)
    {
        var table = FirstEndingAfter(key);
        if (table == _tables.Length) return (DeltaSum, DeltaSum);
        var before = _deltaBefore[table];
        if (LsmKeys.Compare(_tables[table].Lower, key) >= 0) return (before, before);
        var (min, max) = _tables[table].PointDeltaBounds(key);
        return (before + min, before + max);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out LsmEntry entry)
    {
        var table = LastStartingAtOrBefore(key);
        if (table >= 0 && LsmKeys.Compare(key, _tables[table].Upper) < 0) return _tables[table].TryGet(key, out entry);
        entry = default;
        return false;
    }

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        for (var table = FirstEndingAfter(key); table < _tables.Length; table++)
        {
            if (_tables[table].SeekGE(key, inclusive, out entry)) return true;
        }

        entry = default;
        return false;
    }

    public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        for (var table = LastStartingAtOrBefore(key); table >= 0; table--)
        {
            if (_tables[table].SeekLE(key, inclusive, out entry)) return true;
        }

        entry = default;
        return false;
    }

    public bool SeekLast(out LsmEntry entry)
    {
        for (var table = _tables.Length - 1; table >= 0; table--)
        {
            if (_tables[table].SeekLast(out entry)) return true;
        }

        entry = default;
        return false;
    }

    public byte[] BlockFirstKey(int block)
    {
        int lo = 0, hi = _tables.Length - 1;
        while (lo < hi)
        {
            var mid = (lo + hi + 1) >>> 1;
            if (_blocksBefore[mid] <= block) lo = mid;
            else hi = mid - 1;
        }

        // Tables without points have no blocks, so the found table always owns the block.
        while (_blocksBefore[lo + 1] <= block) lo++;
        return _tables[lo].BlockFirstKey(block - _blocksBefore[lo]);
    }

    public void AddFinePivots(int fromBlock, int toBlock, List<byte[]> pivots)
    {
        for (var table = 0; table < _tables.Length; table++)
        {
            var first = Math.Max(fromBlock, _blocksBefore[table]);
            var end = Math.Min(toBlock, _blocksBefore[table + 1]);
            if (first < end)
                _tables[table].AddFinePivots(first - _blocksBefore[table], end - _blocksBefore[table], pivots);
        }
    }

    public int CountBlocksBefore(ReadOnlySpan<byte> key, bool inclusive)
    {
        var table = FirstEndingAfter(key);
        if (table == _tables.Length) return BlockCount;
        return _blocksBefore[table] + _tables[table].CountBlocksBefore(key, inclusive);
    }

    public IEnumerable<LsmEntry> Enumerate()
    {
        foreach (var table in _tables)
        foreach (var entry in table.Enumerate())
            yield return entry;
    }

    public IComponentCursor CreateCursor() => new LevelCursor(this);

    /// Positioned cursor over a level: the current table and a cursor inside it.
    sealed class LevelCursor(LevelComponent level) : IComponentCursor
    {
        readonly TableReader[] _tables = level._tables;
        int _table = -1;
        TableCursor? _cursor;

        public bool Valid => _cursor?.Valid ?? false;
        public LsmEntry Current => _cursor!.Current;

        TableCursor CursorFor(int table)
        {
            if (table != _table || _cursor == null)
            {
                _cursor = new(_tables[table]);
                _table = table;
            }

            return _cursor;
        }

        public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive)
        {
            for (var table = level.FirstEndingAfter(key); table < _tables.Length; table++)
                if (CursorFor(table).SeekGE(key, inclusive)) return true;
            return Invalid();
        }

        public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive)
        {
            for (var table = level.LastStartingAtOrBefore(key); table >= 0; table--)
                if (CursorFor(table).SeekLE(key, inclusive)) return true;
            return Invalid();
        }

        public bool SeekLast()
        {
            for (var table = _tables.Length - 1; table >= 0; table--)
                if (CursorFor(table).SeekLast()) return true;
            return Invalid();
        }

        public bool Next()
        {
            if (!Valid) return false;
            if (_cursor!.Next()) return true;
            for (var table = _table + 1; table < _tables.Length; table++)
                if (CursorFor(table).SeekGE([], true)) return true;
            return Invalid();
        }

        public bool Prev()
        {
            if (!Valid) return false;
            if (_cursor!.Prev()) return true;
            for (var table = _table - 1; table >= 0; table--)
                if (CursorFor(table).SeekLast()) return true;
            return Invalid();
        }

        bool Invalid()
        {
            _cursor = null;
            _table = -1;
            return false;
        }
    }
}
