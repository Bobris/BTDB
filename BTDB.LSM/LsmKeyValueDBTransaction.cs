using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;

namespace BTDB.LSM;

sealed class LsmKeyValueDBTransaction : IKeyValueDBTransaction
{
    readonly LsmKeyValueDB _db;
    readonly LsmState _state;
    readonly bool _readOnly;
    bool _preapprovedWriting;
    bool _writing;
    bool _finished;
    bool _disposed;
    Memtable? _writable;
    LsmView _view;
    ulong _commitUlong;
    ulong[] _ulongs;
    long _transactionNumber;
    bool _persistOnCommit;
    string? _descriptionForLeaks;

    public LsmKeyValueDBTransaction(LsmKeyValueDB db, LsmState state, bool writing, bool readOnly)
    {
        _db = db;
        _state = state;
        _preapprovedWriting = writing;
        _readOnly = readOnly;
        _view = state.CreateView(state.Memtable);
        _commitUlong = state.CommitUlong;
        _ulongs = state.Ulongs;
        _transactionNumber = state.TransactionNumber;
    }

    internal long StateSequence => _state.Sequence;
    internal LsmView View => _view;

    /// Incremented by every modification; cursors use it to invalidate cached positions and indexes.
    internal long ModificationStamp { get; private set; }

    internal bool ModificationForbidden { get; set; }

    public IKeyValueDB Owner => _db;
    public DateTime CreatedTime { get; } = DateTime.UtcNow;

    public string? DescriptionForLeaks
    {
        get => _descriptionForLeaks;
        set => _descriptionForLeaks = value;
    }

    public bool RollbackAdvised { get; set; }
    public IKeyValueDBCursor? FirstCursor { get; set; }
    public IKeyValueDBCursor? LastCursor { get; set; }

    void ThrowIfFinished()
    {
        ObjectDisposedException.ThrowIf(_finished, this);
    }

    public IKeyValueDBCursor CreateCursor()
    {
        ThrowIfFinished();
        return new LsmKeyValueDBCursor(this);
    }

    public long GetKeyValueCount()
    {
        ThrowIfFinished();
        return _view.Count;
    }

    public bool IsWriting() => _writing || _preapprovedWriting;
    public bool IsReadOnly() => _readOnly;
    public bool IsDisposed() => _finished;

    void MakeWritable()
    {
        if (_writing) return;
        if (_readOnly) throw new BTDBTransactionRetryException("Cannot write from readOnly transaction");
        if (_preapprovedWriting) _preapprovedWriting = false;
        else _db.MakeWritable(this, _state);
        _writing = true;
        _writable = _state.Memtable.CloneForWriting(LsmKeyValueDB.NewOwner());
        _view = _state.CreateView(_writable);
        _transactionNumber++;
    }

    void BeginModification()
    {
        ThrowIfFinished();
        if (ModificationForbidden) throw new BTDBException("DB cannot be modified during fast iteration");
        MakeWritable();
        ModificationStamp++;
    }

    /// Creates or updates key; returns true when the key was created.
    internal bool Upsert(byte[] key, ReadOnlySpan<byte> value)
    {
        BeginModification();
        var memtable = _writable!;
        if (memtable.TryGetNode(key, out var isSet, out _))
        {
            memtable.UpdateExisting(key, true, value);
            return !isSet;
        }

        var existsBelow = !memtable.Tombstones.Covers(key) && _view.ExistsFrom(1, key);
        memtable.Insert(key, true, existsBelow, value);
        return !existsBelow;
    }

    internal void NotifyRenamed(byte[] oldKey, byte[] newKey, LsmKeyValueDBCursor origin)
    {
        var cursor = (LsmKeyValueDBCursor?)FirstCursor;
        while (cursor != null)
        {
            if (cursor != origin) cursor.NotifyRenamed(oldKey, newKey);
            cursor = (LsmKeyValueDBCursor?)cursor.NextCursor;
        }
    }

    /// Lets fuzzy invalid cursors adopt a key created exactly in their gap, as index-based cursors do.
    internal void NotifyCreated(byte[] key, LsmKeyValueDBCursor origin)
    {
        var cursor = (LsmKeyValueDBCursor?)FirstCursor;
        while (cursor != null)
        {
            if (cursor != origin) cursor.NotifyCreated(key);
            cursor = (LsmKeyValueDBCursor?)cursor.NextCursor;
        }
    }

    /// Erases an existing key.
    internal void Erase(byte[] key)
    {
        BeginModification();
        var memtable = _writable!;
        if (memtable.TryGetNode(key, out _, out var existsBelow))
        {
            if (existsBelow) memtable.UpdateExisting(key, false, []);
            else memtable.Remove(key);
            return;
        }

        // A live key without a memtable point cannot be covered by a memtable tombstone.
        memtable.Insert(key, false, true, []);
    }

    /// Erases all keys in [first, last]; returns the number of erased keys.
    internal long EraseRange(byte[] first, byte[] last)
    {
        BeginModification();
        var end = LsmKeys.Successor(last);
        var count = _view.Rank(end) - _view.Rank(first);
        if (count == 0) return 0;
        if (count <= _db.Options.PointDeleteRangeLimit)
        {
            var keys = new List<byte[]>();
            var found = _view.SeekGE(first, true, out var key, out _);
            while (found && LsmKeys.Compare(key, end) < 0)
            {
                keys.Add(key);
                found = _view.SeekGE(key, false, out key, out _);
            }

            foreach (var erased in keys) Erase(erased);
            return count;
        }

        var memtable = _writable!;
        memtable.RemoveRange(first, end);
        // Merge with overlapping or touching memtable fragments into one span.
        var spanStart = first;
        var spanEnd = end;
        var kept = new List<LsmTombstone>();
        foreach (var fragment in memtable.Tombstones.Items)
        {
            if (LsmKeys.Compare(fragment.End, first) < 0 || LsmKeys.Compare(fragment.Start, end) > 0)
            {
                kept.Add(fragment);
                continue;
            }

            if (LsmKeys.Compare(fragment.Start, spanStart) < 0) spanStart = fragment.Start;
            if (LsmKeys.Compare(fragment.End, spanEnd) > 0) spanEnd = fragment.End;
        }

        var coveredBelow = _view.RankFrom(1, spanEnd) - _view.RankFrom(1, spanStart);
        if (coveredBelow > 0) kept.Add(new(spanStart, spanEnd, coveredBelow));
        kept.Sort((a, b) => LsmKeys.Compare(a.Start, b.Start));
        memtable.SetTombstones(new([.. kept]));
        return count;
    }

    public ulong GetCommitUlong() => _commitUlong;

    public void SetCommitUlong(ulong value)
    {
        ThrowIfFinished();
        if (_commitUlong == value) return;
        MakeWritable();
        _commitUlong = value;
    }

    public uint GetUlongCount() => (uint)_ulongs.Length;

    public ulong GetUlong(uint idx) => idx < _ulongs.Length ? _ulongs[idx] : 0;

    public void SetUlong(uint idx, ulong value)
    {
        ThrowIfFinished();
        if (GetUlong(idx) == value) return;
        MakeWritable();
        var ulongs = new ulong[Math.Max(_ulongs.Length, idx + 1)];
        _ulongs.CopyTo(ulongs, 0);
        ulongs[idx] = value;
        _ulongs = ulongs;
    }

    /// Persists the next commit's state before Commit returns. Without a WAL (phase L1) this flushes the memtable.
    public void NextCommitTemporaryCloseTransactionLog()
    {
        _persistOnCommit = true;
    }

    public void Commit()
    {
        if (_finished) throw new BTDBException("Transaction already committed or disposed");
        _finished = true;
        if (_writing)
        {
            _writing = false;
            _db.CommitWritingTransaction(_writable!, _commitUlong, _ulongs, _transactionNumber, _persistOnCommit);
        }
        else if (_preapprovedWriting)
        {
            _preapprovedWriting = false;
            _db.RevertWritingTransaction();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _finished = true;
        if (_writing || _preapprovedWriting)
        {
            _writing = false;
            _preapprovedWriting = false;
            _db.RevertWritingTransaction();
        }

        _db.TransactionDisposed(this);
    }

    public long GetTransactionNumber() => _transactionNumber;

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats() =>
        new() { [(0, (uint)_state.Tables.Length)] = 1 };
}
