using System;
using System.Collections.Generic;
using System.Linq;
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
    IMemtable? _writable;
    long _writableBaseBytes;
    WalRecordBuilder? _record;

    // After the first spill: private runs holding earlier changes of this transaction (newest first), the value
    // files they reference, and the memtable bytes moved into them.
    List<SpilledRun>? _spilledRuns;
    List<(ulong FileNumber, long Bytes)>? _spilledValueFiles;
    long _spilledBytes;
    int _spilledSegments;
    bool _spillFailed;
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
    internal int SpilledRunCount => _spilledRuns?.Count ?? 0;
    internal int SpilledSegmentCount => _spilledSegments;
    internal long ManifestSequence => _state.ManifestSequence;

    /// Epoch of the writer session this transaction writes under.
    internal long WriterEpoch { get; set; }
    internal LsmKeyValueDB Db => _db;
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
        _writableBaseBytes = _writable.ApproximateBytes;
        _record = new();
        _view = _state.CreateView(_writable);
        _transactionNumber++;
    }

    void BeginModification()
    {
        ThrowIfFinished();
        if (ModificationForbidden) throw new BTDBException("DB cannot be modified during fast iteration");
        MakeWritable();
        LimitMemory();
        ModificationStamp++;
    }

    long AddedBytes => _writable!.ApproximateBytes - _writableBaseBytes;

    /// Checked before every modification: a transaction holding more than MaxTransactionMemory memtable bytes moves
    /// them into a private table, and one that added more than MaxTransactionSize in total can only be rolled back.
    void LimitMemory()
    {
        ThrowIfTooLarge();
        if (AddedBytes > _db.Options.MaxTransactionMemory) Spill();
    }

    /// Checked before every modification and at commit, so one modification can exceed the limit but never a commit.
    void ThrowIfTooLarge()
    {
        var added = _spilledBytes + AddedBytes;
        var limit = _db.Options.MaxTransactionSize;
        if (added > limit)
            throw new BTDBException(
                $"Transaction added {added} memtable bytes, more than MaxTransactionSize {limit}; roll it back.");
    }

    /// Moves the changes held in memory into a private table that only this transaction reads, so its memory stays
    /// bounded (ImplementationPlan.md, section 7.5). The first spill also separates the changes from the shared
    /// memtable: they are applied again, by their logical effect, to an own memtable stacked on the snapshot, and the
    /// shared memtable is sealed so background work flushes it while the transaction runs. The WAL record is dropped:
    /// such a transaction commits by one manifest edit.
    void Spill()
    {
        if (_spillFailed) throw new BTDBException("The transaction could not spill its changes; roll it back.");
        // A failure part way leaves the changes split between memory and tables; only a rollback is safe then.
        _spillFailed = true;
        if (_spilledRuns == null)
        {
            _db.SealForSpill();
            var log = _record!;
            _record = null;
            _spilledRuns = [];
            _spilledValueFiles = [];
            StartSegment();
            log.ReplayOperations((key, value) => UpsertCore(key, value), EraseCore,
                (start, end) => EraseRangeCore(start, end));
        }

        var segment = _writable!;
        _spilledBytes += AddedBytes;
        if (_db.WriteSpilledSegment(segment) is { } written)
        {
            _spilledRuns.Insert(0, new([written.Table], 1));
            _spilledValueFiles!.AddRange(written.ValueFiles);
            _spilledSegments++;
        }

        // Few runs keep the existence checks of later writes cheap.
        _db.MergeSpilled(_spilledRuns, _state, _spilledValueFiles!, false);

        StartSegment();
        // Only this transaction's view reached the segment, and cursors rebuild their iterators on a new view.
        segment.Arena?.Dispose();
        ModificationStamp++;
        _spillFailed = false;
    }

    void StartSegment()
    {
        _writable = _db.NewSegmentMemtable();
        _writableBaseBytes = _writable.ApproximateBytes;
        _view = _state.CreateView([_writable, .. _spilledRuns!.Select(r => r.Component)]);
    }

    /// Creates or updates key; returns true when the key was created.
    internal bool Upsert(byte[] key, ReadOnlySpan<byte> value)
    {
        BeginModification();
        return UpsertCore(key, value);
    }

    bool UpsertCore(byte[] key, ReadOnlySpan<byte> value)
    {
        var memtable = _writable!;
        if (memtable.TryGetNode(key, out var isSet, out _))
        {
            memtable.UpdateExisting(key, true, value);
            _record?.LogUpdate(key, true, value);
            return !isSet;
        }

        var existsBelow = !memtable.Tombstones.Covers(key) && _view.ExistsFrom(1, key);
        memtable.Insert(key, true, existsBelow, value);
        _record?.LogInsert(key, true, existsBelow, value);
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

    /// Erases an existing key.
    internal void Erase(byte[] key)
    {
        BeginModification();
        EraseCore(key);
    }

    void EraseCore(byte[] key)
    {
        var memtable = _writable!;
        if (memtable.TryGetNode(key, out _, out var existsBelow))
        {
            if (existsBelow)
            {
                memtable.UpdateExisting(key, false, []);
                _record?.LogUpdate(key, false, []);
            }
            else
            {
                memtable.Remove(key);
                _record?.LogRemove(key);
            }
            return;
        }

        // A live key without a memtable point cannot be covered by a memtable tombstone.
        memtable.Insert(key, false, true, []);
        _record?.LogInsert(key, false, true, []);
    }

    /// Erases all keys in [first, last]; returns the number of erased keys.
    internal long EraseRange(byte[] first, byte[] last)
    {
        BeginModification();
        return EraseRangeCore(first, LsmKeys.Successor(last));
    }

    /// Erases all keys in [first, end).
    long EraseRangeCore(byte[] first, byte[] end)
    {
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

            foreach (var erased in keys) EraseCore(erased);
            return count;
        }

        var memtable = _writable!;
        memtable.RemoveRange(first, end);
        _record?.LogRemoveRange(first, end);
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
        var tombstones = new TombstoneSet([.. kept]);
        memtable.SetTombstones(tombstones);
        _record?.LogTombstones(tombstones);
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
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(idx, (uint)Array.MaxLength);
        if (GetUlong(idx) == value) return;
        MakeWritable();
        var ulongs = new ulong[Math.Max(_ulongs.Length, idx + 1)];
        _ulongs.CopyTo(ulongs, 0);
        ulongs[idx] = value;
        _ulongs = ulongs;
    }

    /// Makes the next commit durable before Commit returns, like DurableTransactions for one commit.
    public void NextCommitTemporaryCloseTransactionLog()
    {
        _persistOnCommit = true;
    }

    public void Commit()
    {
        if (_finished) throw new BTDBException("Transaction already committed or disposed");
        if (_spillFailed) throw new BTDBException("The transaction could not spill its changes; roll it back.");
        if (_writing) ThrowIfTooLarge();
        // One modification may have gone above MaxTransactionMemory; such a commit spills too.
        if (_writing && _spilledRuns == null && AddedBytes > _db.Options.MaxTransactionMemory) Spill();
        _finished = true;
        if (_writing)
        {
            _writing = false;
            var metadata = new WalCommitMetadata(_transactionNumber, _commitUlong, _ulongs);
            if (_spilledRuns != null)
            {
                var runs = _spilledRuns;
                _spilledRuns = null;
                _db.CommitSpilledTransaction(this, _writable!, runs, _state, _spilledValueFiles!, metadata);
            }
            else
            {
                _db.CommitWritingTransaction(this, _writable!, _record!.Finish(metadata), metadata, _persistOnCommit);
            }
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
            if (_spilledRuns != null)
            {
                _db.DiscardSpilled(_spilledRuns.SelectMany(r => r.Tables), _spilledValueFiles!);
                _spilledRuns = null;
                // The segment memtable has its own arena that no published state uses.
                _writable!.Arena?.Dispose();
            }

            _db.RevertWritingTransaction();
        }

        _db.TransactionDisposed(this);
    }

    public long GetTransactionNumber() => _transactionNumber;

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats() =>
        new() { [(0, (uint)_state.Version.Components.Length)] = 1 };
}
