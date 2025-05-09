using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Collections;
using BTDB.KVDBLayer.BTreeMem;

namespace BTDB.KVDBLayer;

class InMemoryKeyValueDBTransaction : IKeyValueDBTransaction
{
    readonly InMemoryKeyValueDB _keyValueDB;
    internal IBTreeRootNode? _btreeRoot;
    bool _writing;
    readonly bool _readOnly;
    bool _preapprovedWriting;
    internal IKeyValueDBCursor? Reused1;
    internal IKeyValueDBCursor? Reused2;

    public InMemoryKeyValueDBTransaction(InMemoryKeyValueDB keyValueDB, IBTreeRootNode btreeRoot, bool writing,
        bool readOnly)
    {
        _preapprovedWriting = writing;
        _readOnly = readOnly;
        _keyValueDB = keyValueDB;
        _btreeRoot = btreeRoot;
    }

    public IKeyValueDBCursor CreateCursor()
    {
        ObjectDisposedException.ThrowIf(_btreeRoot == null, this);
        if (Reused1 != null)
        {
            var cursor = Reused1;
            Reused1 = null;
            ((InMemoryKeyValueDBCursor)cursor).Disposed = false;
            return cursor;
        }

        if (Reused2 != null)
        {
            var cursor = Reused2;
            Reused2 = null;
            ((InMemoryKeyValueDBCursor)cursor).Disposed = false;
            return cursor;
        }

        return InMemoryKeyValueDBCursor.Create(this, _writing || _preapprovedWriting);
    }

    internal void MakeWritable()
    {
        if (_writing) return;
        if (_preapprovedWriting)
        {
            _writing = true;
            _preapprovedWriting = false;
            return;
        }

        if (_readOnly)
        {
            throw new BTDBTransactionRetryException("Cannot write from readOnly transaction");
        }

        var oldBTreeRoot = _btreeRoot;
        _btreeRoot = _keyValueDB.MakeWritableTransaction(this, oldBTreeRoot!);
        _btreeRoot.DescriptionForLeaks = _descriptionForLeaks;
        _writing = true;
        var cursor = (IKeyValueDBCursorInternal)FirstCursor;
        while (cursor != null)
        {
            cursor.NotifyWritableTransaction();
            cursor = cursor.NextCursor;
        }
    }

    public long GetKeyValueCount()
    {
        ObjectDisposedException.ThrowIf(_btreeRoot == null, this);
        return _btreeRoot.CalcKeyCount();
    }

    public bool IsWriting()
    {
        return _writing || _preapprovedWriting;
    }

    public bool IsReadOnly()
    {
        return _readOnly;
    }

    public bool IsDisposed()
    {
        return _btreeRoot == null;
    }

    public ulong GetCommitUlong()
    {
        return _btreeRoot!.CommitUlong;
    }

    public void SetCommitUlong(ulong value)
    {
        ObjectDisposedException.ThrowIf(_btreeRoot == null, this);
        if (_btreeRoot.CommitUlong != value)
        {
            MakeWritable();
            _btreeRoot.CommitUlong = value;
        }
    }

    public void NextCommitTemporaryCloseTransactionLog()
    {
        // There is no transaction log ...
    }

    public void Commit()
    {
        if (_btreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
        var currentBtreeRoot = _btreeRoot;
        _btreeRoot = null;
        if (_preapprovedWriting)
        {
            _preapprovedWriting = false;
            _keyValueDB.RevertWritingTransaction();
        }
        else if (_writing)
        {
            _keyValueDB.CommitWritingTransaction(currentBtreeRoot!);
            _writing = false;
        }
    }

    public void Dispose()
    {
        if (Reused1 != null)
        {
            ((InMemoryKeyValueDBCursor)Reused1).RealDispose(this);
        }

        if (Reused2 != null)
        {
            ((InMemoryKeyValueDBCursor)Reused2).RealDispose(this);
        }

        if (_writing || _preapprovedWriting)
        {
            _keyValueDB.RevertWritingTransaction();
            _writing = false;
            _preapprovedWriting = false;
        }

        _btreeRoot = null;
        _keyValueDB.TransactionDisposed(this);
    }

    public long GetTransactionNumber()
    {
        return _btreeRoot!.TransactionId;
    }

    public ulong GetUlong(uint idx)
    {
        return _btreeRoot!.GetUlong(idx);
    }

    public void SetUlong(uint idx, ulong value)
    {
        ObjectDisposedException.ThrowIf(_btreeRoot == null, this);
        if (_btreeRoot.GetUlong(idx) != value)
        {
            MakeWritable();
            _btreeRoot.SetUlong(idx, value);
        }
    }

    public uint GetUlongCount()
    {
        return _btreeRoot!.GetUlongCount();
    }

    string? _descriptionForLeaks;

    public DateTime CreatedTime { get; } = DateTime.UtcNow;

    public string? DescriptionForLeaks
    {
        get => _descriptionForLeaks;
        set
        {
            _descriptionForLeaks = value;
            if (_preapprovedWriting || _writing) _btreeRoot!.DescriptionForLeaks = value;
        }
    }

    public IKeyValueDB Owner => _keyValueDB;

    public bool RollbackAdvised { get; set; }

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats()
    {
        var stats = new RefDictionary<(uint Depth, uint Children), uint>();
        _btreeRoot!.CalcBTreeStats(stats, 0);
        return stats.ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    public IKeyValueDBCursor? FirstCursor { get; set; }

    public IKeyValueDBCursor? LastCursor { get; set; }
}
