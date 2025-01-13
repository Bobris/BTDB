using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BTDB.BTreeLib;
using BTDB.Collections;

namespace BTDB.KVDBLayer;

public class BTreeKeyValueDBTransaction : IKeyValueDBTransaction
{
    internal readonly BTreeKeyValueDB KeyValueDB;
    public IRootNode? BTreeRoot;
    readonly bool _readOnly;
    bool _writing;
    bool _preapprovedWriting;
    bool _temporaryCloseTransactionLog;

    public DateTime CreatedTime { get; } = DateTime.UtcNow;

    public BTreeKeyValueDBTransaction(BTreeKeyValueDB keyValueDB, IRootNode root, bool writing, bool readOnly)
    {
        _preapprovedWriting = writing;
        _readOnly = readOnly;
        KeyValueDB = keyValueDB;
        BTreeRoot = root;
    }

    ~BTreeKeyValueDBTransaction()
    {
        if (BTreeRoot == null && !_writing && !_preapprovedWriting) return;
        Dispose();
        KeyValueDB.Logger?.ReportTransactionLeak(this);
    }

    public IKeyValueDBCursor CreateCursor()
    {
        ObjectDisposedException.ThrowIf(BTreeRoot == null, this);
        return BTreeKeyValueDBCursor.Create(this);
    }

    internal void MakeWritable()
    {
        if (_writing) return;
        if (_preapprovedWriting)
        {
            _writing = true;
            _preapprovedWriting = false;
            KeyValueDB.WriteStartTransaction();
            return;
        }

        if (_readOnly)
        {
            throw new BTDBTransactionRetryException("Cannot write from readOnly transaction");
        }

        BTreeRoot = KeyValueDB.MakeWritableTransaction(this, BTreeRoot!);

        BTreeRoot.DescriptionForLeaks = _descriptionForLeaks;
        _writing = true;
        KeyValueDB.WriteStartTransaction();

        var cursor = (IKeyValueDBCursorInternal)FirstCursor;
        while (cursor != null)
        {
            cursor.NotifyWritableTransaction();
            cursor = cursor.NextCursor;
        }
    }

    public long GetKeyValueCount()
    {
        ObjectDisposedException.ThrowIf(BTreeRoot == null, this);
        return BTreeRoot.GetCount();
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
        return BTreeRoot == null;
    }

    public ulong GetCommitUlong()
    {
        return BTreeRoot!.CommitUlong;
    }

    public void SetCommitUlong(ulong value)
    {
        if (BTreeRoot!.CommitUlong != value)
        {
            MakeWritable();
            BTreeRoot!.CommitUlong = value;
        }
    }

    public void NextCommitTemporaryCloseTransactionLog()
    {
        MakeWritable();
        _temporaryCloseTransactionLog = true;
    }

    internal void CommitFromCompactor()
    {
        if (BTreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
        var currentRoot = BTreeRoot;
        BTreeRoot = null;
        _preapprovedWriting = false;
        KeyValueDB.CommitFromCompactor(currentRoot);
    }

    public void Commit()
    {
        if (BTreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
        var currentRoot = BTreeRoot;
        BTreeRoot = null;
        if (_preapprovedWriting)
        {
            _preapprovedWriting = false;
            KeyValueDB.RevertWritingTransaction(currentRoot!, true);
        }
        else if (_writing)
        {
            KeyValueDB.CommitWritingTransaction(currentRoot!, _temporaryCloseTransactionLog);
            _writing = false;
        }
        else
        {
            KeyValueDB.DereferenceRoot(currentRoot!);
        }
    }

    public void Dispose()
    {
        if (FirstCursor != null)
        {
            throw new BTDBException("Forgot to dispose cursor for transaction " + _descriptionForLeaks);
        }

        var currentRoot = Interlocked.Exchange(ref BTreeRoot, null);
        if (_writing || _preapprovedWriting)
        {
            KeyValueDB.RevertWritingTransaction(currentRoot!, _preapprovedWriting);
            _writing = false;
            _preapprovedWriting = false;
        }
        else if (currentRoot != null)
        {
            KeyValueDB.DereferenceRoot(currentRoot);
        }

        KeyValueDB.TransactionDisposed(this);
        GC.SuppressFinalize(this);
    }

    public long GetTransactionNumber()
    {
        return BTreeRoot!.TransactionId;
    }

    public ulong GetUlong(uint idx)
    {
        return BTreeRoot!.GetUlong(idx);
    }

    public void SetUlong(uint idx, ulong value)
    {
        ObjectDisposedException.ThrowIf(BTreeRoot == null, this);
        if (BTreeRoot!.GetUlong(idx) != value)
        {
            MakeWritable();
            BTreeRoot!.SetUlong(idx, value);
        }
    }

    public uint GetUlongCount()
    {
        return BTreeRoot!.GetUlongCount();
    }

    string? _descriptionForLeaks;

    public string? DescriptionForLeaks
    {
        get => _descriptionForLeaks;
        set
        {
            _descriptionForLeaks = value;
            if (_preapprovedWriting || _writing) BTreeRoot!.DescriptionForLeaks = value;
        }
    }

    public IKeyValueDB Owner => KeyValueDB;

    public bool RollbackAdvised { get; set; }

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats()
    {
        ObjectDisposedException.ThrowIf(BTreeRoot == null, this);
        var stats = new RefDictionary<(uint Depth, uint Children), uint>();
        BTreeRoot.CalcBTreeStats(stats, 0);
        return stats.ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    public IKeyValueDBCursor? FirstCursor { get; set; }
    public IKeyValueDBCursor? LastCursor { get; set; }
}
