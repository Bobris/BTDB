using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.BTreeLib;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class BTreeKeyValueDBCursor : IKeyValueDBCursorInternal
{
    BTreeKeyValueDBTransaction _transaction;
    ICursor _cursor;
    bool _modifiedFromLastFind;
    long _keyIndex;

    public BTreeKeyValueDBCursor(BTreeKeyValueDBTransaction transaction)
    {
        _transaction = transaction;
        _cursor = transaction.BTreeRoot!.CreateCursor();
        _modifiedFromLastFind = false;
        _keyIndex = -1;
        if (transaction.FirstCursor == null)
        {
            transaction.FirstCursor = this;
            transaction.LastCursor = this;
        }
        else
        {
            ((IKeyValueDBCursorInternal)transaction.LastCursor!).NextCursor = this;
            PrevCursor = (IKeyValueDBCursorInternal)transaction.LastCursor;
            transaction.LastCursor = this;
        }
    }

    public void Dispose()
    {
        var nextCursor = NextCursor;
        var prevCursor = PrevCursor;
        if (nextCursor == null)
        {
            if (prevCursor == null)
            {
                _transaction.FirstCursor = null!;
                _transaction.LastCursor = null!;
            }
            else
            {
                prevCursor.NextCursor = null;
                _transaction.LastCursor = prevCursor;
            }
        }
        else
        {
            nextCursor.PrevCursor = prevCursor;
            if (prevCursor == null)
            {
                _transaction.FirstCursor = nextCursor;
            }
            else
            {
                prevCursor.NextCursor = nextCursor;
            }
        }

        _cursor = null!;
        _transaction = null!;
    }

    public IKeyValueDBTransaction Transaction => _transaction;

    public bool ModifiedFromLastFind => _modifiedFromLastFind;

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        _keyIndex = -1;
        return _cursor.FindFirst(prefix);
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        _keyIndex = _cursor.FindLastWithPrefix(prefix);
        return _keyIndex >= 0;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        if (!_cursor.IsValid()) return FindLastKey(prefix);
        if (_cursor.MovePrevious())
        {
            if (_cursor.KeyHasPrefix(prefix))
            {
                if (_keyIndex != -1) _keyIndex--;
                return true;
            }
        }

        _keyIndex = -1;
        _cursor.Invalidate();
        return false;
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        if (!_cursor.IsValid()) return FindFirstKey(prefix);
        if (_cursor.MoveNext())
        {
            if (_cursor.KeyHasPrefix(prefix))
            {
                if (_keyIndex != -1) _keyIndex++;
                return true;
            }
        }

        _keyIndex = -1;
        _cursor.Invalidate();
        return false;
    }

    public FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        var result = _cursor.Find(key);
        _keyIndex = -1;
        if (prefixLen == 0) return result;
        switch (result)
        {
            case FindResult.Previous when !_cursor.KeyHasPrefix(key[..(int)prefixLen]):
            {
                if (!_cursor.MoveNext())
                {
                    return FindResult.NotFound;
                }

                if (_cursor.KeyHasPrefix(key[..(int)prefixLen]))
                {
                    return FindResult.Next;
                }

                _cursor.Invalidate();
                return FindResult.NotFound;
            }
            case FindResult.Next when !_cursor.KeyHasPrefix(key[..(int)prefixLen]):
                // FindResult.Previous is preferred that's why it has to be NotFound when next does not match prefix
                _cursor.Invalidate();
                return FindResult.NotFound;
            default:
                return result;
        }
    }

    public long GetKeyIndex()
    {
        if (_keyIndex == -1) _keyIndex = _cursor.CalcIndex();
        return _keyIndex;
    }

    public bool FindKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        if (!_cursor.FindFirst(prefix))
        {
            _keyIndex = -1;
            return false;
        }

        index += _cursor.CalcIndex();
        if (_cursor.SeekIndex(index))
        {
            _keyIndex = index;
            if (_cursor.KeyHasPrefix(prefix))
                return true;
        }

        _keyIndex = -1;
        _cursor.Invalidate();
        return false;
    }

    public bool FindKeyIndex(long index)
    {
        _keyIndex = -1;
        if (_cursor.SeekIndex(index))
        {
            _keyIndex = index;
            return true;
        }

        _cursor.Invalidate();
        return false;
    }

    public void Invalidate()
    {
        _keyIndex = -1;
        _cursor.Invalidate();
    }

    public bool IsValid()
    {
        return _cursor.IsValid();
    }

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        if (!IsValid()) return new();
        var keyLen = _cursor.GetKeyLength();
        var trueValue = _cursor.GetValue();
        return new(
            (uint)keyLen,
            _transaction._keyValueDB.CalcValueSize(MemoryMarshal.Read<uint>(trueValue),
                MemoryMarshal.Read<uint>(trueValue[4..]),
                MemoryMarshal.Read<int>(trueValue[8..])));
    }

    public ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy = false)
    {
        return _cursor.GetKeyMemory(ref buffer, copy);
    }

    public ReadOnlySpan<byte> GetKeySpan(ref Memory<byte> buffer, bool copy = false)
    {
        return _cursor.GetKeySpan(ref buffer, copy);
    }

    public bool IsValueCorrupted()
    {
        if (!IsValid()) return false;
        var trueValue = _cursor.GetValue();
        return _transaction._keyValueDB.IsCorruptedValue(trueValue);
    }

    public ReadOnlyMemory<byte> GetValueMemory(ref Memory<byte> buffer, bool copy = false)
    {
        if (!IsValid()) return new();
        var trueValue = _cursor.GetValue();
        var keyValueDB = _transaction._keyValueDB;
        try
        {
            return keyValueDB.ReadValueMemory(trueValue, ref buffer, copy);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IRootNode)keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IRootNode)keyValueDB.ReferenceAndGetLastCommitted();
            try
            {
                var treeRoot = _transaction.BTreeRoot!;
                throw new BTDBException(
                    $"{ex.Message} in TrId:{treeRoot.TransactionId},TRL:{treeRoot.TrLogFileId},Ofs:{treeRoot.TrLogOffset},ComUlong:{treeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong}",
                    ex);
            }
            finally
            {
                keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                keyValueDB.DereferenceRootNodeInternal(lastCommitted);
            }
        }
    }

    public ReadOnlySpan<byte> GetValueSpan(ref Memory<byte> buffer, bool copy = false)
    {
        if (!IsValid()) return new();
        var trueValue = _cursor.GetValue();
        var keyValueDB = _transaction._keyValueDB;
        try
        {
            return keyValueDB.ReadValueSpan(trueValue, ref buffer, copy);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IRootNode)keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IRootNode)keyValueDB.ReferenceAndGetLastCommitted();
            try
            {
                var treeRoot = _transaction.BTreeRoot!;
                throw new BTDBException(
                    $"{ex.Message} in TrId:{treeRoot.TransactionId},TRL:{treeRoot.TrLogFileId},Ofs:{treeRoot.TrLogOffset},ComUlong:{treeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong}",
                    ex);
            }
            finally
            {
                keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                keyValueDB.DereferenceRootNodeInternal(lastCommitted);
            }
        }
    }

    void EnsureValidKey()
    {
        if (!_cursor.IsValid())
        {
            throw new InvalidOperationException("Current key is not valid");
        }
    }

    [SkipLocalsInit]
    public void SetValue(in ReadOnlySpan<byte> value)
    {
        EnsureValidKey();
        _transaction.MakeWritable();
        Span<byte> trueValue = stackalloc byte[12];
        _transaction._keyValueDB.WriteCreateOrUpdateCommand(_cursor.GetKeyParts(out var keySuffix), keySuffix, value,
            trueValue);
        _cursor.WriteValue(trueValue);
    }

    public void EraseCurrent()
    {
        EnsureValidKey();
        var keyIndexToRemove = GetKeyIndex();
        _transaction.MakeWritable();
        _transaction._keyValueDB.WriteEraseOneCommand(_cursor.GetKeyParts(out var keySuffix), keySuffix);
        var cursor = (IKeyValueDBCursorInternal)_transaction.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this)
            {
                cursor.NotifyRemove(keyIndexToRemove, keyIndexToRemove);
            }

            cursor = cursor.NextCursor;
        }
        _cursor.Erase();

        InvalidateCurrentKey();
    }

    public void NotifyRemove(ulong startIndex, ulong endIndex)
    {
        throw new NotImplementedException();
    }

    public void NotifyInsert(ulong index)
    {
        throw new NotImplementedException();
    }

    public IKeyValueDBCursorInternal? PrevCursor { get; set; }
    public IKeyValueDBCursorInternal? NextCursor { get; set; }

    public void NotifyWritableTransaction()
    {
        _cursor.SetNewRoot(_transaction.BTreeRoot!);
    }
}

public class BTreeKeyValueDBTransaction : IKeyValueDBTransaction
{
    internal readonly BTreeKeyValueDB _keyValueDB;
    public IRootNode? BTreeRoot;
    readonly ICursor _cursor;
    ICursor? _cursor2;
    readonly bool _readOnly;
    bool _writing;
    bool _preapprovedWriting;
    bool _temporaryCloseTransactionLog;
    long _keyIndex;
    long _cursorMovedCounter;

    public DateTime CreatedTime { get; } = DateTime.UtcNow;

    public BTreeKeyValueDBTransaction(BTreeKeyValueDB keyValueDB, IRootNode root, bool writing, bool readOnly)
    {
        _preapprovedWriting = writing;
        _readOnly = readOnly;
        _keyValueDB = keyValueDB;
        _keyIndex = -1;
        _cursor = root.CreateCursor();
        _cursor2 = null;
        BTreeRoot = root;
        _cursorMovedCounter = 0;
    }

    ~BTreeKeyValueDBTransaction()
    {
        if (BTreeRoot != null || _writing || _preapprovedWriting)
        {
            Dispose();
            _keyValueDB.Logger?.ReportTransactionLeak(this);
        }
    }

    public IKeyValueDBCursor CreateCursor()
    {
        return new BTreeKeyValueDBCursor(this);
    }

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        _cursorMovedCounter++;
        _keyIndex = -1;
        return _cursor.FindFirst(prefix);
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        _cursorMovedCounter++;
        _keyIndex = _cursor.FindLastWithPrefix(prefix);
        return _keyIndex >= 0;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        if (!_cursor.IsValid()) return FindLastKey(prefix);
        _cursorMovedCounter++;
        _keyIndex = -1;
        if (_cursor.MovePrevious())
        {
            if (_cursor.KeyHasPrefix(prefix))
            {
                return true;
            }
        }

        InvalidateCurrentKey();
        return false;
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        if (!_cursor.IsValid()) return FindFirstKey(prefix);
        _cursorMovedCounter++;
        _keyIndex = -1;
        if (_cursor.MoveNext())
        {
            if (_cursor.KeyHasPrefix(prefix))
            {
                return true;
            }
        }

        InvalidateCurrentKey();
        return false;
    }

    public FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        _cursorMovedCounter++;
        var result = _cursor.Find(key);
        _keyIndex = -1;
        if (prefixLen == 0) return result;
        switch (result)
        {
            case FindResult.Previous when !_cursor.KeyHasPrefix(key[..(int)prefixLen]):
            {
                if (!_cursor.MoveNext())
                {
                    return FindResult.NotFound;
                }

                if (_cursor.KeyHasPrefix(key[..(int)prefixLen]))
                {
                    return FindResult.Next;
                }

                InvalidateCurrentKey();
                return FindResult.NotFound;
            }
            case FindResult.Next when !_cursor.KeyHasPrefix(key[..(int)prefixLen]):
                // FindResult.Previous is preferred that's why it has to be NotFound when next does not match prefix
                InvalidateCurrentKey();
                return FindResult.NotFound;
            default:
                return result;
        }
    }

    [SkipLocalsInit]
    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        _cursorMovedCounter++;
        MakeWritable();
        Span<byte> trueValue = stackalloc byte[12];
        _keyValueDB.WriteCreateOrUpdateCommand(key, default, value, trueValue);
        var result = _cursor.Upsert(key, trueValue);
        _keyIndex = -1;
        return result;
    }

    public UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        _cursorMovedCounter++;
        MakeWritable();
        if (!_cursor.FindFirst(key[..(int)prefixLen])) return UpdateKeySuffixResult.NotFound;
        if (_cursor.MoveNext())
        {
            if (_cursor.KeyHasPrefix(key[..(int)prefixLen]))
            {
                return UpdateKeySuffixResult.NotUniquePrefix;
            }
        }

        _cursor.MovePrevious();

        _keyIndex = -1;
        if (_cursor.KeyHasPrefix(key) && _cursor.GetKeyLength() == key.Length)
        {
            return UpdateKeySuffixResult.NothingToDo;
        }

        _cursor.UpdateKeySuffix(key);
        _keyValueDB.WriteUpdateKeySuffixCommand(key, prefixLen);
        return UpdateKeySuffixResult.Updated;
    }

    internal void MakeWritable()
    {
        if (_writing) return;
        if (_preapprovedWriting)
        {
            _writing = true;
            _preapprovedWriting = false;
            _keyValueDB.WriteStartTransaction();
            return;
        }

        if (_readOnly)
        {
            throw new BTDBTransactionRetryException("Cannot write from readOnly transaction");
        }

        BTreeRoot = _keyValueDB.MakeWritableTransaction(this, BTreeRoot!);

        _cursor.SetNewRoot(BTreeRoot);
        _cursor2?.SetNewRoot(BTreeRoot);
        BTreeRoot.DescriptionForLeaks = _descriptionForLeaks;
        _writing = true;
        _keyValueDB.WriteStartTransaction();

        var cursor = (IKeyValueDBCursorInternal)FirstCursor;
        while (cursor != null)
        {
            cursor.NotifyWritableTransaction();
            cursor = cursor.NextCursor;
        }
    }

    public long GetKeyValueCount()
    {
        return BTreeRoot!.GetCount();
    }

    public long GetKeyIndex()
    {
        if (_keyIndex == -1) _keyIndex = _cursor.CalcIndex();
        return _keyIndex;
    }

    public bool SetKeyIndex(long index)
    {
        _cursorMovedCounter++;
        _keyIndex = index;
        if (_cursor.SeekIndex(index)) return true;
        InvalidateCurrentKey();
        return false;
    }

    public bool SetKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        _cursorMovedCounter++;
        if (!_cursor.FindFirst(prefix))
        {
            InvalidateCurrentKey();
            return false;
        }

        index += _cursor.CalcIndex();
        if (_cursor.SeekIndex(index))
        {
            _keyIndex = index;
            if (_cursor.KeyHasPrefix(prefix))
                return true;
        }

        InvalidateCurrentKey();
        return false;
    }

    public void InvalidateCurrentKey()
    {
        _cursorMovedCounter++;
        _keyIndex = -1;
        _cursor.Invalidate();
    }

    public bool IsValidKey()
    {
        return _cursor.IsValid();
    }

    public ReadOnlySpan<byte> GetKey()
    {
        Span<byte> dummyKeyBuffer = stackalloc byte[1];
        return GetKey(ref MemoryMarshal.GetReference(dummyKeyBuffer), 0);
    }

    public byte[] GetKeyToArray()
    {
        return _cursor.GetKeyAsArray();
    }

    public ReadOnlySpan<byte> GetKey(scoped ref byte buffer, int bufferLength)
    {
        return _cursor.GetKey(ref buffer, bufferLength);
    }

    public ReadOnlySpan<byte> GetClonedValue(ref byte buffer, int bufferLength)
    {
        if (!IsValidKey()) return new();
        var reader = MemReader.CreateFromPinnedSpan(MemoryMarshal.CreateSpan(ref buffer, bufferLength));
        GetValue(ref reader);
        return reader.PeekSpanTillEof();
    }

    public ReadOnlySpan<byte> GetValue()
    {
        if (!IsValidKey()) return new();
        var trueValue = _cursor.GetValue();
        try
        {
            return _keyValueDB.ReadValue(trueValue);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IRootNode)_keyValueDB.ReferenceAndGetLastCommitted();
            try
            {
                throw new BTDBException(
                    $"GetValue failed in TrId:{BTreeRoot!.TransactionId},TRL:{BTreeRoot.TrLogFileId},Ofs:{BTreeRoot.TrLogOffset},ComUlong:{BTreeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}",
                    ex);
            }
            finally
            {
                _keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                _keyValueDB.DereferenceRootNodeInternal(lastCommitted);
            }
        }
    }

    public ReadOnlyMemory<byte> GetValueAsMemory()
    {
        if (!IsValidKey()) return new();
        var reader = new MemReader();
        GetValue(ref reader);
        return reader.AsReadOnlyMemory();
    }

    public void GetValue(ref MemReader reader)
    {
        if (!IsValidKey())
        {
            reader.Dispose();
            return;
        }

        var trueValue = _cursor.GetValue();
        try
        {
            _keyValueDB.ReadValueIntoMemReader(trueValue, ref reader);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IRootNode)_keyValueDB.ReferenceAndGetLastCommitted();
            try
            {
                throw new BTDBException(
                    $"GetValue failed in TrId:{BTreeRoot!.TransactionId},TRL:{BTreeRoot.TrLogFileId},Ofs:{BTreeRoot.TrLogOffset},ComUlong:{BTreeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}",
                    ex);
            }
            finally
            {
                _keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                _keyValueDB.DereferenceRootNodeInternal(lastCommitted);
            }
        }
    }

    public bool IsValueCorrupted()
    {
        if (!IsValidKey()) return false;
        var trueValue = _cursor.GetValue();
        return _keyValueDB.IsCorruptedValue(trueValue);
    }

    void EnsureValidKey()
    {
        if (!_cursor.IsValid())
        {
            throw new InvalidOperationException("Current key is not valid");
        }
    }

    [SkipLocalsInit]
    public void SetValue(in ReadOnlySpan<byte> value)
    {
        EnsureValidKey();
        MakeWritable();
        Span<byte> trueValue = stackalloc byte[12];
        Span<byte> buffer = stackalloc byte[512];
        _keyValueDB.WriteCreateOrUpdateCommand(_cursor.GetKeyParts(out var keySuffix), keySuffix, value, trueValue);
        _cursor.WriteValue(trueValue);
    }

    public void EraseCurrent()
    {
        EnsureValidKey();
        MakeWritable();
        _keyValueDB.WriteEraseOneCommand(_cursor.GetKeyParts(out var keySuffix), keySuffix);
        _cursor.Erase();
        InvalidateCurrentKey();
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey)
    {
        if (_cursor.Find(exactKey) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            return false;
        }

        MakeWritable();
        _keyValueDB.WriteEraseOneCommand(exactKey, default);
        _cursor.Erase();
        InvalidateCurrentKey();
        return true;
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey, ref MemReader valueReader)
    {
        if (_cursor.Find(exactKey) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            return false;
        }

        GetValue(ref valueReader);
        MakeWritable();
        _keyValueDB.WriteEraseOneCommand(exactKey, default);
        _cursor.Erase();
        InvalidateCurrentKey();
        return true;
    }

    public void EraseAll()
    {
        EraseRange(0, GetKeyValueCount() - 1);
    }

    [SkipLocalsInit]
    public void EraseRange(long firstKeyIndex, long lastKeyIndex)
    {
        if (firstKeyIndex < 0) firstKeyIndex = 0;
        if (lastKeyIndex >= GetKeyValueCount()) lastKeyIndex = GetKeyValueCount() - 1;
        if (lastKeyIndex < firstKeyIndex) return;
        MakeWritable();
        _cursor.SeekIndex(firstKeyIndex);
        if (lastKeyIndex != firstKeyIndex)
        {
            _cursor2 ??= BTreeRoot!.CreateCursor();
            _cursor2.SeekIndex(lastKeyIndex);
            var firstKeyPrefix = _cursor.GetKeyParts(out var firstKeySuffix);
            var secondKeyPrefix = _cursor2.GetKeyParts(out var secondKeySuffix);
            _keyValueDB.WriteEraseRangeCommand(firstKeyPrefix, firstKeySuffix, secondKeyPrefix, secondKeySuffix);
            _cursor.EraseTo(_cursor2);
        }
        else
        {
            _keyValueDB.WriteEraseOneCommand(_cursor.GetKeyParts(out var keySuffix), keySuffix);
            _cursor.Erase();
        }

        InvalidateCurrentKey();
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
        _keyValueDB.CommitFromCompactor(currentRoot);
    }

    public void Commit()
    {
        if (BTreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
        InvalidateCurrentKey();
        var currentRoot = BTreeRoot;
        BTreeRoot = null;
        if (_preapprovedWriting)
        {
            _preapprovedWriting = false;
            _keyValueDB.RevertWritingTransaction(currentRoot!, true);
        }
        else if (_writing)
        {
            _keyValueDB.CommitWritingTransaction(currentRoot!, _temporaryCloseTransactionLog);
            _writing = false;
        }
        else
        {
            _keyValueDB.DereferenceRoot(currentRoot!);
        }
    }

    public void Dispose()
    {
        var currentRoot = Interlocked.Exchange(ref BTreeRoot, null);
        if (_writing || _preapprovedWriting)
        {
            _keyValueDB.RevertWritingTransaction(currentRoot!, _preapprovedWriting);
            _writing = false;
            _preapprovedWriting = false;
        }
        else if (currentRoot != null)
        {
            _keyValueDB.DereferenceRoot(currentRoot);
        }

        GC.SuppressFinalize(this);
    }

    public long GetTransactionNumber()
    {
        return BTreeRoot!.TransactionId;
    }

    public long CursorMovedCounter => _cursorMovedCounter;

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        if (!IsValidKey()) return new KeyValuePair<uint, uint>();
        var keyLen = _cursor.GetKeyLength();
        var trueValue = _cursor.GetValue();
        return new KeyValuePair<uint, uint>(
            (uint)keyLen,
            _keyValueDB.CalcValueSize(MemoryMarshal.Read<uint>(trueValue), MemoryMarshal.Read<uint>(trueValue[4..]),
                MemoryMarshal.Read<int>(trueValue[8..])));
    }

    public ulong GetUlong(uint idx)
    {
        return BTreeRoot!.GetUlong(idx);
    }

    public void SetUlong(uint idx, ulong value)
    {
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

    public IKeyValueDB Owner => _keyValueDB;

    public bool RollbackAdvised { get; set; }

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats()
    {
        var stats = new RefDictionary<(uint Depth, uint Children), uint>();
        BTreeRoot!.CalcBTreeStats(stats, 0);
        return stats.ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    public IKeyValueDBCursor? FirstCursor { get; set; }
    public IKeyValueDBCursor? LastCursor { get; set; }
}
