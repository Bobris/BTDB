using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using BTDB.Collections;
using BTDB.KVDBLayer.BTreeMem;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

class InMemoryKeyValueDBCursor : IKeyValueDBCursorInternal
{
    InMemoryKeyValueDBTransaction _transaction;
    StructList<NodeIdxPair> _stack;
    bool _modifiedFromLastFind;
    bool _removedCurrent;
    long _keyIndex;

    public InMemoryKeyValueDBCursor(InMemoryKeyValueDBTransaction transaction)
    {
        _transaction = transaction;
        _keyIndex = -1;
        _modifiedFromLastFind = false;
        _removedCurrent = false;
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

        _stack.Clear();
        _transaction = null!;
    }

    public IKeyValueDBTransaction Transaction => _transaction;

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        _modifiedFromLastFind = false;
        if (_transaction._btreeRoot!.FindKey(ref _stack, out _keyIndex, prefix, (uint)prefix.Length) ==
            FindResult.NotFound)
        {
            return false;
        }

        return true;
    }

    bool CheckPrefixIn(in ReadOnlySpan<byte> prefix, in ReadOnlySpan<byte> key)
    {
        return BTreeRoot.KeyStartsWithPrefix(prefix, key);
    }

    ReadOnlySpan<byte> GetCurrentKeyFromStack()
    {
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Span;
    }

    ReadOnlyMemory<byte> GetCurrentKeyFromStackAsMemory()
    {
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx);
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        _modifiedFromLastFind = false;
        _keyIndex = _transaction._btreeRoot!.FindLastWithPrefix(prefix);
        if (_keyIndex == -1)
        {
            _stack.Clear();
            return false;
        }

        _transaction._btreeRoot!.FillStackByIndex(ref _stack, _keyIndex);
        return true;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        if (_modifiedFromLastFind)
        {
            if (FindKeyIndex(_keyIndex - 1) && CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
            {
                return true;
            }
        }
        else
        {
            if (_keyIndex < 0) return FindLastKey(prefix);
            if (_transaction._btreeRoot!.FindPreviousKey(ref _stack))
            {
                if (CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
                {
                    _keyIndex--;
                    return true;
                }
            }
        }

        _keyIndex = -1;
        _stack.Clear();
        return false;
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        if (_modifiedFromLastFind)
        {
            if (!_removedCurrent) _keyIndex++;
            if (FindKeyIndex(_keyIndex) && CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
            {
                return true;
            }
        }
        else
        {
            if (_keyIndex < 0) return FindFirstKey(prefix);
            if (_transaction._btreeRoot!.FindNextKey(ref _stack))
            {
                if (CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
                {
                    _keyIndex++;
                    return true;
                }
            }
        }

        _keyIndex = -1;
        _stack.Clear();
        return false;
    }

    public FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        _modifiedFromLastFind = false;
        return _transaction._btreeRoot!.FindKey(ref _stack, out _keyIndex, key, prefixLen);
    }

    public long GetKeyIndex()
    {
        return _keyIndex;
    }

    public bool FindKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        _modifiedFromLastFind = false;
        if (_transaction._btreeRoot!.FindKey(ref _stack, out _keyIndex, prefix, (uint)prefix.Length) ==
            FindResult.NotFound)
        {
            return false;
        }

        _keyIndex += index;
        if (_keyIndex < 0 || _keyIndex >= _transaction._btreeRoot!.CalcKeyCount())
        {
            _keyIndex = -1;
            _stack.Clear();
            return false;
        }

        _transaction._btreeRoot!.FillStackByIndex(ref _stack, _keyIndex);
        return true;
    }

    public bool FindKeyIndex(long index)
    {
        _modifiedFromLastFind = false;
        _keyIndex = index;
        if (index < 0 || index >= _transaction._btreeRoot!.CalcKeyCount())
        {
            _keyIndex = -1;
            _stack.Clear();
            return false;
        }

        _transaction._btreeRoot!.FillStackByIndex(ref _stack, index);
        return true;
    }

    public void Invalidate()
    {
        _modifiedFromLastFind = false;
        _keyIndex = -1;
        _stack.Clear();
    }

    public bool IsValid()
    {
        return _keyIndex >= 0;
    }

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        var nodeIdxPair = _stack[^1];
        return new(
            (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Length,
            (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx).Length);
    }

    public ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy = false)
    {
        if (copy)
        {
            var key = GetCurrentKeyFromStackAsMemory();
            if (buffer.Length < key.Length) buffer = GC.AllocateUninitializedArray<byte>(key.Length);
            key.Span.CopyTo(buffer.Span);
            return buffer[..key.Length];
        }

        return GetCurrentKeyFromStackAsMemory();
    }

    public ReadOnlySpan<byte> GetKeySpan(ref Memory<byte> buffer, bool copy = false)
    {
        if (copy)
        {
            var key = GetCurrentKeyFromStack();
            if (buffer.Length < key.Length) buffer = GC.AllocateUninitializedArray<byte>(key.Length);
            key.CopyTo(buffer.Span);
            return buffer.Span[..key.Length];
        }

        return GetCurrentKeyFromStack();
    }

    public bool IsValueCorrupted()
    {
        return false;
    }

    public ReadOnlyMemory<byte> GetValueMemory(ref Memory<byte> buffer, bool copy = false)
    {
        var nodeIdxPair = _stack[^1];
        var value = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
        if (copy)
        {
            if (buffer.Length < value.Length) buffer = GC.AllocateUninitializedArray<byte>(value.Length);
            value.Span.CopyTo(buffer.Span);
            return buffer[..value.Length];
        }

        return value;
    }

    public ReadOnlySpan<byte> GetValueSpan(ref Memory<byte> buffer, bool copy = false)
    {
        var nodeIdxPair = _stack[^1];
        var value = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx).Span;
        if (copy)
        {
            if (buffer.Length < value.Length) buffer = GC.AllocateUninitializedArray<byte>(value.Length);
            value.CopyTo(buffer.Span);
            return buffer.Span[..value.Length];
        }

        return value;
    }

    public void SetValue(in ReadOnlySpan<byte> value)
    {
        _transaction.MakeWritable();
        if (_keyIndex != -1 && _stack.Count == 0)
        {
            _transaction._btreeRoot!.FillStackByIndex(ref _stack, _keyIndex);
        }

        var nodeIdxPair = _stack[^1];
        ((IBTreeLeafNode)nodeIdxPair.Node).SetMemberValue(nodeIdxPair.Idx, value);
    }

    void EnsureValidCursor()
    {
        if (_keyIndex == -1 || _stack.Count == 0)
        {
            if (_modifiedFromLastFind && _keyIndex != -1)
            {
                if (FindKeyIndex(_keyIndex))
                {
                    return;
                }
            }

            throw new InvalidOperationException("Current key is not valid");
        }
    }

    public void EraseCurrent()
    {
        EnsureValidCursor();
        var keyIndexToRemove = (ulong)_keyIndex;
        var cursor = (IKeyValueDBCursorInternal)_transaction.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this)
            {
                cursor.NotifyRemove(keyIndexToRemove, keyIndexToRemove);
            }

            cursor = cursor.NextCursor;
        }

        _transaction.MakeWritable();

        _transaction._btreeRoot!.EraseRange((long)keyIndexToRemove, (long)keyIndexToRemove);
        _keyIndex = (long)keyIndexToRemove;
        _modifiedFromLastFind = true;
        _removedCurrent = true;
    }

    public void EraseUpTo(IKeyValueDBCursor to)
    {
        EnsureValidCursor();
        var trueTo = (InMemoryKeyValueDBCursor)to;
        trueTo.EnsureValidCursor();
        var firstKeyIndex = (ulong)GetKeyIndex();
        var lastKeyIndex = (ulong)trueTo.GetKeyIndex();
        if (lastKeyIndex < firstKeyIndex) return;

        var cursor = (IKeyValueDBCursorInternal)_transaction.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this || cursor != to)
            {
                cursor.NotifyRemove(firstKeyIndex, lastKeyIndex);
            }

            cursor = cursor.NextCursor;
        }

        _transaction.MakeWritable();
        _transaction._btreeRoot!.EraseRange((long)firstKeyIndex, (long)lastKeyIndex);
        _stack.Clear();
        trueTo._stack.Clear();
        _keyIndex = (long)firstKeyIndex;
        trueTo._keyIndex = (long)firstKeyIndex;
        _modifiedFromLastFind = true;
        trueTo._modifiedFromLastFind = true;
        _removedCurrent = true;
        trueTo._removedCurrent = true;
    }

    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        var cursor = (IKeyValueDBCursorInternal)_transaction.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this)
            {
                cursor.PreNotifyUpsert();
            }

            cursor = cursor.NextCursor;
        }

        _modifiedFromLastFind = false;
        var ctx = new CreateOrUpdateCtx
        {
            Key = key,
            Value = value,
            Stack = ref _stack
        };
        _transaction.MakeWritable();
        _transaction._btreeRoot!.CreateOrUpdate(ref ctx);
        _keyIndex = ctx.KeyIndex;
        if (ctx.Created)
        {
            cursor = (IKeyValueDBCursorInternal)_transaction.FirstCursor;
            while (cursor != null)
            {
                if (cursor != this)
                {
                    cursor.NotifyInsert((ulong)_keyIndex);
                }

                cursor = cursor.NextCursor;
            }
        }

        return ctx.Created;
    }

    public UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        var cursor = (IKeyValueDBCursorInternal)_transaction.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this)
            {
                cursor.PreNotifyUpsert();
            }

            cursor = cursor.NextCursor;
        }

        _modifiedFromLastFind = false;
        var ctx = new UpdateKeySuffixCtx
        {
            Key = key,
            PrefixLen = prefixLen,
            Stack = ref _stack
        };
        _transaction.MakeWritable();
        _transaction._btreeRoot!.UpdateKeySuffix(ref ctx);
        _keyIndex = ctx.KeyIndex;
        return ctx.Result;
    }

    public void NotifyRemove(ulong startIndex, ulong endIndex)
    {
        if (_modifiedFromLastFind)
        {
            if (_keyIndex == -1) return;
        }
        else
        {
            _stack.Clear();
            _modifiedFromLastFind = true;
            _removedCurrent = false;
        }

        if ((ulong)_keyIndex >= startIndex && (ulong)_keyIndex <= endIndex)
        {
            _keyIndex = (long)startIndex;
            _removedCurrent = true;
        }
        else if ((ulong)_keyIndex > endIndex)
        {
            _keyIndex -= (long)(endIndex - startIndex + 1);
        }
    }

    public void PreNotifyUpsert()
    {
        _stack.Clear();
        if (!_modifiedFromLastFind)
        {
            _removedCurrent = false;
        }

        _modifiedFromLastFind = true;
    }

    public void NotifyInsert(ulong index)
    {
        if (_keyIndex != -1)
        {
            if ((ulong)_keyIndex >= index)
            {
                _keyIndex++;
            }
        }
    }

    public IKeyValueDBCursorInternal? PrevCursor { get; set; }

    public IKeyValueDBCursorInternal? NextCursor { get; set; }

    public void NotifyWritableTransaction()
    {
        _stack.Clear();
    }
}

class InMemoryKeyValueDBTransaction : IKeyValueDBTransaction
{
    readonly InMemoryKeyValueDB _keyValueDB;
    internal IBTreeRootNode? _btreeRoot;
    StructList<NodeIdxPair> _stack;
    bool _writing;
    readonly bool _readOnly;
    bool _preapprovedWriting;
    long _keyIndex;
    long _cursorMovedCounter;

    public InMemoryKeyValueDBTransaction(InMemoryKeyValueDB keyValueDB, IBTreeRootNode btreeRoot, bool writing,
        bool readOnly)
    {
        _preapprovedWriting = writing;
        _readOnly = readOnly;
        _keyValueDB = keyValueDB;
        _btreeRoot = btreeRoot;
        _keyIndex = -1;
        _cursorMovedCounter = 0;
    }

    public IKeyValueDBCursor CreateCursor()
    {
        return new InMemoryKeyValueDBCursor(this);
    }

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(ref _stack, out _keyIndex, prefix, (uint)prefix.Length) == FindResult.NotFound)
        {
            return false;
        }

        return true;
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        _cursorMovedCounter++;
        _keyIndex = _btreeRoot!.FindLastWithPrefix(prefix);
        if (_keyIndex == -1)
        {
            _stack.Clear();
            return false;
        }

        _btreeRoot!.FillStackByIndex(ref _stack, _keyIndex);
        return true;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        if (_keyIndex < 0) return FindLastKey(prefix);
        _cursorMovedCounter++;
        if (_btreeRoot!.FindPreviousKey(ref _stack))
        {
            if (CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
            {
                _keyIndex--;
                return true;
            }
        }

        InvalidateCurrentKey();
        return false;
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        if (_keyIndex < 0) return FindFirstKey(prefix);
        _cursorMovedCounter++;
        if (_btreeRoot!.FindNextKey(ref _stack))
        {
            if (CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
            {
                _keyIndex++;
                return true;
            }
        }

        InvalidateCurrentKey();
        return false;
    }

    public FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        _cursorMovedCounter++;
        return _btreeRoot!.FindKey(ref _stack, out _keyIndex, key, prefixLen);
    }

    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        _cursorMovedCounter++;
        MakeWritable();
        var ctx = new CreateOrUpdateCtx
        {
            Key = key,
            Value = value,
            Stack = ref _stack
        };
        _btreeRoot!.CreateOrUpdate(ref ctx);
        _keyIndex = ctx.KeyIndex;
        return ctx.Created;
    }

    public UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        _cursorMovedCounter++;
        MakeWritable();
        var ctx = new UpdateKeySuffixCtx
        {
            Key = key,
            PrefixLen = prefixLen,
            Stack = ref _stack
        };
        _btreeRoot!.UpdateKeySuffix(ref ctx);
        _keyIndex = ctx.KeyIndex;
        return ctx.Result;
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
        InvalidateCurrentKey();
        var cursor = (IKeyValueDBCursorInternal)FirstCursor;
        while (cursor != null)
        {
            cursor.NotifyWritableTransaction();
            cursor = cursor.NextCursor;
        }
    }

    public long GetKeyValueCount()
    {
        return _btreeRoot!.CalcKeyCount();
    }

    public long GetKeyIndex()
    {
        if (_keyIndex < 0) return -1;
        return _keyIndex;
    }

    public bool SetKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(ref _stack, out _keyIndex, prefix, (uint)prefix.Length) == FindResult.NotFound)
        {
            return false;
        }

        index += _keyIndex;
        if (index < _btreeRoot!.CalcKeyCount())
        {
            _btreeRoot!.FillStackByIndex(ref _stack, index);
            _keyIndex = index;
            if (CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
            {
                return true;
            }
        }

        InvalidateCurrentKey();
        return false;
    }

    public bool SetKeyIndex(long index)
    {
        _cursorMovedCounter++;
        _keyIndex = index;
        if (index < 0 || index >= _btreeRoot!.CalcKeyCount())
        {
            InvalidateCurrentKey();
            return false;
        }

        _btreeRoot!.FillStackByIndex(ref _stack, index);
        return true;
    }

    bool CheckPrefixIn(in ReadOnlySpan<byte> prefix, in ReadOnlySpan<byte> key)
    {
        return BTreeRoot.KeyStartsWithPrefix(prefix, key);
    }

    ReadOnlySpan<byte> GetCurrentKeyFromStack()
    {
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Span;
    }

    public void InvalidateCurrentKey()
    {
        _cursorMovedCounter++;
        _keyIndex = -1;
        _stack.Clear();
    }

    public bool IsValidKey()
    {
        return _keyIndex >= 0;
    }

    public ReadOnlySpan<byte> GetKey()
    {
        if (!IsValidKey()) return new();
        return GetCurrentKeyFromStack();
    }

    public byte[] GetKeyToArray()
    {
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).ToArray();
    }

    public ReadOnlySpan<byte> GetKey(ref byte buffer, int bufferLength)
    {
        if (!IsValidKey()) return new();
        var originalRes = GetCurrentKeyFromStack();
        var len = originalRes.Length;
        var res = len <= bufferLength
            ? MemoryMarshal.CreateSpan(ref buffer, len)
            : GC.AllocateUninitializedArray<byte>(len, pinned: true).AsSpan(0, len);
        originalRes.CopyTo(res);
        return res;
    }

    public ReadOnlySpan<byte> GetClonedValue(ref byte buffer, int bufferLength)
    {
        // it is always read only memory already
        return GetValue();
    }

    public ReadOnlySpan<byte> GetValue()
    {
        if (!IsValidKey()) return new();
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx).Span;
    }

    public ReadOnlyMemory<byte> GetValueAsMemory()
    {
        if (!IsValidKey()) return new();
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
    }

    public void GetValue(ref MemReader reader)
    {
        MemReader.InitFromReadOnlyMemory(ref reader, GetValueAsMemory());
    }

    public bool IsValueCorrupted()
    {
        return false;
    }

    void EnsureValidKey()
    {
        if (_keyIndex < 0)
        {
            throw new InvalidOperationException("Current key is not valid");
        }
    }

    public void SetValue(in ReadOnlySpan<byte> value)
    {
        EnsureValidKey();
        var keyIndexBackup = _keyIndex;
        MakeWritable();
        if (_keyIndex != keyIndexBackup)
        {
            _keyIndex = keyIndexBackup;
            _btreeRoot!.FillStackByIndex(ref _stack, _keyIndex);
        }

        var nodeIdxPair = _stack[^1];
        ((IBTreeLeafNode)nodeIdxPair.Node).SetMemberValue(nodeIdxPair.Idx, value);
    }

    public void EraseCurrent()
    {
        _cursorMovedCounter++;
        EnsureValidKey();
        var keyIndex = _keyIndex;
        MakeWritable();
        InvalidateCurrentKey();
        _btreeRoot!.EraseRange(keyIndex, keyIndex);
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(ref _stack, out var keyIndex, exactKey, 0) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            return false;
        }

        MakeWritable();
        InvalidateCurrentKey();
        _btreeRoot!.EraseRange(keyIndex, keyIndex);
        return true;
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey, ref MemReader valueReader)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(ref _stack, out var keyIndex, exactKey, 0) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            return false;
        }

        _keyIndex = 0; // Fake value is enough
        GetValue(ref valueReader);
        MakeWritable();
        InvalidateCurrentKey();
        _btreeRoot!.EraseRange(keyIndex, keyIndex);
        return true;
    }

    public void EraseAll()
    {
        _cursorMovedCounter++;
        EraseRange(0, GetKeyValueCount() - 1);
    }

    public void EraseRange(long firstKeyIndex, long lastKeyIndex)
    {
        if (firstKeyIndex < 0) firstKeyIndex = 0;
        if (lastKeyIndex >= GetKeyValueCount()) lastKeyIndex = GetKeyValueCount() - 1;
        if (lastKeyIndex < firstKeyIndex) return;
        _cursorMovedCounter++;
        MakeWritable();
        InvalidateCurrentKey();
        _btreeRoot!.EraseRange(firstKeyIndex, lastKeyIndex);
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
        if (_btreeRoot!.CommitUlong != value)
        {
            MakeWritable();
            _btreeRoot!.CommitUlong = value;
        }
    }

    public void NextCommitTemporaryCloseTransactionLog()
    {
        // There is no transaction log ...
    }

    public void Commit()
    {
        if (_btreeRoot! == null) throw new BTDBException("Transaction already committed or disposed");
        InvalidateCurrentKey();
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
        if (_writing || _preapprovedWriting)
        {
            _keyValueDB.RevertWritingTransaction();
            _writing = false;
            _preapprovedWriting = false;
        }

        _btreeRoot = null;
    }

    public long GetTransactionNumber()
    {
        return _btreeRoot!.TransactionId;
    }

    public long CursorMovedCounter => _cursorMovedCounter;

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        var nodeIdxPair = _stack[^1];
        return new KeyValuePair<uint, uint>(
            (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Length,
            (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx).Length);
    }

    public ulong GetUlong(uint idx)
    {
        return _btreeRoot!.GetUlong(idx);
    }

    public void SetUlong(uint idx, ulong value)
    {
        if (_btreeRoot!.GetUlong(idx) != value)
        {
            MakeWritable();
            _btreeRoot!.SetUlong(idx, value);
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
