using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.Collections;
using BTDB.KVDBLayer.BTree;

namespace BTDB.KVDBLayer;

class KeyValueDBTransaction : IKeyValueDBTransaction
{
    readonly KeyValueDB _keyValueDB;
    IBTreeRootNode? _btreeRoot;
    readonly List<NodeIdxPair> _stack = new();
    bool _writing;
    readonly bool _readOnly;
    bool _preapprovedWriting;
    bool _temporaryCloseTransactionLog;
    long _keyIndex;
    long _cursorMovedCounter;

    public DateTime CreatedTime { get; } = DateTime.UtcNow;

    public KeyValueDBTransaction(KeyValueDB keyValueDB, IBTreeRootNode btreeRoot, bool writing, bool readOnly)
    {
        _preapprovedWriting = writing;
        _readOnly = readOnly;
        _keyValueDB = keyValueDB;
        _btreeRoot = btreeRoot;
        _keyIndex = -1;
        _cursorMovedCounter = 0;
        _keyValueDB.StartedUsingBTreeRoot(_btreeRoot);
    }

    ~KeyValueDBTransaction()
    {
        if (_btreeRoot != null || _writing || _preapprovedWriting)
        {
            Dispose();
            _keyValueDB.Logger?.ReportTransactionLeak(this);
        }
    }

    internal IBTreeRootNode? BtreeRoot => _btreeRoot;

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(_stack, out _keyIndex, prefix, (uint)prefix.Length) !=
            FindResult.NotFound) return true;
        InvalidateCurrentKey();
        return false;
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        _cursorMovedCounter++;
        _keyIndex = _btreeRoot!.FindLastWithPrefix(prefix);
        if (_keyIndex == -1) return false;
        _btreeRoot.FillStackByIndex(_stack, _keyIndex);
        return true;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        if (_keyIndex == -1) return FindLastKey(prefix);
        _cursorMovedCounter++;
        if (_btreeRoot!.FindPreviousKey(_stack))
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
        if (_keyIndex == -1) return FindFirstKey(prefix);
        _cursorMovedCounter++;
        if (_btreeRoot!.FindNextKey(_stack))
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
        return _btreeRoot!.FindKey(_stack, out _keyIndex, key, prefixLen);
    }

    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        _cursorMovedCounter++;
        MakeWritable();
        _keyValueDB.WriteCreateOrUpdateCommand(key, value, out var valueFileId, out var valueOfs, out var valueSize);
        var ctx = new CreateOrUpdateCtx
        {
            Key = key,
            ValueFileId = valueFileId,
            ValueOfs = valueOfs,
            ValueSize = valueSize,
            Stack = _stack
        };
        _btreeRoot!.CreateOrUpdate(ref ctx);
        _keyIndex = ctx.KeyIndex;
        return ctx.Created;
    }

    void MakeWritable()
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

        var oldBTreeRoot = _btreeRoot;
        _btreeRoot = _keyValueDB.MakeWritableTransaction(this, oldBTreeRoot!);
        _keyValueDB.StartedUsingBTreeRoot(_btreeRoot);
        _keyValueDB.FinishedUsingBTreeRoot(oldBTreeRoot);
        _btreeRoot.DescriptionForLeaks = _descriptionForLeaks;
        _writing = true;
        InvalidateCurrentKey();
        _keyValueDB.WriteStartTransaction();
    }

    public long GetKeyValueCount() => _btreeRoot!.CalcKeyCount();

    public long GetKeyIndex() => _keyIndex;

    public bool SetKeyIndex(long index)
    {
        _cursorMovedCounter++;
        if (index < 0 || index >= _btreeRoot!.CalcKeyCount())
        {
            InvalidateCurrentKey();
            return false;
        }

        _keyIndex = index;
        _btreeRoot!.FillStackByIndex(_stack, _keyIndex);
        return true;
    }

    public bool SetKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(_stack, out _keyIndex, prefix, (uint)prefix.Length) ==
            FindResult.NotFound)
        {
            InvalidateCurrentKey();
            return false;
        }

        index += _keyIndex;
        if (index < _btreeRoot!.CalcKeyCount())
        {
            _keyIndex = index;
            _btreeRoot!.FillStackByIndex(_stack, _keyIndex);
            if (CheckPrefixIn(prefix, GetCurrentKeyFromStack()))
                return true;
        }

        InvalidateCurrentKey();
        return false;
    }

    static bool CheckPrefixIn(in ReadOnlySpan<byte> prefix, in ReadOnlySpan<byte> key)
    {
        return BTreeRoot.KeyStartsWithPrefix(prefix, key);
    }

    ReadOnlySpan<byte> GetCurrentKeyFromStack()
    {
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx);
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
        if (!IsValidKey()) return new ReadOnlySpan<byte>();
        return GetCurrentKeyFromStack();
    }

    public byte[] GetKeyToArray()
    {
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).ToArray();
    }

    public ReadOnlySpan<byte> GetKey(ref byte buffer, int bufferLength)
    {
        if (!IsValidKey()) return new ReadOnlySpan<byte>();
        return GetCurrentKeyFromStack();
    }

    public ReadOnlySpan<byte> GetClonedValue(ref byte buffer, int bufferLength)
    {
        if (!IsValidKey()) return ReadOnlySpan<byte>.Empty;
        var nodeIdxPair = _stack[^1];
        var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
        try
        {
            return _keyValueDB.ReadValue(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize, ref buffer,
                bufferLength);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IBTreeRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IBTreeRootNode)_keyValueDB.ReferenceAndGetLastCommitted();
            // no need to dereference roots because we know it is managed
            throw new BTDBException(
                $"GetValue failed in TrId:{_btreeRoot!.TransactionId},TRL:{_btreeRoot!.TrLogFileId},Ofs:{_btreeRoot!.TrLogOffset},ComUlong:{_btreeRoot!.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}",
                ex);
        }
    }

    public ReadOnlySpan<byte> GetValue()
    {
        return GetClonedValue(ref Unsafe.AsRef((byte)0), 0);
    }

    public ReadOnlyMemory<byte> GetValueAsMemory()
    {
        if (!IsValidKey()) return new ();
        var nodeIdxPair = _stack[^1];
        var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
        try
        {
            return _keyValueDB.ReadValueAsMemory(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IBTreeRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IBTreeRootNode)_keyValueDB.ReferenceAndGetLastCommitted();
            // no need to dereference roots because we know it is managed
            throw new BTDBException(
                $"GetValue failed in TrId:{_btreeRoot!.TransactionId},TRL:{_btreeRoot!.TrLogFileId},Ofs:{_btreeRoot!.TrLogOffset},ComUlong:{_btreeRoot!.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}",
                ex);
        }
    }

    public bool IsValueCorrupted()
    {
        if (!IsValidKey()) return false;
        var nodeIdxPair = _stack[^1];
        var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
        return _keyValueDB.IsCorruptedValue(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize);
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
            _btreeRoot!.FillStackByIndex(_stack, _keyIndex);
        }

        var nodeIdxPair = _stack[^1];
        var memberValue = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
        var memberKey = ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx);
        _keyValueDB.WriteCreateOrUpdateCommand(memberKey, value, out memberValue.ValueFileId, out memberValue.ValueOfs,
            out memberValue.ValueSize);
        ((IBTreeLeafNode)nodeIdxPair.Node).SetMemberValue(nodeIdxPair.Idx, memberValue);
    }

    public void EraseCurrent()
    {
        _cursorMovedCounter++;
        EnsureValidKey();
        var keyIndex = _keyIndex;
        MakeWritable();
        if (_keyIndex != keyIndex)
        {
            _keyIndex = keyIndex;
            _btreeRoot!.FillStackByIndex(_stack, keyIndex);
        }

        _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
        InvalidateCurrentKey();
        _btreeRoot!.EraseOne(keyIndex);
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(_stack, out _keyIndex, exactKey, 0) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            return false;
        }

        var keyIndex = _keyIndex;
        MakeWritable();
        _keyValueDB.WriteEraseOneCommand(exactKey);
        InvalidateCurrentKey();
        _btreeRoot!.EraseOne(keyIndex);
        return true;
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey, ref byte buffer, int bufferLength,
        out ReadOnlySpan<byte> value)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(_stack, out _keyIndex, exactKey, 0) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            value = ReadOnlySpan<byte>.Empty;
            return false;
        }

        var keyIndex = _keyIndex;
        value = GetClonedValue(ref buffer, bufferLength);
        MakeWritable();
        _keyValueDB.WriteEraseOneCommand(exactKey);
        InvalidateCurrentKey();
        _btreeRoot!.EraseOne(keyIndex);
        return true;
    }

    public void EraseAll()
    {
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
        _btreeRoot!.FillStackByIndex(_stack, firstKeyIndex);
        if (firstKeyIndex == lastKeyIndex)
        {
            _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
        }
        else
        {
            var firstKey = GetCurrentKeyFromStack();
            _btreeRoot!.FillStackByIndex(_stack, lastKeyIndex);
            _keyValueDB.WriteEraseRangeCommand(firstKey, GetCurrentKeyFromStack());
        }

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
        return BtreeRoot == null;
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
        MakeWritable();
        _temporaryCloseTransactionLog = true;
    }

    public void Commit()
    {
        if (BtreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
        InvalidateCurrentKey();
        var currentBtreeRoot = _btreeRoot;
        _keyValueDB.FinishedUsingBTreeRoot(_btreeRoot!);
        _btreeRoot = null;
        GC.SuppressFinalize(this);
        if (_preapprovedWriting)
        {
            _preapprovedWriting = false;
            _keyValueDB.RevertWritingTransaction(true);
        }
        else if (_writing)
        {
            _keyValueDB.CommitWritingTransaction(currentBtreeRoot!, _temporaryCloseTransactionLog);
            _writing = false;
        }
    }

    public void Dispose()
    {
        if (_writing || _preapprovedWriting)
        {
            _keyValueDB.RevertWritingTransaction(_preapprovedWriting);
            _writing = false;
            _preapprovedWriting = false;
        }

        if (_btreeRoot == null) return;
        _keyValueDB.FinishedUsingBTreeRoot(_btreeRoot);
        _btreeRoot = null;
        GC.SuppressFinalize(this);
    }

    public long GetTransactionNumber()
    {
        return _btreeRoot!.TransactionId;
    }

    public long CursorMovedCounter => _cursorMovedCounter;

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        if (!IsValidKey()) return new KeyValuePair<uint, uint>();
        var nodeIdxPair = _stack[^1];
        var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);

        return new KeyValuePair<uint, uint>(
            (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Length,
            KeyValueDB.CalcValueSize(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize));
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
        return _btreeRoot!.UlongsArray == null ? 0U : (uint)_btreeRoot!.UlongsArray.Length;
    }

    string? _descriptionForLeaks;

    public IKeyValueDB Owner => _keyValueDB;

    public string? DescriptionForLeaks
    {
        get => _descriptionForLeaks;
        set
        {
            _descriptionForLeaks = value;
            if (_preapprovedWriting || _writing) _btreeRoot!.DescriptionForLeaks = value;
        }
    }

    public bool RollbackAdvised { get; set; }

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats()
    {
        var stats = new RefDictionary<(uint Depth, uint Children), uint>();
        _btreeRoot!.CalcBTreeStats(stats, 0);
        return stats.ToDictionary(kv => kv.Key, kv => kv.Value);
    }
}
