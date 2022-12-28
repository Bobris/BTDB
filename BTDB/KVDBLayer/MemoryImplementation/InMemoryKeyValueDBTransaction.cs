using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Collections;
using BTDB.KVDBLayer.BTreeMem;

namespace BTDB.KVDBLayer;

class InMemoryKeyValueDBTransaction : IKeyValueDBTransaction
{
    readonly InMemoryKeyValueDB _keyValueDB;
    IBTreeRootNode? _btreeRoot;
    readonly List<NodeIdxPair> _stack = new List<NodeIdxPair>();
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

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(_stack, out _keyIndex, prefix, (uint)prefix.Length) == FindResult.NotFound)
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

        _btreeRoot!.FillStackByIndex(_stack, _keyIndex);
        return true;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        if (_keyIndex < 0) return FindLastKey(prefix);
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
        if (_keyIndex < 0) return FindFirstKey(prefix);
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
        var ctx = new CreateOrUpdateCtx
        {
            Key = key,
            Value = value,
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
        if (_btreeRoot!.FindKey(_stack, out _keyIndex, prefix, (uint)prefix.Length) == FindResult.NotFound)
        {
            return false;
        }

        index += _keyIndex;
        if (index < _btreeRoot!.CalcKeyCount())
        {
            _btreeRoot!.FillStackByIndex(_stack, index);
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

        _btreeRoot!.FillStackByIndex(_stack, index);
        return true;
    }

    bool CheckPrefixIn(in ReadOnlySpan<byte> prefix, in ReadOnlySpan<byte> key)
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
        // it is always read only memory already
        return GetValue();
    }

    public ReadOnlySpan<byte> GetValue()
    {
        if (!IsValidKey()) return new ();
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx).Span;
    }

    public ReadOnlyMemory<byte> GetValueAsMemory()
    {
        if (!IsValidKey()) return new ();
        var nodeIdxPair = _stack[^1];
        return ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
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
            _btreeRoot!.FillStackByIndex(_stack, _keyIndex);
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
        if (_btreeRoot!.FindKey(_stack, out var keyIndex, exactKey, 0) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            return false;
        }

        MakeWritable();
        InvalidateCurrentKey();
        _btreeRoot!.EraseRange(keyIndex, keyIndex);
        return true;
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey, ref byte buffer, int bufferLength,
        out ReadOnlySpan<byte> value)
    {
        _cursorMovedCounter++;
        if (_btreeRoot!.FindKey(_stack, out var keyIndex, exactKey, 0) != FindResult.Exact)
        {
            InvalidateCurrentKey();
            value = ReadOnlySpan<byte>.Empty;
            return false;
        }

        _keyIndex = 0; // Fake value is enough
        value = GetClonedValue(ref buffer, bufferLength);
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
}
