using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer.BTreeMem;

namespace BTDB.KVDBLayer
{
    class InMemoryKeyValueDBTransaction : IKeyValueDBTransaction
    {
        readonly InMemoryKeyValueDB _keyValueDB;
        IBTreeRootNode? _btreeRoot;
        readonly List<NodeIdxPair> _stack = new List<NodeIdxPair>();
        bool _writing;
        readonly bool _readOnly;
        bool _preapprovedWriting;
        long _keyIndex;

        public InMemoryKeyValueDBTransaction(InMemoryKeyValueDB keyValueDB, IBTreeRootNode btreeRoot, bool writing, bool readOnly)
        {
            _preapprovedWriting = writing;
            _readOnly = readOnly;
            _keyValueDB = keyValueDB;
            _btreeRoot = btreeRoot;
            _keyIndex = -1;
        }

        internal IBTreeRootNode BtreeRoot => _btreeRoot;

        public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
        {
            if (BtreeRoot.FindKey(_stack, out _keyIndex, prefix, (uint) prefix.Length) == FindResult.NotFound)
            {
                return false;
            }

            return true;
        }

        public bool FindLastKey(in ReadOnlySpan<byte> prefix)
        {
            _keyIndex = BtreeRoot.FindLastWithPrefix(prefix);
            if (_keyIndex == -1)
            {
                _stack.Clear();
                return false;
            }

            BtreeRoot.FillStackByIndex(_stack, _keyIndex);
            return true;
        }

        public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
        {
            if (_keyIndex < 0) return FindLastKey(prefix);
            if (BtreeRoot.FindPreviousKey(_stack))
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
            if (BtreeRoot.FindNextKey(_stack))
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
            return BtreeRoot.FindKey(_stack, out _keyIndex, key, prefixLen);
        }

        public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            MakeWritable();
            var ctx = new CreateOrUpdateCtx
                {
                    Key = key,
                    Value = value,
                    Stack = _stack
                };
            BtreeRoot.CreateOrUpdate(ref ctx);
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
            var oldBTreeRoot = BtreeRoot;
            _btreeRoot = _keyValueDB.MakeWritableTransaction(this, oldBTreeRoot);
            _btreeRoot.DescriptionForLeaks = _descriptionForLeaks;
            _writing = true;
            InvalidateCurrentKey();
        }

        public long GetKeyValueCount()
        {
            return BtreeRoot.CalcKeyCount();
        }

        public long GetKeyIndex()
        {
            if (_keyIndex < 0) return -1;
            return _keyIndex;
        }

        public bool SetKeyIndex(in ReadOnlySpan<byte> prefix, long index)
        {
            if (BtreeRoot.FindKey(_stack, out _keyIndex, prefix, (uint) prefix.Length) == FindResult.NotFound)
            {
                return false;
            }

            index += _keyIndex;
            if (index < BtreeRoot.CalcKeyCount())
            {
                BtreeRoot.FillStackByIndex(_stack, index);
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
            _keyIndex = index;
            if (index < 0 || index >= BtreeRoot.CalcKeyCount())
            {
                InvalidateCurrentKey();
                return false;
            }
            BtreeRoot.FillStackByIndex(_stack, index);
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
            _keyIndex = -1;
            _stack.Clear();
        }

        public bool IsValidKey()
        {
            return _keyIndex >= 0;
        }

        public ReadOnlySpan<byte> GetKeyAsReadOnlySpan()
        {
            if (!IsValidKey()) return new ReadOnlySpan<byte>();
            return GetCurrentKeyFromStack();
        }

        public ByteBuffer GetKeyIncludingPrefix()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            return ByteBuffer.NewAsync(GetCurrentKeyFromStack());
        }

        public ByteBuffer GetValue()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var nodeIdxPair = _stack[^1];
            return ByteBuffer.NewAsync(((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx));
        }

        public ReadOnlySpan<byte> GetValueAsReadOnlySpan()
        {
            if (!IsValidKey()) return new ReadOnlySpan<byte>();
            var nodeIdxPair = _stack[^1];
            return ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
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
                BtreeRoot.FillStackByIndex(_stack, _keyIndex);
            }
            var nodeIdxPair = _stack[^1];
            ((IBTreeLeafNode)nodeIdxPair.Node).SetMemberValue(nodeIdxPair.Idx, value);
        }

        public void EraseCurrent()
        {
            EnsureValidKey();
            var keyIndex = _keyIndex;
            MakeWritable();
            InvalidateCurrentKey();
            BtreeRoot.EraseRange(keyIndex, keyIndex);
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
            MakeWritable();
            InvalidateCurrentKey();
            BtreeRoot.EraseRange(firstKeyIndex, lastKeyIndex);
        }

        public bool IsWriting()
        {
            return _writing || _preapprovedWriting;
        }

        public bool IsReadOnly()
        {
            return _readOnly;
        }

        public ulong GetCommitUlong()
        {
            return BtreeRoot.CommitUlong;
        }

        public void SetCommitUlong(ulong value)
        {
            if (BtreeRoot.CommitUlong != value)
            {
                MakeWritable();
                BtreeRoot.CommitUlong = value;
            }
        }

        public void NextCommitTemporaryCloseTransactionLog()
        {
            // There is no transaction log ...
        }

        public void Commit()
        {
            if (BtreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
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
                _keyValueDB.CommitWritingTransaction(currentBtreeRoot);
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
            return _btreeRoot.TransactionId;
        }

        public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
        {
            var nodeIdxPair = _stack[^1];
            return new KeyValuePair<uint, uint>(
                (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Length,
                (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx).Length);
        }

        public ulong GetUlong(uint idx)
        {
            return BtreeRoot.GetUlong(idx);
        }

        public void SetUlong(uint idx, ulong value)
        {
            if (BtreeRoot.GetUlong(idx) != value)
            {
                MakeWritable();
                BtreeRoot.SetUlong(idx, value);
            }
        }

        public uint GetUlongCount()
        {
            return BtreeRoot.GetUlongCount();
        }

        string? _descriptionForLeaks;

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
    }
}
