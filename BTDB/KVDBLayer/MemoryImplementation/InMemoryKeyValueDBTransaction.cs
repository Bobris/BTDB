using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer.BTreeMem;

namespace BTDB.KVDBLayer
{
    class InMemoryKeyValueDBTransaction : IKeyValueDBTransaction
    {
        readonly InMemoryKeyValueDB _keyValueDB;
        IBTreeRootNode _btreeRoot;
        readonly List<NodeIdxPair> _stack = new List<NodeIdxPair>();
        byte[] _prefix;
        bool _writting;
        readonly bool _readOnly;
        bool _preapprovedWritting;
        long _prefixKeyStart;
        long _prefixKeyCount;
        long _keyIndex;

        public InMemoryKeyValueDBTransaction(InMemoryKeyValueDB keyValueDB, IBTreeRootNode btreeRoot, bool writting, bool readOnly)
        {
            _preapprovedWritting = writting;
            _readOnly = readOnly;
            _keyValueDB = keyValueDB;
            _btreeRoot = btreeRoot;
            _prefix = BitArrayManipulation.EmptyByteArray;
            _prefixKeyStart = 0;
            _prefixKeyCount = -1;
            _keyIndex = -1;
        }

        internal IBTreeRootNode BtreeRoot => _btreeRoot;

        public void SetKeyPrefix(ByteBuffer prefix)
        {
            _prefix = prefix.ToByteArray();
            if (_prefix.Length == 0)
            {
                _prefixKeyStart = 0;
            }
            else
            {
                _prefixKeyStart = -1;
            }
            _prefixKeyCount = -1;
            InvalidateCurrentKey();
        }

        public bool FindFirstKey()
        {
            return SetKeyIndex(0);
        }

        public bool FindLastKey()
        {
            var count = GetKeyValueCount();
            if (count <= 0) return false;
            return SetKeyIndex(count - 1);
        }

        public bool FindPreviousKey()
        {
            if (_keyIndex < 0) return FindLastKey();
            if (BtreeRoot.FindPreviousKey(_stack))
            {
                if (CheckPrefixIn(GetCurrentKeyFromStack()))
                {
                    _keyIndex--;
                    return true;
                }
            }
            InvalidateCurrentKey();
            return false;
        }

        public bool FindNextKey()
        {
            if (_keyIndex < 0) return FindFirstKey();
            if (BtreeRoot.FindNextKey(_stack))
            {
                if (CheckPrefixIn(GetCurrentKeyFromStack()))
                {
                    _keyIndex++;
                    return true;
                }
            }
            InvalidateCurrentKey();
            return false;
        }

        public FindResult Find(ByteBuffer key)
        {
            return BtreeRoot.FindKey(_stack, out _keyIndex, _prefix, key);
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            MakeWrittable();
            var ctx = new CreateOrUpdateCtx
                {
                    KeyPrefix = _prefix,
                    Key = key,
                    Value = value,
                    Stack = _stack
                };
            BtreeRoot.CreateOrUpdate(ctx);
            _keyIndex = ctx.KeyIndex;
            if (ctx.Created && _prefixKeyCount >= 0) _prefixKeyCount++;
            return ctx.Created;
        }

        void MakeWrittable()
        {
            if (_writting) return;
            if (_preapprovedWritting)
            {
                _writting = true;
                _preapprovedWritting = false;
                return;
            }
            if (_readOnly)
            {
                throw new BTDBTransactionRetryException("Cannot write from readOnly transaction");
            }
            var oldBTreeRoot = BtreeRoot;
            _btreeRoot = _keyValueDB.MakeWrittableTransaction(this, oldBTreeRoot);
            _btreeRoot.DescriptionForLeaks = _descriptionForLeaks;
            _writting = true;
            InvalidateCurrentKey();
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount >= 0) return _prefixKeyCount;
            if (_prefix.Length == 0)
            {
                _prefixKeyCount = BtreeRoot.CalcKeyCount();
                return _prefixKeyCount;
            }
            CalcPrefixKeyStart();
            if (_prefixKeyStart < 0)
            {
                _prefixKeyCount = 0;
                return 0;
            }
            _prefixKeyCount = BtreeRoot.FindLastWithPrefix(_prefix) - _prefixKeyStart + 1;
            return _prefixKeyCount;
        }

        public long GetKeyIndex()
        {
            if (_keyIndex < 0) return -1;
            CalcPrefixKeyStart();
            return _keyIndex - _prefixKeyStart;
        }

        void CalcPrefixKeyStart()
        {
            if (_prefixKeyStart >= 0) return;
            if (BtreeRoot.FindKey(new List<NodeIdxPair>(), out _prefixKeyStart, _prefix, ByteBuffer.NewEmpty()) == FindResult.NotFound)
            {
                _prefixKeyStart = -1;
            }
        }

        public bool SetKeyIndex(long index)
        {
            CalcPrefixKeyStart();
            if (_prefixKeyStart < 0)
            {
                InvalidateCurrentKey();
                return false;
            }
            _keyIndex = index + _prefixKeyStart;
            if (_keyIndex >= BtreeRoot.CalcKeyCount())
            {
                InvalidateCurrentKey();
                return false;
            }
            BtreeRoot.FillStackByIndex(_stack, _keyIndex);
            if (_prefixKeyCount >= 0)
                return true;
            var key = GetCurrentKeyFromStack();
            if (CheckPrefixIn(key))
            {
                return true;
            }
            InvalidateCurrentKey();
            return false;
        }

        bool CheckPrefixIn(ByteBuffer key)
        {
            return BTreeRoot.KeyStartsWithPrefix(_prefix, key);
        }

        ByteBuffer GetCurrentKeyFromStack()
        {
            var nodeIdxPair = _stack[_stack.Count - 1];
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

        public ByteBuffer GetKey()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var wholeKey = GetCurrentKeyFromStack();
            return ByteBuffer.NewAsync(wholeKey.Buffer, wholeKey.Offset + _prefix.Length, wholeKey.Length - _prefix.Length);
        }

        public ByteBuffer GetValue()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var nodeIdxPair = _stack[_stack.Count - 1];
            return ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
        }

        void EnsureValidKey()
        {
            if (_keyIndex < 0)
            {
                throw new InvalidOperationException("Current key is not valid");
            }
        }

        public void SetValue(ByteBuffer value)
        {
            EnsureValidKey();
            var keyIndexBackup = _keyIndex;
            MakeWrittable();
            if (_keyIndex != keyIndexBackup)
            {
                _keyIndex = keyIndexBackup;
                BtreeRoot.FillStackByIndex(_stack, _keyIndex);
            }
            var nodeIdxPair = _stack[_stack.Count - 1];
            ((IBTreeLeafNode)nodeIdxPair.Node).SetMemberValue(nodeIdxPair.Idx, value);
        }

        public void EraseCurrent()
        {
            EnsureValidKey();
            var keyIndex = _keyIndex;
            MakeWrittable();
            InvalidateCurrentKey();
            _prefixKeyCount--;
            BtreeRoot.EraseRange(keyIndex, keyIndex);
        }

        public void EraseAll()
        {
            EraseRange(0, long.MaxValue);
        }

        public void EraseRange(long firstKeyIndex, long lastKeyIndex)
        {
            if (firstKeyIndex < 0) firstKeyIndex = 0;
            if (lastKeyIndex >= GetKeyValueCount()) lastKeyIndex = _prefixKeyCount - 1;
            if (lastKeyIndex < firstKeyIndex) return;
            MakeWrittable();
            firstKeyIndex += _prefixKeyStart;
            lastKeyIndex += _prefixKeyStart;
            InvalidateCurrentKey();
            _prefixKeyCount -= lastKeyIndex - firstKeyIndex + 1;
            BtreeRoot.EraseRange(firstKeyIndex, lastKeyIndex);
        }

        public bool IsWritting()
        {
            return _writting;
        }

        public ulong GetCommitUlong()
        {
            return BtreeRoot.CommitUlong;
        }

        public void SetCommitUlong(ulong value)
        {
            if (BtreeRoot.CommitUlong != value)
            {
                MakeWrittable();
                BtreeRoot.CommitUlong = value;
            }
        }

        public void NextCommitTemporaryCloseTransactionLog()
        {
            // There is no transaction log ...
        }

        public void Commit()
        {
            if (BtreeRoot == null) throw new BTDBException("Transaction already commited or disposed");
            InvalidateCurrentKey();
            var currentBtreeRoot = _btreeRoot;
            _btreeRoot = null;
            if (_preapprovedWritting)
            {
                _preapprovedWritting = false;
                _keyValueDB.RevertWrittingTransaction();
            }
            else if (_writting)
            {
                _keyValueDB.CommitWrittingTransaction(currentBtreeRoot);
                _writting = false;
            }
        }

        public void Dispose()
        {
            if (_writting || _preapprovedWritting)
            {
                _keyValueDB.RevertWrittingTransaction();
                _writting = false;
                _preapprovedWritting = false;
            }
            _btreeRoot = null;
        }

        public long GetTransactionNumber()
        {
            return _btreeRoot.TransactionId;
        }

        public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
        {
            var nodeIdxPair = _stack[_stack.Count - 1];
            return new KeyValuePair<uint, uint>(
                (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Length,
                (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx).Length);
        }

        public byte[] GetKeyPrefix()
        {
            return _prefix;
        }

        string _descriptionForLeaks;
        public string DescriptionForLeaks
        {
            get { return _descriptionForLeaks; }
            set
            {
                _descriptionForLeaks = value;
                if (_preapprovedWritting || _writting) _btreeRoot.DescriptionForLeaks = value;
            }
        }
    }
}