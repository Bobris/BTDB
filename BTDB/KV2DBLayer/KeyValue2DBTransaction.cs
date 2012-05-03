using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KV2DBLayer.BTree;
using BTDB.KVDBLayer;

namespace BTDB.KV2DBLayer
{
    internal class KeyValue2DBTransaction : IKeyValueDBTransaction
    {
        readonly KeyValue2DB _keyValue2DB;
        IBTreeRootNode _btreeRoot;
        readonly List<NodeIdxPair> _stack = new List<NodeIdxPair>();
        byte[] _prefix;
        bool _writting;
        bool _preapprovedWritting;
        long _prefixKeyStart;
        long _prefixKeyCount;
        long _keyIndex;

        public KeyValue2DBTransaction(KeyValue2DB keyValue2DB, IBTreeRootNode btreeRoot, bool writting)
        {
            _preapprovedWritting = writting;
            _keyValue2DB = keyValue2DB;
            _btreeRoot = btreeRoot;
            _prefix = BitArrayManipulation.EmptyByteArray;
            _prefixKeyStart = 0;
            _prefixKeyCount = -1;
            _keyIndex = -1;
            _keyValue2DB.StartedUsingBTreeRoot(_btreeRoot);
        }

        internal IBTreeRootNode BtreeRoot
        {
            get { return _btreeRoot; }
        }

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
            uint valueFileId;
            uint valueOfs;
            int valueSize;
            _keyValue2DB.WriteCreateOrUpdateCommand(_prefix, key, value, out valueFileId, out valueOfs, out valueSize);
            var ctx = new CreateOrUpdateCtx
                {
                    KeyPrefix = _prefix,
                    Key = key,
                    ValueFileId = valueFileId,
                    ValueOfs = valueOfs,
                    ValueSize = valueSize,
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
                _keyValue2DB.WriteStartTransaction();
                return;
            }
            var oldBTreeRoot = BtreeRoot;
            _btreeRoot = _keyValue2DB.MakeWrittableTransaction(this, oldBTreeRoot);
            _keyValue2DB.StartedUsingBTreeRoot(_btreeRoot);
            _keyValue2DB.FinishedUsingBTreeRoot(oldBTreeRoot);
            _writting = true;
            InvalidateCurrentKey();
            _keyValue2DB.WriteStartTransaction();
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

        bool CheckPrefixIn(byte[] key)
        {
            return BTreeRoot.KeyStartsWithPrefix(_prefix, key);
        }

        byte[] GetCurrentKeyFromStack()
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
            return ByteBuffer.NewAsync(wholeKey, _prefix.Length, wholeKey.Length - _prefix.Length);
        }

        public ByteBuffer GetValue()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var nodeIdxPair = _stack[_stack.Count - 1];
            var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMember(nodeIdxPair.Idx);
            return _keyValue2DB.ReadValue(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize);
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
            var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMember(nodeIdxPair.Idx);
            _keyValue2DB.WriteCreateOrUpdateCommand(BitArrayManipulation.EmptyByteArray, ByteBuffer.NewAsync(leafMember.Key), value, out leafMember.ValueFileId, out leafMember.ValueOfs, out leafMember.ValueSize);
            ((IBTreeLeafNode)nodeIdxPair.Node).SetMember(nodeIdxPair.Idx, leafMember);
        }

        public void EraseCurrent()
        {
            EnsureValidKey();
            EraseRange(GetKeyIndex(), GetKeyIndex());
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
            BtreeRoot.FillStackByIndex(_stack, firstKeyIndex);
            if (firstKeyIndex == lastKeyIndex)
            {
                _keyValue2DB.WriteEraseOneCommand(GetCurrentKeyFromStack());
            }
            else
            {
                var firstKey = GetCurrentKeyFromStack();
                BtreeRoot.FillStackByIndex(_stack, lastKeyIndex);
                _keyValue2DB.WriteEraseRangeCommand(firstKey, GetCurrentKeyFromStack());
            }
            BtreeRoot.EraseRange(firstKeyIndex, lastKeyIndex);
        }

        public bool IsWritting()
        {
            return _writting;
        }

        public void Commit()
        {
            if (BtreeRoot == null) throw new BTDBException("Transaction already commited or disposed");
            InvalidateCurrentKey();
            if (_preapprovedWritting)
            {
                _preapprovedWritting = false;
                _keyValue2DB.RevertWrittingTransaction(true);
            }
            else if (_writting)
            {
                _keyValue2DB.CommitWrittingTransaction(BtreeRoot);
                _writting = false;
            }
            _keyValue2DB.FinishedUsingBTreeRoot(_btreeRoot);
            _btreeRoot = null;
        }

        public void Dispose()
        {
            if (_writting || _preapprovedWritting)
            {
                _keyValue2DB.RevertWrittingTransaction(_preapprovedWritting);
                _writting = false;
                _preapprovedWritting = false;
            }
            if (_btreeRoot == null) return;
            _keyValue2DB.FinishedUsingBTreeRoot(_btreeRoot);
            _btreeRoot = null;
        }
    }
}
