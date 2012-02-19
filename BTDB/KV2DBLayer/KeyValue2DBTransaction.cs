using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KV2DBLayer.BTree;
using BTDB.KVDBLayer;

namespace BTDB.KV2DBLayer
{
    internal class KeyValue2DBTransaction : IKeyValue2DBTransaction
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
        }

        public void SetKeyPrefix(ByteBuffer prefix)
        {
            _prefix = prefix.ToByteArray();
            _prefixKeyStart = -1;
            _prefixKeyCount = -1;
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
            throw new NotImplementedException();
        }

        public bool FindNextKey()
        {
            if (_keyIndex < 0) return FindFirstKey();
            throw new NotImplementedException();
        }

        public FindResult Find(ByteBuffer key)
        {
            return _btreeRoot.FindKey(_stack, out _keyIndex, _prefix, key);
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            MakeWrittable();
            int valueFileId;
            int valueOfs;
            int valueSize;
            _keyValue2DB.WriteCreateOrUpdateCommand(key, value, out valueFileId, out valueOfs, out valueSize);
            var ctx = new CreateOrUpdateCtx
                {
                    KeyPrefix = _prefix,
                    Key = key,
                    ValueFileId = valueFileId,
                    ValueOfs = valueOfs,
                    ValueSize = valueSize,
                    Stack = _stack
                };
            _btreeRoot.CreateOrUpdate(ctx);
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
                _keyValue2DB.WriteStartTransaction(_btreeRoot.TransactionId);
                return;
            }
            _btreeRoot = _keyValue2DB.MakeWrittableTransaction(this, _btreeRoot);
            _writting = true;
            InvalidateCurrentKey();
            _keyValue2DB.WriteStartTransaction(_btreeRoot.TransactionId);
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount >= 0) return _prefixKeyCount;
            if (_prefix.Length == 0)
            {
                _prefixKeyCount = _btreeRoot.CalcKeyCount();
                return _prefixKeyCount;
            }
            CalcPrefixKeyStart();
            if (_prefixKeyStart < 0)
            {
                _prefixKeyCount = 0;
                return 0;
            }
            _prefixKeyCount = _btreeRoot.FindLastWithPrefix(_prefix) - _prefixKeyStart + 1;
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
            if (_prefix.Length == 0)
            {
                _prefixKeyStart = 0;
                return;
            }
            if (_btreeRoot.FindKey(new List<NodeIdxPair>(), out _prefixKeyStart, _prefix, ByteBuffer.NewEmpty()) == FindResult.NotFound)
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
            if (_keyIndex >= _btreeRoot.CalcKeyCount())
            {
                InvalidateCurrentKey();
                return false;
            }
            _btreeRoot.FillStackByIndex(_stack, _keyIndex);
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
            return BitArrayManipulation.CompareByteArray(
                key, 0, Math.Min(key.Length, _prefix.Length),
                _prefix, 0, _prefix.Length) == 0;
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
            EnsureValidKey();
            var wholeKey = GetCurrentKeyFromStack();
            return ByteBuffer.NewAsync(wholeKey, _prefix.Length, wholeKey.Length - _prefix.Length);
        }

        void EnsureValidKey()
        {
            if (_keyIndex < 0)
            {
                throw new InvalidOperationException("Current key is not valid");
            }
        }

        public ByteBuffer GetValue()
        {
            EnsureValidKey();
            var nodeIdxPair = _stack[_stack.Count - 1];
            var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMember(nodeIdxPair.Idx);
            return _keyValue2DB.ReadValue(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize);
        }

        public void SetValue(ByteBuffer value)
        {
            EnsureValidKey();
            var keyIndexBackup = _keyIndex;
            MakeWrittable();
            if (_keyIndex != keyIndexBackup)
            {
                _btreeRoot.FillStackByIndex(_stack, _keyIndex);
            }
            var nodeIdxPair = _stack[_stack.Count - 1];
            var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMember(nodeIdxPair.Idx);
            _keyValue2DB.WriteCreateOrUpdateCommand(ByteBuffer.NewAsync(leafMember.Key),value,out leafMember.ValueFileId, out leafMember.ValueOfs, out leafMember.ValueSize);
            ((IBTreeLeafNode) nodeIdxPair.Node).SetMember(nodeIdxPair.Idx, leafMember);
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

            throw new NotImplementedException();
        }

        public bool IsWritting()
        {
            return _writting;
        }

        public void Commit()
        {
            if (_btreeRoot == null) throw new BTDBException("Transaction already commited or disposed");
            InvalidateCurrentKey();
            if (_preapprovedWritting)
            {
                _preapprovedWritting = false;
                _keyValue2DB.RevertWrittingTransaction(true);
            }
            else if (_writting)
            {
                _keyValue2DB.CommitWrittingTransaction(_btreeRoot);
                _writting = false;
            }
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
            _btreeRoot = null;
        }
    }
}
