using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KV2DBLayer.BTree;
using BTDB.KVDBLayer;

namespace BTDB.KV2DBLayer
{
    public class KeyValue2DB : IKeyValue2DB
    {
        readonly IFileCollection _fileCollection;
        IBTreeRootNode _lastCommited;
        KeyValue2DBTransaction _writingTransaction;
        readonly object _writeLock = new object();



        public KeyValue2DB(IFileCollection fileCollection)
        {
            _fileCollection = fileCollection;
        }

        public void Dispose()
        {
        }

        public IKeyValue2DBTransaction StartTransaction()
        {
            return new KeyValue2DBTransaction(this, _lastCommited);
        }

        public Task<IKeyValue2DBTransaction> StartWritingTransaction()
        {
            throw new NotImplementedException();
        }

        internal IBTreeRootNode MakeWrittableTransaction(KeyValue2DBTransaction keyValue2DBTransaction, IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBTransactionRetryException("Another writting transaction already running");
                if (_lastCommited != btreeRoot) throw new BTDBTransactionRetryException("Another writting transaction already finished");
                _writingTransaction = keyValue2DBTransaction;
                return btreeRoot.NewTransactionRoot();
            }
        }

        internal void CommitWrittingTransaction(KeyValue2DBTransaction keyValue2DBTransaction, IBTreeRootNode btreeRoot)
        {
            Debug.Assert(_writingTransaction == keyValue2DBTransaction);
            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = btreeRoot;
            }
        }

        internal void RevertWrittingTransaction(KeyValue2DBTransaction keyValue2DBTransaction)
        {
            Debug.Assert(_writingTransaction == keyValue2DBTransaction);
            lock (_writeLock)
            {
                _writingTransaction = null;
            }
        }
    }

    internal class KeyValue2DBTransaction : IKeyValue2DBTransaction
    {
        readonly KeyValue2DB _keyValue2DB;
        IBTreeRootNode _btreeRoot;
        List<NodeIdxPair> _stack = new List<NodeIdxPair>();
        byte[] _prefix;
        bool _writting;
        long _prefixKeyStart;
        long _prefixKeyCount;
        long _keyIndex;

        public KeyValue2DBTransaction(KeyValue2DB keyValue2DB, IBTreeRootNode btreeRoot)
        {
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
            throw new NotImplementedException();
        }

        public bool FindPreviousKey()
        {
            throw new NotImplementedException();
        }

        public bool FindNextKey()
        {
            throw new NotImplementedException();
        }

        public FindResult Find(ByteBuffer key)
        {
            return _btreeRoot.FindKey(_stack, out _keyIndex, _prefix, key);
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            MakeWrittable();
            var ctx = new CreateOrUpdateCtx();
            ctx.KeyPrefix = _prefix;
            ctx.Key = key;
            ctx.ValueFileId = 0;
            ctx.ValueOfs = 0;
            ctx.ValueSize = value.Length;
            ctx.Stack = _stack;
            _btreeRoot.CreateOrUpdate(ctx);
            _keyIndex = ctx.KeyIndex;
            return ctx.Created;
        }

        void MakeWrittable()
        {
            if (_writting) return;
            _btreeRoot = _keyValue2DB.MakeWrittableTransaction(this, _btreeRoot);
            _writting = true;
            InvalidateCurrentKey();
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount >= 0) return _prefixKeyCount;
            if (_prefix.Length == 0)
            {
                _prefixKeyCount = _btreeRoot.CalcKeyCount();
                return _prefixKeyCount;
            }
            throw new NotImplementedException();
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
            ((IBTreeLeafNode)nodeIdxPair.Node).GetMember(nodeIdxPair.Idx);
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
            if (!_writting) return;
            InvalidateCurrentKey();
            _keyValue2DB.CommitWrittingTransaction(this, _btreeRoot);
            _writting = false;
            _btreeRoot = null;
        }

        public void Dispose()
        {
            if (_writting)
            {
                _keyValue2DB.RevertWrittingTransaction(this);
                _writting = false;
            }
            _btreeRoot = null;
        }
    }
}
