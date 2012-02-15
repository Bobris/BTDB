using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KV2DBLayer.BTree;

namespace BTDB.KV2DBLayer
{
    public class KeyValue2DB : IKeyValue2DB
    {
        readonly IFileCollection _fileCollection;
        IBTreeRootNode _lastCommited;
        KeyValue2DBTransaction _writingTransaction;

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
    }

    internal class KeyValue2DBTransaction : IKeyValue2DBTransaction
    {
        readonly KeyValue2DB _keyValue2DB;
        readonly IBTreeRootNode _btreeRoot;
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
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
            var nodeAndIdx = _stack[_stack.Count - 1];
            var wholeKey = ((IBTreeLeafNode)nodeAndIdx.Node).GetKey(nodeAndIdx.Idx);
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
            throw new NotImplementedException();
        }

        public void SetValue(ByteBuffer value)
        {
            EnsureValidKey();
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
            if (lastKeyIndex < firstKeyIndex) return;
            throw new NotImplementedException();
        }

        public bool IsWritting()
        {
            return _writting;
        }

        public void Commit()
        {
            throw new NotImplementedException();
        }
    }
}
