using System;
using System.Threading.Tasks;
using BTDB.Buffer;

namespace BTDB.KV2DBLayer
{
    public class KeyValue2DB : IKeyValue2DB
    {
        readonly IFileCollection _fileCollection;

        public KeyValue2DB(IFileCollection fileCollection)
        {
            _fileCollection = fileCollection;
        }

        public void Dispose()
        {
        }

        public IKeyValue2DBTransaction StartTransaction()
        {
            return new KeyValue2DBTransaction(this);
        }

        public Task<IKeyValue2DBTransaction> StartWritingTransaction()
        {
            throw new NotImplementedException();
        }
    }

    internal class KeyValue2DBTransaction : IKeyValue2DBTransaction
    {
        readonly KeyValue2DB _keyValue2DB;
        byte[] _prefix;
        bool _writting;
        long _prefixKeyStart;
        long _prefixKeyCount;
        long _keyIndex;

        public KeyValue2DBTransaction(KeyValue2DB keyValue2DB)
        {
            _keyValue2DB = keyValue2DB;
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
            throw new NotImplementedException();
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            throw new NotImplementedException();
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount >= 0) return _prefixKeyCount;
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
            throw new NotImplementedException();
        }

        public bool SetKeyIndex(long index)
        {
            CalcPrefixKeyStart();
            _keyIndex = index + _prefixKeyStart;
            throw new NotImplementedException();
        }

        public void InvalidateCurrentKey()
        {
            _keyIndex = -1;
        }

        public ByteBuffer GetKey()
        {
            throw new NotImplementedException();
        }

        public ByteBuffer GetValue()
        {
            throw new NotImplementedException();
        }

        public void SetValue(ByteBuffer value)
        {
            throw new NotImplementedException();
        }

        public void EraseCurrent()
        {
            if (_keyIndex < 0) throw new InvalidOperationException("Current key is invalid, cannot be erased");
            EraseRange(GetKeyIndex(), GetKeyIndex());
        }

        public void EraseAll()
        {
            EraseRange(0, long.MaxValue);
        }

        public void EraseRange(long firstKeyIndex, long lastKeyIndex)
        {
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
