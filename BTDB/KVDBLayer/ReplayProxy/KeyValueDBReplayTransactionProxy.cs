using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    internal class KeyValueDBReplayTransactionProxy : IKeyValueDBTransaction
    {
        readonly IKeyValueDBTransaction _tr;
        readonly AbstractBufferedWriter _log;
        readonly uint _trIndex;

        public KeyValueDBReplayTransactionProxy(IKeyValueDBTransaction tr, AbstractBufferedWriter log, ref int trCounter)
        {
            _tr = tr;
            _log = log;
            _trIndex = (uint)System.Threading.Interlocked.Increment(ref trCounter);
        }

        internal uint TrIndex
        {
            get { return _trIndex; }
        }

        void LogSimpleOperation(KVReplayOperation operation)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)operation);
                _log.WriteVUInt32(TrIndex);
                _log.FlushBuffer();
            }
        }

        public void Dispose()
        {
            LogSimpleOperation(KVReplayOperation.TransactionDispose);
            _tr.Dispose();
        }

        public bool IsWritting()
        {
            // This is so simple method that it does not need to be logged
            return _tr.IsWritting();
        }

        public void InvalidateCurrentKey()
        {
            LogSimpleOperation(KVReplayOperation.InvalidateCurrentKey);
            _tr.InvalidateCurrentKey();
        }

        public bool IsValidKey()
        {
            // This is so simple method that it does not need to be logged
            return _tr.IsValidKey();
        }

        public ByteBuffer GetKey()
        {
            LogSimpleOperation(KVReplayOperation.GetKey);
            return _tr.GetKey();
        }

        public ByteBuffer GetValue()
        {
            LogSimpleOperation(KVReplayOperation.GetValue);
            return _tr.GetValue();
        }

        public void SetValue(ByteBuffer value)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.SetValue);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(value.Length);
                _log.WriteVInt32(value.Offset);
                _log.WriteBlock(value);
                _log.FlushBuffer();
            }
            _tr.SetValue(value);
        }

        public void SetKeyPrefix(ByteBuffer prefix)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.SetKeyPrefix);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(prefix.Length);
                _log.WriteVInt32(prefix.Offset);
                _log.WriteBlock(prefix);
                _log.FlushBuffer();
            }
            _tr.SetKeyPrefix(prefix);
        }

        public bool FindFirstKey()
        {
            LogSimpleOperation(KVReplayOperation.FindFirstKey);
            return _tr.FindFirstKey();
        }

        public bool FindLastKey()
        {
            LogSimpleOperation(KVReplayOperation.FindLastKey);
            return _tr.FindLastKey();
        }

        public bool FindPreviousKey()
        {
            LogSimpleOperation(KVReplayOperation.FindPreviousKey);
            return _tr.FindPreviousKey();
        }

        public bool FindNextKey()
        {
            LogSimpleOperation(KVReplayOperation.FindNextKey);
            return _tr.FindNextKey();
        }

        public FindResult Find(ByteBuffer key)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.FindKey);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(key.Length);
                _log.WriteVInt32(key.Offset);
                _log.WriteBlock(key.Buffer, key.Offset, key.Length);
                _log.FlushBuffer();
            }
            return _tr.Find(key);
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.CreateOrUpdateKeyValue);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(key.Length);
                _log.WriteVInt32(key.Offset);
                _log.WriteBlock(key);
                _log.WriteVInt32(value.Length);
                _log.WriteVInt32(value.Offset);
                _log.WriteBlock(value);
                _log.FlushBuffer();
            }
            return _tr.CreateOrUpdateKeyValue(key,value);
        }

        public long GetKeyValueCount()
        {
            LogSimpleOperation(KVReplayOperation.GetKeyValueCount);
            return _tr.GetKeyValueCount();
        }

        public long GetKeyIndex()
        {
            LogSimpleOperation(KVReplayOperation.GetKeyIndex);
            return _tr.GetKeyIndex();
        }

        public bool SetKeyIndex(long index)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.SetKeyIndex);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt64(index);
                _log.FlushBuffer();
            }
            return _tr.SetKeyIndex(index);
        }

        public void EraseCurrent()
        {
            LogSimpleOperation(KVReplayOperation.EraseCurrent);
            _tr.EraseCurrent();
        }

        public void EraseAll()
        {
            LogSimpleOperation(KVReplayOperation.EraseAll);
            _tr.EraseAll();
        }

        public void EraseRange(long firstKeyIndex, long lastKeyIndex)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.EraseRange);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt64(firstKeyIndex);
                _log.WriteVInt64(lastKeyIndex);
                _log.FlushBuffer();
            }
            _tr.EraseRange(firstKeyIndex, lastKeyIndex);
        }

        public void Commit()
        {
            LogSimpleOperation(KVReplayOperation.Commit);
            _tr.Commit();
        }
    }
}