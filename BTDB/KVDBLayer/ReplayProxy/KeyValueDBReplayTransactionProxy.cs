using BTDB.KVDBLayer.Interface;
using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.KVDBLayer.ReplayProxy
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

        public void SetKeyPrefix(byte[] prefix, int prefixOfs, int prefixLen)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.SetKeyPrefix);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(prefixLen);
                _log.WriteVInt32(prefixOfs);
                _log.WriteBlock(prefix, prefixOfs, prefixLen);
                _log.FlushBuffer();
            }
            _tr.SetKeyPrefix(prefix, prefixOfs, prefixLen);
        }

        public void InvalidateCurrentKey()
        {
            LogSimpleOperation(KVReplayOperation.InvalidateCurrentKey);
            _tr.InvalidateCurrentKey();
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

        public FindKeyResult FindKey(byte[] keyBuf, int keyOfs, int keyLen, FindKeyStrategy strategy)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.FindKey);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(keyLen);
                _log.WriteVInt32(keyOfs);
                _log.WriteBlock(keyBuf, keyOfs, keyLen);
                _log.WriteVUInt32((uint)strategy);
                _log.FlushBuffer();
            }
            return _tr.FindKey(keyBuf, keyOfs, keyLen, strategy);
        }

        public bool CreateOrUpdateKeyValue(byte[] keyBuf, int keyOfs, int keyLen, byte[] valueBuf, int valueOfs, int valueLen)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.CreateOrUpdateKeyValue);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(keyLen);
                _log.WriteVInt32(keyOfs);
                _log.WriteBlock(keyBuf, keyOfs, keyLen);
                _log.WriteVInt32(valueLen);
                _log.WriteVInt32(valueOfs);
                _log.WriteBlock(valueBuf, valueOfs, valueLen);
                _log.FlushBuffer();
            }
            return _tr.CreateOrUpdateKeyValue(keyBuf, keyOfs, keyLen, valueBuf, valueOfs, valueLen);
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

        public int GetKeySize()
        {
            LogSimpleOperation(KVReplayOperation.GetKeySize);
            return _tr.GetKeySize();
        }

        public long GetValueSize()
        {
            LogSimpleOperation(KVReplayOperation.GetValueSize);
            return _tr.GetValueSize();
        }

        public void PeekKey(int ofs, out int len, out byte[] buf, out int bufOfs)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.PeekKey);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(ofs);
                _log.FlushBuffer();
            }
            _tr.PeekKey(ofs, out len, out buf, out bufOfs);
        }

        public void ReadKey(int ofs, int len, byte[] buf, int bufOfs)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.ReadKey);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(ofs);
                _log.WriteVInt32(len);
                _log.WriteVInt32(bufOfs);
                _log.FlushBuffer();
            }
            _tr.ReadKey(ofs, len, buf, bufOfs);
        }

        public void PeekValue(long ofs, out int len, out byte[] buf, out int bufOfs)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.PeekValue);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt64(ofs);
                _log.FlushBuffer();
            }
            _tr.PeekValue(ofs, out len, out buf, out bufOfs);
        }

        public void ReadValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.ReadValue);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt64(ofs);
                _log.WriteVInt32(len);
                _log.WriteVInt32(bufOfs);
                _log.FlushBuffer();
            }
            _tr.ReadValue(ofs, len, buf, bufOfs);
        }

        public void WriteValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.WriteValue);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt64(ofs);
                _log.WriteVInt32(bufOfs);
                _log.WriteVInt32(len);
                _log.WriteBlock(buf, bufOfs, len);
                _log.FlushBuffer();
            }
            _tr.WriteValue(ofs, len, buf, bufOfs);
        }

        public void SetValueSize(long newSize)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.SetValueSize);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt64(newSize);
                _log.FlushBuffer();
            }
            _tr.SetValueSize(newSize);
        }

        public void SetValue(byte[] buf, int bufOfs, int len)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.SetValue);
                _log.WriteVUInt32(TrIndex);
                _log.WriteVInt32(bufOfs);
                _log.WriteVInt32(len);
                _log.WriteBlock(buf, bufOfs, len);
                _log.FlushBuffer();
            }
            _tr.SetValue(buf, bufOfs, len);
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

        public KeyValueDBStats CalculateStats()
        {
            LogSimpleOperation(KVReplayOperation.CalculateStats);
            return _tr.CalculateStats();
        }
    }
}