using System;
using System.Threading.Tasks;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBReplayProxy : IKeyValueDB
    {
        readonly IKeyValueDB _db;
        readonly AbstractBufferedWriter _log;
        int _trCounter;

        public KeyValueDBReplayProxy(IKeyValueDB db, AbstractBufferedWriter log)
        {
            _db = db;
            _log = log;
        }

        public void Dispose()
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.KeyValueDBDispose);
                _log.FlushBuffer();
            }
            var disposableLog = _log as IDisposable;
            if (disposableLog != null) disposableLog.Dispose();
            _db.Dispose();
        }

        public int CacheSizeInMB
        {
            get { return _db.CacheSizeInMB; }
            set { _db.CacheSizeInMB = value; }
        }

        public bool DurableTransactions
        {
            get { return _db.DurableTransactions; }
            set { _db.DurableTransactions = value; }
        }

        public bool Open(IPositionLessStream positionLessStream, bool dispose)
        {
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.Open);
                ulong size = positionLessStream.GetSize();
                _log.WriteVUInt64(size);
                ulong pos = 0;
                var buf = new byte[4096];
                while (pos < size)
                {
                    var read = positionLessStream.Read(buf, 0, buf.Length, pos);
                    // Next 2 conditions should not happen or file is mutated when it should not
                    if (read == 0) break;
                    if ((ulong)read > size - pos) read = (int)(size - pos);
                    _log.WriteBlock(buf, 0, read);
                    pos += (ulong)read;
                }
                while (pos < size)
                {
                    _log.WriteUInt8(0);
                    pos++;
                }
                _log.FlushBuffer();
            }
            return _db.Open(positionLessStream, dispose);
        }

        public string HumanReadableDescriptionInHeader
        {
            get { return _db.HumanReadableDescriptionInHeader; }
            set
            {
                lock (_log)
                {
                    _log.WriteUInt8((byte)KVReplayOperation.SetHumanReadableDescriptionInHeader);
                    _log.WriteString(value);
                    _log.FlushBuffer();
                }
                _db.HumanReadableDescriptionInHeader = value;
            }
        }

        public IKeyValueDBTransaction StartTransaction()
        {
            var result = new KeyValueDBReplayTransactionProxy(_db.StartTransaction(), _log, ref _trCounter);
            lock (_log)
            {
                _log.WriteUInt8((byte)KVReplayOperation.StartTransaction);
                _log.WriteVUInt32(result.TrIndex);
                _log.FlushBuffer();
            }
            return result;
        }

        public Task<IKeyValueDBTransaction> StartWritingTransaction()
        {
            return _db.StartWritingTransaction().ContinueWith<IKeyValueDBTransaction>(t =>
                {
                    var result = new KeyValueDBReplayTransactionProxy(t.Result, _log, ref _trCounter);
                    lock (_log)
                    {
                        _log.WriteUInt8((byte)KVReplayOperation.StartWritingTransaction);
                        _log.WriteVUInt32(result.TrIndex);
                        _log.FlushBuffer();
                    }
                    return result;
                }, TaskContinuationOptions.ExecuteSynchronously);
        }
    }
}
