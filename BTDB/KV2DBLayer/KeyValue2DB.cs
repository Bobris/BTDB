using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KV2DBLayer.BTree;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class KeyValue2DB : IKeyValue2DB
    {
        readonly IFileCollection _fileCollection;
        IBTreeRootNode _lastCommited;
        KeyValue2DBTransaction _writingTransaction;
        readonly Queue<TaskCompletionSource<IKeyValue2DBTransaction>> _writeWaitingQueue = new Queue<TaskCompletionSource<IKeyValue2DBTransaction>>();
        readonly object _writeLock = new object();
        int _fileIdWithTransactionLog;
        int _fileIdWithTransactionLogStartOfTr;
        long _positionOfStartOfTr;
        IPositionLessStream _fileWithTransactionLog;
        AbstractBufferedWriter _writerWithTransactionLog;
        readonly byte[] _magicStartOfFile = new[] { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };
        readonly byte[] _magicStartOfTransaction = new[] { (byte)'t', (byte)'R', (byte)'s', (byte)'T' };
        const long MaxTrLogFileSize = 2000000000;

        public KeyValue2DB(IFileCollection fileCollection)
        {
            _fileCollection = fileCollection;
            _lastCommited = new BTreeRoot(0);
            _fileIdWithTransactionLog = -1;
        }

        public void Dispose()
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBException("Cannot dispose KeyValue2DB when writting transaction still running");
                while (_writeWaitingQueue.Count > 0)
                {
                    _writeWaitingQueue.Dequeue().TrySetCanceled();
                }
            }
        }

        public IKeyValue2DBTransaction StartTransaction()
        {
            return new KeyValue2DBTransaction(this, _lastCommited, false);
        }

        public Task<IKeyValue2DBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {
                var tcs = new TaskCompletionSource<IKeyValue2DBTransaction>();
                if (_writingTransaction == null)
                {
                    NewWrittingTransactionUnsafe(tcs);
                }
                else
                {
                    _writeWaitingQueue.Enqueue(tcs);
                }
                return tcs.Task;
            }
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

        internal void CommitWrittingTransaction(IBTreeRootNode btreeRoot)
        {

            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = btreeRoot;
                TryDequeWaiterForWrittingTransaction();
            }
        }

        void TryDequeWaiterForWrittingTransaction()
        {
            if (_writeWaitingQueue.Count > 0)
            {
                var tcs = _writeWaitingQueue.Dequeue();
                NewWrittingTransactionUnsafe(tcs);
            }
        }

        void NewWrittingTransactionUnsafe(TaskCompletionSource<IKeyValue2DBTransaction> tcs)
        {
            var newTransactionRoot = _lastCommited.NewTransactionRoot();
            _writingTransaction = new KeyValue2DBTransaction(this, newTransactionRoot, true);
            tcs.TrySetResult(_writingTransaction);
        }

        internal void RevertWrittingTransaction(bool nothingWrittenToTransactionLog)
        {
            if (!nothingWrittenToTransactionLog)
            {
                while (_fileIdWithTransactionLog != _fileIdWithTransactionLogStartOfTr)
                {
                    var reader = new PositionLessStreamReader(_fileCollection.GetFile(_fileIdWithTransactionLog));
                    reader.SkipBlock(_magicStartOfFile.Length + 1);
                    var prevFileId = reader.ReadVInt64();
                    _fileCollection.RemoveFile(_fileIdWithTransactionLog);
                    _fileIdWithTransactionLog = (int)prevFileId;
                }
                _fileWithTransactionLog = _fileCollection.GetFile(_fileIdWithTransactionLog);
                _fileWithTransactionLog.SetSize((ulong)_positionOfStartOfTr);
                _writerWithTransactionLog = new PositionLessStreamWriter(_fileWithTransactionLog, true);
            }
            lock (_writeLock)
            {
                _writingTransaction = null;
                TryDequeWaiterForWrittingTransaction();
            }
        }

        internal void WriteStartTransaction(long transactionId)
        {
            if (_fileIdWithTransactionLog == -1)
            {
                WriteStartOfNewTransactionLogFile();
            }
            else if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _fileIdWithTransactionLogStartOfTr = _fileIdWithTransactionLog;
            _positionOfStartOfTr = _writerWithTransactionLog.GetCurrentPosition();
            _writerWithTransactionLog.WriteByteArrayRaw(_magicStartOfTransaction);
            _writerWithTransactionLog.WriteVUInt64((ulong)transactionId);
        }

        void WriteStartOfNewTransactionLogFile()
        {
            var previousTrLogFileId = _fileIdWithTransactionLog;
            _fileIdWithTransactionLog = _fileCollection.AddFile("trl");
            _fileWithTransactionLog = _fileCollection.GetFile(_fileIdWithTransactionLog);
            _writerWithTransactionLog = new PositionLessStreamWriter(_fileWithTransactionLog);
            _writerWithTransactionLog.WriteByteArrayRaw(_magicStartOfFile);
            _writerWithTransactionLog.WriteUInt8((byte)KV2FileType.TransactionLog);
            _writerWithTransactionLog.WriteVInt64(previousTrLogFileId);
            _writerWithTransactionLog.WriteDateTime(DateTime.UtcNow);
        }

        public void WriteCreateOrUpdateCommand(ByteBuffer key, ByteBuffer value, out int valueFileId, out int valueOfs, out int valueSize)
        {
            _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.CreateOrUpdate);
            _writerWithTransactionLog.WriteVInt32(key.Length);
            _writerWithTransactionLog.WriteVInt32(value.Length);
            _writerWithTransactionLog.WriteBlock(key);
            valueFileId = _fileIdWithTransactionLog;
            valueOfs = (int)_writerWithTransactionLog.GetCurrentPosition();
            valueSize = value.Length;
            _writerWithTransactionLog.WriteBlock(value);
        }

        public ByteBuffer ReadValue(int valueFileId, int valueOfs, int valueSize)
        {
            var result = ByteBuffer.NewAsync(new byte[valueSize]);
            var file = _fileCollection.GetFile(valueFileId);
            file.Read(result.Buffer, 0, valueSize, (ulong) valueOfs);
            return result;
        }
    }
}
