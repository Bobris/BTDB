using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KV2DBLayer.BTree;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class KeyValue2DB : IKeyValueDB
    {
        readonly IFileCollection _fileCollection;
        IBTreeRootNode _lastCommited;
        IBTreeRootNode _nextRoot;
        KeyValue2DBTransaction _writingTransaction;
        readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();
        readonly object _writeLock = new object();
        int _fileIdWithTransactionLog;
        long _fileGeneration;
        IPositionLessStream _fileWithTransactionLog;
        AbstractBufferedWriter _writerWithTransactionLog;
        readonly byte[] _magicStartOfFile = new[] { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };
        readonly byte[] _magicStartOfTransaction = new[] { (byte)'t', (byte)'R', (byte)'s', (byte)'T' };
        const long MaxTrLogFileSize = 2000000000;
        readonly ConcurrentDictionary<int, IFileInfo> _fileInfos = new ConcurrentDictionary<int, IFileInfo>();

        public KeyValue2DB(IFileCollection fileCollection)
        {
            DurableTransactions = false;
            _fileCollection = fileCollection;
            _lastCommited = new BTreeRoot(0);
            _fileIdWithTransactionLog = -1;
            _fileGeneration = -1;
            LoadInfoAboutFiles();
        }

        void LoadInfoAboutFiles()
        {
            foreach (var fileId in _fileCollection.Enumerate())
            {
                try
                {
                    var reader = new PositionLessStreamReader(_fileCollection.GetFile(fileId));
                    var magicCheck = new byte[_magicStartOfFile.Length];
                    reader.ReadBlock(magicCheck);
                    if (BitArrayManipulation.CompareByteArray(magicCheck, 0, magicCheck.Length, _magicStartOfFile, 0, _magicStartOfFile.Length) == 0)
                    {
                        var fileType = (KV2FileType)reader.ReadUInt8();
                        switch (fileType)
                        {
                            case KV2FileType.TransactionLog:
                                _fileInfos.TryAdd(fileId, new FileTransactionLog(reader));
                                break;
                            default:
                                _fileInfos.TryAdd(fileId, new UnknownFile());
                                break;
                        }
                    }
                }
                catch (Exception)
                {
                    _fileInfos.TryAdd(fileId, new UnknownFile());
                }
            }
            long latestGeneration = -1;
            int lastestTrLogFileId = -1;
            foreach (var fileInfo in _fileInfos)
            {
                var trLog = fileInfo.Value as IFileTransactionLog;
                if (trLog != null)
                {
                    if (trLog.Generation > latestGeneration)
                    {
                        latestGeneration = trLog.Generation;
                        lastestTrLogFileId = fileInfo.Key;
                    }
                }
            }
            _fileGeneration = latestGeneration;
            var firstTrLogId = LinkTransactionLogFileIds(lastestTrLogFileId);
            LoadTransactionLogs(firstTrLogId);
        }

        void LoadTransactionLogs(int firstTrLogId)
        {
            while (firstTrLogId != -1)
            {
                _fileIdWithTransactionLog = firstTrLogId;
                LoadTransactionLog(firstTrLogId);
                IFileInfo fileInfo;
                _fileInfos.TryGetValue(firstTrLogId, out fileInfo);
                firstTrLogId = ((IFileTransactionLog)fileInfo).NextFileId;
            }
        }

        void LoadTransactionLog(int fileId)
        {
            var stack = new List<NodeIdxPair>();
            var reader = new PositionLessStreamReader(_fileCollection.GetFile(fileId));
            reader.SkipBlock(_magicStartOfFile.Length + 1);
            reader.SkipVInt64(); // generation
            reader.SkipVInt32(); // previous file id
            while (!reader.Eof)
            {
                if (!CheckMagic(reader, _magicStartOfTransaction))
                {
                    throw new BTDBException("corrupted db");
                }
                _nextRoot = _lastCommited.NewTransactionRoot();
                while (!reader.Eof)
                {
                    switch ((KV2CommandType)reader.ReadUInt8())
                    {
                        case KV2CommandType.CreateOrUpdate:
                            {
                                var keyLen = reader.ReadVInt32();
                                var valueLen = reader.ReadVInt32();
                                var key = new byte[keyLen];
                                reader.ReadBlock(key);
                                var ctx = new CreateOrUpdateCtx
                                    {
                                        KeyPrefix = BitArrayManipulation.EmptyByteArray,
                                        Key = ByteBuffer.NewAsync(key),
                                        ValueFileId = fileId,
                                        ValueOfs = (int)reader.GetCurrentPosition(),
                                        ValueSize = valueLen
                                    };
                                _nextRoot.CreateOrUpdate(ctx);
                                reader.SkipBlock(valueLen);
                            }
                            break;
                        case KV2CommandType.EraseOne:
                            {
                                var keyLen = reader.ReadVInt32();
                                var key = new byte[keyLen];
                                reader.ReadBlock(key);
                                long keyIndex;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex,
                                                                   BitArrayManipulation.EmptyByteArray,
                                                                   ByteBuffer.NewAsync(key));
                                if (findResult == FindResult.Exact)
                                    _nextRoot.EraseRange(keyIndex, keyIndex);
                            }
                            break;
                        case KV2CommandType.EraseRange:
                            {
                                var keyLen1 = reader.ReadVInt32();
                                var keyLen2 = reader.ReadVInt32();
                                var key = new byte[keyLen1];
                                reader.ReadBlock(key);
                                long keyIndex1;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex1,
                                                                   BitArrayManipulation.EmptyByteArray,
                                                                   ByteBuffer.NewAsync(key));
                                if (findResult == FindResult.Previous) keyIndex1++;
                                key = new byte[keyLen2];
                                reader.ReadBlock(key);
                                long keyIndex2;
                                findResult = _nextRoot.FindKey(stack, out keyIndex2,
                                                               BitArrayManipulation.EmptyByteArray,
                                                               ByteBuffer.NewAsync(key));
                                if (findResult == FindResult.Next) keyIndex2--;
                                _nextRoot.EraseRange(keyIndex1, keyIndex2);
                            }
                            break;
                        case KV2CommandType.Commit:
                            _lastCommited = _nextRoot;
                            _nextRoot = null;
                            goto EndTr;
                        case KV2CommandType.Rollback:
                            _nextRoot = null;
                            goto EndTr;
                        default:
                            throw new BTDBException("corrupted db");
                    }
                }
            EndTr:
                ;
            }
        }

        static bool CheckMagic(AbstractBufferedReader reader, byte[] magic)
        {
            try
            {
                var buf = new byte[magic.Length];
                reader.ReadBlock(buf);
                if (BitArrayManipulation.CompareByteArray(buf, 0, buf.Length, magic, 0, magic.Length) == 0)
                {
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }
            return false;
        }

        int LinkTransactionLogFileIds(int lastestTrLogFileId)
        {
            var nextId = -1;
            var currentId = lastestTrLogFileId;
            while (currentId != -1)
            {
                IFileInfo fileInfo;
                if (_fileInfos.TryGetValue(currentId, out fileInfo))
                {
                    var fileTransactionLog = fileInfo as IFileTransactionLog;
                    fileTransactionLog.NextFileId = nextId;
                    nextId = currentId;
                    currentId = fileTransactionLog.PreviousFileId;
                }
            }
            return nextId;
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

        public bool DurableTransactions { get; set; }

        public IKeyValueDBTransaction StartTransaction()
        {
            return new KeyValue2DBTransaction(this, _lastCommited, false);
        }

        public Task<IKeyValueDBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {
                var tcs = new TaskCompletionSource<IKeyValueDBTransaction>();
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

        public string CalcStats()
        {
            return "KeyValueCount:" + _lastCommited.CalcKeyCount() + Environment.NewLine
                   + "FileCount:" + _fileCollection.GetCount() + Environment.NewLine
                   + "FileGeneration:" + _fileGeneration;
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
            _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.Commit);
            _writerWithTransactionLog.FlushBuffer();
            _fileWithTransactionLog.Flush();
            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = btreeRoot;
                TryDequeWaiterForWrittingTransaction();
            }
        }

        void TryDequeWaiterForWrittingTransaction()
        {
            if (_writeWaitingQueue.Count == 0) return;
            var tcs = _writeWaitingQueue.Dequeue();
            NewWrittingTransactionUnsafe(tcs);
        }

        void NewWrittingTransactionUnsafe(TaskCompletionSource<IKeyValueDBTransaction> tcs)
        {
            var newTransactionRoot = _lastCommited.NewTransactionRoot();
            _writingTransaction = new KeyValue2DBTransaction(this, newTransactionRoot, true);
            tcs.TrySetResult(_writingTransaction);
        }

        internal void RevertWrittingTransaction(bool nothingWrittenToTransactionLog)
        {
            if (!nothingWrittenToTransactionLog)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.Rollback);
            }
            lock (_writeLock)
            {
                _writingTransaction = null;
                TryDequeWaiterForWrittingTransaction();
            }
        }

        internal void WriteStartTransaction()
        {
            if (_fileIdWithTransactionLog == -1)
            {
                WriteStartOfNewTransactionLogFile();
            }
            else
            {
                if (_writerWithTransactionLog == null)
                {
                    _fileWithTransactionLog = _fileCollection.GetFile(_fileIdWithTransactionLog);
                    _writerWithTransactionLog = new PositionLessStreamWriter(_fileWithTransactionLog, true);
                }
                if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
                {
                    WriteStartOfNewTransactionLogFile();
                }
            }
            _writerWithTransactionLog.WriteByteArrayRaw(_magicStartOfTransaction);
        }

        void WriteStartOfNewTransactionLogFile()
        {
            if (_writerWithTransactionLog != null)
            {
                _writerWithTransactionLog.FlushBuffer();
                _fileWithTransactionLog.HardFlush();
            }
            var previousTrLogFileId = _fileIdWithTransactionLog;
            _fileIdWithTransactionLog = _fileCollection.AddFile("trl");
            _fileWithTransactionLog = _fileCollection.GetFile(_fileIdWithTransactionLog);
            _writerWithTransactionLog = new PositionLessStreamWriter(_fileWithTransactionLog);
            _writerWithTransactionLog.WriteByteArrayRaw(_magicStartOfFile);
            _writerWithTransactionLog.WriteUInt8((byte)KV2FileType.TransactionLog);
            _writerWithTransactionLog.WriteVInt64(NextGeneration());
            _writerWithTransactionLog.WriteVInt64(previousTrLogFileId);
        }

        long NextGeneration()
        {
            return Interlocked.Increment(ref _fileGeneration) + 1;
        }

        public void WriteCreateOrUpdateCommand(byte[] prefix, ByteBuffer key, ByteBuffer value, out int valueFileId, out int valueOfs, out int valueSize)
        {
            if (_writerWithTransactionLog.GetCurrentPosition() + prefix.Length+key.Length + 16 > int.MaxValue)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.CreateOrUpdate);
            _writerWithTransactionLog.WriteVInt32(prefix.Length+key.Length);
            _writerWithTransactionLog.WriteVInt32(value.Length);
            _writerWithTransactionLog.WriteBlock(prefix);
            _writerWithTransactionLog.WriteBlock(key);
            valueFileId = _fileIdWithTransactionLog;
            valueOfs = (int)_writerWithTransactionLog.GetCurrentPosition();
            valueSize = value.Length;
            _writerWithTransactionLog.WriteBlock(value);
            _writerWithTransactionLog.FlushBuffer();
        }

        public ByteBuffer ReadValue(int valueFileId, int valueOfs, int valueSize)
        {
            var result = ByteBuffer.NewAsync(new byte[valueSize]);
            var file = _fileCollection.GetFile(valueFileId);
            file.Read(result.Buffer, 0, valueSize, (ulong)valueOfs);
            return result;
        }

        public void WriteEraseOneCommand(byte[] key)
        {
            if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.EraseOne);
            _writerWithTransactionLog.WriteVInt32(key.Length);
            _writerWithTransactionLog.WriteBlock(key);
        }

        public void WriteEraseRangeCommand(byte[] firstKey, byte[] secondKey)
        {
            if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.EraseRange);
            _writerWithTransactionLog.WriteVInt32(firstKey.Length);
            _writerWithTransactionLog.WriteVInt32(secondKey.Length);
            _writerWithTransactionLog.WriteBlock(firstKey);
            _writerWithTransactionLog.WriteBlock(secondKey);
        }
    }
}
