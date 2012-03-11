using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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
        internal static readonly byte[] MagicStartOfFile = new[] { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };
        static readonly byte[] MagicStartOfTransaction = new[] { (byte)'t', (byte)'R' };
        const long MaxTrLogFileSize = int.MaxValue;
        readonly ConcurrentDictionary<int, IFileInfo> _fileInfos = new ConcurrentDictionary<int, IFileInfo>();
        readonly ICompressionStrategy _compression;

        public KeyValue2DB(IFileCollection fileCollection)
            : this(fileCollection, new SnappyCompressionStrategy())
        {
        }

        public KeyValue2DB(IFileCollection fileCollection, ICompressionStrategy compression)
        {
            if (fileCollection == null) throw new ArgumentNullException("fileCollection");
            if (compression == null) throw new ArgumentNullException("compression");
            _compression = compression;
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
                    var magicCheck = new byte[MagicStartOfFile.Length];
                    reader.ReadBlock(magicCheck);
                    if (BitArrayManipulation.CompareByteArray(magicCheck, 0, magicCheck.Length, MagicStartOfFile, 0, MagicStartOfFile.Length) == 0)
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
                _fileIdWithTransactionLog = -1;
                if (LoadTransactionLog(firstTrLogId))
                {
                    _fileIdWithTransactionLog = firstTrLogId;
                }
                else
                {
                    _nextRoot = null;
                }
                IFileInfo fileInfo;
                _fileInfos.TryGetValue(firstTrLogId, out fileInfo);
                firstTrLogId = ((IFileTransactionLog)fileInfo).NextFileId;
            }
        }

        bool LoadTransactionLog(int fileId)
        {
            var stack = new List<NodeIdxPair>();
            var reader = new PositionLessStreamReader(_fileCollection.GetFile(fileId));
            try
            {
                FileTransactionLog.SkipHeader(reader);
                while (!reader.Eof)
                {
                    var command = (KV2CommandType)reader.ReadUInt8();
                    switch (command & KV2CommandType.CommandMask)
                    {
                        case KV2CommandType.CreateOrUpdate:
                            {
                                var keyLen = reader.ReadVInt32();
                                var valueLen = reader.ReadVInt32();
                                var key = new byte[keyLen];
                                reader.ReadBlock(key);
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKeyFromTransactionLog(ref keyBuf);
                                }
                                var ctx = new CreateOrUpdateCtx
                                {
                                    KeyPrefix = BitArrayManipulation.EmptyByteArray,
                                    Key = keyBuf,
                                    ValueFileId = fileId,
                                    ValueOfs = (int)reader.GetCurrentPosition(),
                                    ValueSize = NegateIf((command & KV2CommandType.SecondParamCompressed) != 0, valueLen)
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
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKeyFromTransactionLog(ref keyBuf);
                                }
                                long keyIndex;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex, BitArrayManipulation.EmptyByteArray, keyBuf);
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
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKeyFromTransactionLog(ref keyBuf);
                                }
                                long keyIndex1;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex1, BitArrayManipulation.EmptyByteArray, keyBuf);
                                if (findResult == FindResult.Previous) keyIndex1++;
                                key = new byte[keyLen2];
                                reader.ReadBlock(key);
                                keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.SecondParamCompressed) != 0)
                                {
                                    _compression.DecompressKeyFromTransactionLog(ref keyBuf);
                                }
                                long keyIndex2;
                                findResult = _nextRoot.FindKey(stack, out keyIndex2, BitArrayManipulation.EmptyByteArray, keyBuf);
                                if (findResult == FindResult.Next) keyIndex2--;
                                _nextRoot.EraseRange(keyIndex1, keyIndex2);
                            }
                            break;
                        case KV2CommandType.TransactionStart:
                            if (!CheckMagic(reader, MagicStartOfTransaction))
                                return false;
                            _nextRoot = _lastCommited.NewTransactionRoot();
                            break;
                        case KV2CommandType.Commit:
                            _lastCommited = _nextRoot;
                            _nextRoot = null;
                            break;
                        case KV2CommandType.Rollback:
                            _nextRoot = null;
                            break;
                        case KV2CommandType.EndOfFile:
                            return true;
                        default:
                            return false;
                    }
                }
                return _nextRoot == null;
            }
            catch (EndOfStreamException)
            {
                return false;
            }
        }

        static int NegateIf(bool condition, int value)
        {
            if (condition) return -value;
            return value;
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
            _writerWithTransactionLog.WriteUInt8((byte) KV2CommandType.TransactionStart);
            _writerWithTransactionLog.WriteByteArrayRaw(MagicStartOfTransaction);
        }

        void WriteStartOfNewTransactionLogFile()
        {
            if (_writerWithTransactionLog != null)
            {
                _writerWithTransactionLog.WriteUInt8((byte) KV2CommandType.EndOfFile);
                _writerWithTransactionLog.FlushBuffer();
                _fileWithTransactionLog.HardFlush();
            }
            var previousTrLogFileId = _fileIdWithTransactionLog;
            _fileIdWithTransactionLog = _fileCollection.AddFile("trl");
            _fileWithTransactionLog = _fileCollection.GetFile(_fileIdWithTransactionLog);
            _writerWithTransactionLog = new PositionLessStreamWriter(_fileWithTransactionLog);
            _writerWithTransactionLog.WriteByteArrayRaw(MagicStartOfFile);
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
            var command = KV2CommandType.CreateOrUpdate;
            if (_compression.ShouldTryToCompressKeyToTransactionLog(prefix.Length + key.Length))
            {
                if (prefix.Length != 0)
                {
                    var fullkey = new byte[prefix.Length + key.Length];
                    Array.Copy(prefix, 0, fullkey, 0, prefix.Length);
                    Array.Copy(key.Buffer, prefix.Length + key.Offset, fullkey, prefix.Length, key.Length);
                    prefix = BitArrayManipulation.EmptyByteArray;
                    key = ByteBuffer.NewAsync(fullkey);
                }
                if (_compression.CompressKeyToTransactionLog(ref key))
                {
                    command |= KV2CommandType.FirstParamCompressed;
                }
            }
            if (_compression.CompressValueToTransactionLog(ref value))
            {
                command |= KV2CommandType.SecondParamCompressed;
            }
            if (_writerWithTransactionLog.GetCurrentPosition() + prefix.Length + key.Length + 16 > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)command);
            _writerWithTransactionLog.WriteVInt32(prefix.Length + key.Length);
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
            var compressed = false;
            if (valueSize<0)
            {
                compressed = true;
                valueSize = -valueSize;
            }
            var result = ByteBuffer.NewAsync(new byte[valueSize]);
            var file = _fileCollection.GetFile(valueFileId);
            file.Read(result.Buffer, 0, valueSize, (ulong)valueOfs);
            if (compressed)
                _compression.DecompressValueFromTransactionLog(ref result);
            return result;
        }

        public void WriteEraseOneCommand(byte[] key)
        {
            var command = KV2CommandType.EraseOne;
            var keyBuf = ByteBuffer.NewSync(key);
            if (_compression.ShouldTryToCompressKeyToTransactionLog(keyBuf.Length))
            {
                if (_compression.CompressKeyToTransactionLog(ref keyBuf))
                {
                    command |= KV2CommandType.FirstParamCompressed;
                }
            }
            if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)command);
            _writerWithTransactionLog.WriteVInt32(keyBuf.Length);
            _writerWithTransactionLog.WriteBlock(keyBuf);
        }

        public void WriteEraseRangeCommand(byte[] firstKey, byte[] secondKey)
        {
            var command = KV2CommandType.EraseRange;
            var firstKeyBuf = ByteBuffer.NewSync(firstKey);
            var secondKeyBuf = ByteBuffer.NewSync(secondKey);
            if (_compression.ShouldTryToCompressKeyToTransactionLog(firstKeyBuf.Length))
            {
                if (_compression.CompressKeyToTransactionLog(ref firstKeyBuf))
                {
                    command |= KV2CommandType.FirstParamCompressed;
                }
            }
            if (_compression.ShouldTryToCompressKeyToTransactionLog(secondKeyBuf.Length))
            {
                if (_compression.CompressKeyToTransactionLog(ref secondKeyBuf))
                {
                    command |= KV2CommandType.SecondParamCompressed;
                }
            }
            if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)command);
            _writerWithTransactionLog.WriteVInt32(firstKeyBuf.Length);
            _writerWithTransactionLog.WriteVInt32(secondKeyBuf.Length);
            _writerWithTransactionLog.WriteBlock(firstKeyBuf);
            _writerWithTransactionLog.WriteBlock(secondKeyBuf);
        }
    }
}
