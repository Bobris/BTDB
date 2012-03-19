using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        uint _fileIdWithTransactionLog;
        uint _fileIdWithPreviousTransactionLog;
        long _fileGeneration;
        IPositionLessStream _fileWithTransactionLog;
        AbstractBufferedWriter _writerWithTransactionLog;
        internal static readonly byte[] MagicStartOfFile = new[] { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };
        static readonly byte[] MagicStartOfTransaction = new[] { (byte)'t', (byte)'R' };
        const long MaxTrLogFileSize = int.MaxValue;
        readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
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
            LoadInfoAboutFiles();
        }

        void LoadInfoAboutFiles()
        {
            foreach (var fileId in FileCollection.Enumerate())
            {
                try
                {
                    var reader = new PositionLessStreamReader(FileCollection.GetFile(fileId));
                    if (CheckMagic(reader, MagicStartOfFile))
                    {
                        var fileType = (KV2FileType)reader.ReadUInt8();
                        IFileInfo fileInfo;
                        switch (fileType)
                        {
                            case KV2FileType.TransactionLog:
                                fileInfo = new FileTransactionLog(reader);
                                break;
                            case KV2FileType.KeyIndex:
                                fileInfo = new FileKeyIndex(reader);
                                break;
                            case KV2FileType.PureValues:
                                fileInfo = new FilePureValues(reader);
                                break;
                            default:
                                fileInfo = UnknownFile.Instance;
                                break;
                        }
                        if (_fileGeneration < fileInfo.Generation) _fileGeneration = fileInfo.Generation;
                        _fileInfos.TryAdd(fileId, fileInfo);
                    }
                }
                catch (Exception)
                {
                    _fileInfos.TryAdd(fileId, UnknownFile.Instance);
                }
            }
            long latestGeneration = -1;
            uint lastestTrLogFileId = 0;
            var keyIndexes = new List<KeyValuePair<uint, long>>();
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
                    continue;
                }
                var keyIndex = fileInfo.Value as IKeyIndex;
                if (keyIndex == null) continue;
                keyIndexes.Add(new KeyValuePair<uint, long>(fileInfo.Key, keyIndex.Generation));
            }
            if (keyIndexes.Count > 1)
                keyIndexes.Sort((l, r) => Comparer<long>.Default.Compare(l.Value, r.Value));
            var firstTrLogId = LinkTransactionLogFileIds(lastestTrLogFileId);
            var firstTrLogOffset = 0u;
            while (keyIndexes.Count > 0)
            {
                var keyIndex = keyIndexes[keyIndexes.Count - 1];
                keyIndexes.RemoveAt(keyIndexes.Count - 1);
                var info = (IKeyIndex)_fileInfos[keyIndex.Key];
                _nextRoot = LastCommited.NewTransactionRoot();
                if (LoadKeyIndex(keyIndex.Key, info))
                {
                    _lastCommited = _nextRoot;
                    _nextRoot = null;
                    firstTrLogId = info.TrLogFileId;
                    firstTrLogOffset = info.TrLogOffset;
                    break;
                }
                _fileInfos[keyIndex.Key] = UnknownFile.Instance;
            }
            while (keyIndexes.Count > 0)
            {
                var keyIndex = keyIndexes[keyIndexes.Count - 1];
                keyIndexes.RemoveAt(keyIndexes.Count - 1);
                _fileInfos[keyIndex.Key] = UnknownFile.Instance;
            }
            LoadTransactionLogs(firstTrLogId, firstTrLogOffset);
            if (lastestTrLogFileId != firstTrLogId)
            {
                CreateKeyIndexFile(LastCommited);
            }
            DeleteAllUnknownFiles();
        }

        void DeleteAllUnknownFiles()
        {
            foreach (var fileId in _fileInfos.Where(fi => fi.Value.FileType == KV2FileType.Unknown).Select(fi => fi.Key).ToArray())
            {
                FileCollection.RemoveFile(fileId);
                _fileInfos.TryRemove(fileId);
            }
        }

        bool LoadKeyIndex(uint fileId, IKeyIndex info)
        {
            try
            {
                var stream = FileCollection.GetFile(fileId);
                var reader = new PositionLessStreamReader(stream);
                FileKeyIndex.SkipHeader(reader);
                var keyCount = info.KeyValueCount;
                _nextRoot.TrLogFileId = info.TrLogFileId;
                _nextRoot.TrLogOffset = info.TrLogOffset;
                _nextRoot.BuildTree(keyCount, () =>
                    {
                        var keyLength = reader.ReadVInt32();
                        var key = ByteBuffer.NewAsync(new byte[Math.Abs(keyLength)]);
                        reader.ReadBlock(key);
                        if (keyLength < 0)
                        {
                            _compression.DecompressKey(ref key);
                        }
                        return new BTreeLeafMember
                            {
                                Key = key.ToByteArray(),
                                ValueFileId = reader.ReadVUInt32(),
                                ValueOfs = reader.ReadVUInt32(),
                                ValueSize = reader.ReadVInt32()
                            };
                    });
                return reader.Eof;
            }
            catch (Exception)
            {
                return false;
            }
        }

        void LoadTransactionLogs(uint firstTrLogId, uint firstTrLogOffset)
        {
            while (firstTrLogId != 0 && firstTrLogId != uint.MaxValue)
            {
                _fileIdWithTransactionLog = 0;
                if (LoadTransactionLog(firstTrLogId, firstTrLogOffset))
                {
                    _fileIdWithTransactionLog = firstTrLogId;
                }
                firstTrLogOffset = 0;
                _fileIdWithPreviousTransactionLog = firstTrLogId;
                IFileInfo fileInfo;
                if (!_fileInfos.TryGetValue(firstTrLogId, out fileInfo))
                    return;
                firstTrLogId = ((IFileTransactionLog)fileInfo).NextFileId;
            }
        }

        // Return true if it is suitable for continuing writing new transactions
        bool LoadTransactionLog(uint fileId, uint logOffset)
        {
            var stack = new List<NodeIdxPair>();
            var reader = new PositionLessStreamReader(FileCollection.GetFile(fileId));
            try
            {
                if (logOffset == 0)
                {
                    FileTransactionLog.SkipHeader(reader);
                }
                else
                {
                    reader.SkipBlock(logOffset);
                }
                while (!reader.Eof)
                {
                    var command = (KV2CommandType)reader.ReadUInt8();
                    switch (command & KV2CommandType.CommandMask)
                    {
                        case KV2CommandType.CreateOrUpdate:
                            {
                                if (_nextRoot == null) return false;
                                var keyLen = reader.ReadVInt32();
                                var valueLen = reader.ReadVInt32();
                                var key = new byte[keyLen];
                                reader.ReadBlock(key);
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }
                                var ctx = new CreateOrUpdateCtx
                                {
                                    KeyPrefix = BitArrayManipulation.EmptyByteArray,
                                    Key = keyBuf,
                                    ValueFileId = fileId,
                                    ValueOfs = (uint)reader.GetCurrentPosition(),
                                    ValueSize = (command & KV2CommandType.SecondParamCompressed) != 0 ? -valueLen : valueLen
                                };
                                _nextRoot.CreateOrUpdate(ctx);
                                reader.SkipBlock(valueLen);
                            }
                            break;
                        case KV2CommandType.EraseOne:
                            {
                                if (_nextRoot == null) return false;
                                var keyLen = reader.ReadVInt32();
                                var key = new byte[keyLen];
                                reader.ReadBlock(key);
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }
                                long keyIndex;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex, BitArrayManipulation.EmptyByteArray, keyBuf);
                                if (findResult == FindResult.Exact)
                                    _nextRoot.EraseRange(keyIndex, keyIndex);
                            }
                            break;
                        case KV2CommandType.EraseRange:
                            {
                                if (_nextRoot == null) return false;
                                var keyLen1 = reader.ReadVInt32();
                                var keyLen2 = reader.ReadVInt32();
                                var key = new byte[keyLen1];
                                reader.ReadBlock(key);
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }
                                long keyIndex1;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex1, BitArrayManipulation.EmptyByteArray, keyBuf);
                                if (findResult == FindResult.Previous) keyIndex1++;
                                key = new byte[keyLen2];
                                reader.ReadBlock(key);
                                keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KV2CommandType.SecondParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
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
                            _nextRoot = LastCommited.NewTransactionRoot();
                            break;
                        case KV2CommandType.Commit:
                            _nextRoot.TrLogFileId = fileId;
                            _nextRoot.TrLogOffset = (uint)reader.GetCurrentPosition();
                            _lastCommited = _nextRoot;
                            _nextRoot = null;
                            break;
                        case KV2CommandType.Rollback:
                            _nextRoot = null;
                            break;
                        case KV2CommandType.EndOfFile:
                            return false;
                        default:
                            _nextRoot = null;
                            return false;
                    }
                }
                return _nextRoot == null;
            }
            catch (EndOfStreamException)
            {
                _nextRoot = null;
                return false;
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

        uint LinkTransactionLogFileIds(uint lastestTrLogFileId)
        {
            var nextId = 0u;
            var currentId = lastestTrLogFileId;
            while (currentId != 0)
            {
                IFileInfo fileInfo;
                if (!_fileInfos.TryGetValue(currentId, out fileInfo))
                {
                    break;
                }
                var fileTransactionLog = fileInfo as IFileTransactionLog;
                if (fileTransactionLog == null) break;
                fileTransactionLog.NextFileId = nextId;
                nextId = currentId;
                currentId = fileTransactionLog.PreviousFileId;
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

        internal IBTreeRootNode LastCommited
        {
            get { return _lastCommited; }
        }

        internal IFileCollection FileCollection
        {
            get { return _fileCollection; }
        }

        public IKeyValueDBTransaction StartTransaction()
        {
            return new KeyValue2DBTransaction(this, LastCommited, false);
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
            return "KeyValueCount:" + LastCommited.CalcKeyCount() + Environment.NewLine
                   + "FileCount:" + FileCollection.GetCount() + Environment.NewLine
                   + "FileGeneration:" + _fileGeneration;
        }

        internal IBTreeRootNode MakeWrittableTransaction(KeyValue2DBTransaction keyValue2DBTransaction, IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBTransactionRetryException("Another writting transaction already running");
                if (LastCommited != btreeRoot) throw new BTDBTransactionRetryException("Another writting transaction already finished");
                _writingTransaction = keyValue2DBTransaction;
                return btreeRoot.NewTransactionRoot();
            }
        }

        internal void CommitWrittingTransaction(IBTreeRootNode btreeRoot)
        {
            _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.Commit);
            _writerWithTransactionLog.FlushBuffer();
            btreeRoot.TrLogFileId = _fileIdWithTransactionLog;
            btreeRoot.TrLogOffset = (uint)_writerWithTransactionLog.GetCurrentPosition();
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
            var newTransactionRoot = LastCommited.NewTransactionRoot();
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
            if (_fileIdWithTransactionLog == 0)
            {
                WriteStartOfNewTransactionLogFile();
            }
            else
            {
                if (_writerWithTransactionLog == null)
                {
                    _fileWithTransactionLog = FileCollection.GetFile(_fileIdWithTransactionLog);
                    _writerWithTransactionLog = new PositionLessStreamWriter(_fileWithTransactionLog, true);
                }
                if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
                {
                    WriteStartOfNewTransactionLogFile();
                }
            }
            _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.TransactionStart);
            _writerWithTransactionLog.WriteByteArrayRaw(MagicStartOfTransaction);
        }

        void WriteStartOfNewTransactionLogFile()
        {
            if (_writerWithTransactionLog != null)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KV2CommandType.EndOfFile);
                _writerWithTransactionLog.FlushBuffer();
                _fileWithTransactionLog.HardFlush();
                _fileIdWithPreviousTransactionLog = _fileIdWithTransactionLog;
            }
            _fileIdWithTransactionLog = FileCollection.AddFile("trl");
            _fileWithTransactionLog = FileCollection.GetFile(_fileIdWithTransactionLog);
            var transactionLog = new FileTransactionLog(NextGeneration(), _fileIdWithPreviousTransactionLog);
            _writerWithTransactionLog = new PositionLessStreamWriter(_fileWithTransactionLog);
            transactionLog.WriteHeader(_writerWithTransactionLog);
            _fileInfos.TryAdd(_fileIdWithTransactionLog, transactionLog);
        }

        long NextGeneration()
        {
            return Interlocked.Increment(ref _fileGeneration);
        }

        public void WriteCreateOrUpdateCommand(byte[] prefix, ByteBuffer key, ByteBuffer value, out uint valueFileId, out uint valueOfs, out int valueSize)
        {
            var command = KV2CommandType.CreateOrUpdate;
            if (_compression.ShouldTryToCompressKey(prefix.Length + key.Length))
            {
                if (prefix.Length != 0)
                {
                    var fullkey = new byte[prefix.Length + key.Length];
                    Array.Copy(prefix, 0, fullkey, 0, prefix.Length);
                    Array.Copy(key.Buffer, prefix.Length + key.Offset, fullkey, prefix.Length, key.Length);
                    prefix = BitArrayManipulation.EmptyByteArray;
                    key = ByteBuffer.NewAsync(fullkey);
                }
                if (_compression.CompressKey(ref key))
                {
                    command |= KV2CommandType.FirstParamCompressed;
                }
            }
            valueSize = value.Length;
            if (_compression.CompressValue(ref value))
            {
                command |= KV2CommandType.SecondParamCompressed;
                valueSize = -value.Length;
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
            if (valueSize!=0)
            {
                valueFileId = _fileIdWithTransactionLog;
                valueOfs = (uint)_writerWithTransactionLog.GetCurrentPosition();
                _writerWithTransactionLog.WriteBlock(value);
            }
            else
            {
                valueFileId = 0;
                valueOfs = 0;
            }
            _writerWithTransactionLog.FlushBuffer();
        }

        public ByteBuffer ReadValue(uint valueFileId, uint valueOfs, int valueSize)
        {
            if (valueSize == 0) return ByteBuffer.NewEmpty();
            var compressed = false;
            if (valueSize < 0)
            {
                compressed = true;
                valueSize = -valueSize;
            }
            var result = ByteBuffer.NewAsync(new byte[valueSize]);
            var file = FileCollection.GetFile(valueFileId);
            file.Read(result.Buffer, 0, valueSize, valueOfs);
            if (compressed)
                _compression.DecompressValue(ref result);
            return result;
        }

        public void WriteEraseOneCommand(byte[] key)
        {
            var command = KV2CommandType.EraseOne;
            var keyBuf = ByteBuffer.NewSync(key);
            if (_compression.ShouldTryToCompressKey(keyBuf.Length))
            {
                if (_compression.CompressKey(ref keyBuf))
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
            if (_compression.ShouldTryToCompressKey(firstKeyBuf.Length))
            {
                if (_compression.CompressKey(ref firstKeyBuf))
                {
                    command |= KV2CommandType.FirstParamCompressed;
                }
            }
            if (_compression.ShouldTryToCompressKey(secondKeyBuf.Length))
            {
                if (_compression.CompressKey(ref secondKeyBuf))
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

        void CreateKeyIndexFile(IBTreeRootNode root)
        {
            var fileId = FileCollection.AddFile("kvi");
            var stream = FileCollection.GetFile(fileId);
            var writer = new PositionLessStreamWriter(stream);
            var keyCount = root.CalcKeyCount();
            var keyIndex = new FileKeyIndex(NextGeneration(), root.TrLogFileId, root.TrLogOffset, keyCount);
            keyIndex.WriteHeader(writer);
            if (keyCount > 0)
            {
                var stack = new List<NodeIdxPair>();
                root.FillStackByIndex(stack, 0);
                do
                {
                    var nodeIdxPair = stack[stack.Count - 1];
                    var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMember(nodeIdxPair.Idx);
                    var key = ByteBuffer.NewSync(leafMember.Key);
                    var keyCompressed = false;
                    if (_compression.ShouldTryToCompressKey(leafMember.Key.Length))
                    {
                        keyCompressed = _compression.CompressKey(ref key);
                    }
                    writer.WriteVInt32(keyCompressed ? -key.Length : key.Length);
                    writer.WriteBlock(key);
                    writer.WriteVUInt32(leafMember.ValueFileId);
                    writer.WriteVUInt32(leafMember.ValueOfs);
                    writer.WriteVInt32(leafMember.ValueSize);
                } while (root.FindNextKey(stack));
            }
            writer.FlushBuffer();
            stream.HardFlush();
            _fileInfos.TryAdd(fileId, keyIndex);
        }

        void Compact()
        {
            new Compactor(this).Run();
        }

        internal bool ContainsValues(uint fileId)
        {
            IFileInfo info;
            if (!_fileInfos.TryGetValue(fileId, out info)) return false;
            return (info.FileType == KV2FileType.TransactionLog || info.FileType == KV2FileType.PureValues);
        }

        internal AbstractBufferedWriter StartPureValuesFile(out uint fileId)
        {
            var fId = FileCollection.AddFile("pvl");
            fileId = fId;
            var stream = FileCollection.GetFile(fId);
            var pureValues = new FilePureValues(NextGeneration());
            var writer = new PositionLessStreamWriter(stream,()=>
                {
                    stream.HardFlush();
                    _fileInfos[fId] = pureValues;
                });
            pureValues.WriteHeader(writer);
            return writer;
        }
    }
}
