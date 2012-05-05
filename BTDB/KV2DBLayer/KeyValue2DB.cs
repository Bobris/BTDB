using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        const int MaxValueSizeInlineInMemory = 7;
        readonly IFileCollection _fileCollection;
        IBTreeRootNode _lastCommited;
        IBTreeRootNode _nextRoot;
        KeyValue2DBTransaction _writingTransaction;
        readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();
        readonly object _writeLock = new object();
        uint _fileIdWithTransactionLog;
        uint _fileIdWithPreviousTransactionLog;
        long _fileGeneration;
        IFileCollectionFile _fileWithTransactionLog;
        AbstractBufferedWriter _writerWithTransactionLog;
        internal static readonly byte[] MagicStartOfFile = new[] { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };
        static readonly byte[] MagicStartOfTransaction = new[] { (byte)'t', (byte)'R' };
        internal readonly long MaxTrLogFileSize;
        readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
        readonly ICompressionStrategy _compression;
        readonly CompactorScheduler _compactorScheduler;
        readonly Dictionary<long, IBTreeRootNode> _usedBTreeRootNodes = new Dictionary<long, IBTreeRootNode>();
        readonly object _usedBTreeRootNodesLock = new object();

        public KeyValue2DB(IFileCollection fileCollection)
            : this(fileCollection, new SnappyCompressionStrategy())
        {
        }

        public KeyValue2DB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize = int.MaxValue)
        {
            if (fileCollection == null) throw new ArgumentNullException("fileCollection");
            if (compression == null) throw new ArgumentNullException("compression");
            if (fileSplitSize < 1024 || fileSplitSize > int.MaxValue) throw new ArgumentOutOfRangeException("fileSplitSize", "Allowed range 1024 - 2G");
            MaxTrLogFileSize = fileSplitSize;
            _compression = compression;
            DurableTransactions = false;
            _fileCollection = fileCollection;
            _lastCommited = new BTreeRoot(0);
            _compactorScheduler = new CompactorScheduler(token => new Compactor(this, token).Run());
            LoadInfoAboutFiles();
        }

        void LoadInfoAboutFiles()
        {
            foreach (var file in FileCollection.Enumerate())
            {
                try
                {
                    var reader = file.GetExclusiveReader();
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
                        _fileInfos.TryAdd(file.Index, fileInfo);
                    }
                    else
                    {
                        _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
                    }
                }
                catch (Exception)
                {
                    _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
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
            var hasKeyIndex = false;
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
                    hasKeyIndex = true;
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
            if (lastestTrLogFileId != firstTrLogId && firstTrLogId != 0)
            {
                CreateIndexFile(CancellationToken.None);
                hasKeyIndex = true;
            }
            if (hasKeyIndex)
            {
                new Compactor(this, CancellationToken.None).FastStartCleanUp();
            }
            DeleteAllUnknownFiles();
            _compactorScheduler.AdviceRunning();
        }

        internal void CreateIndexFile(CancellationToken cancellation)
        {
            var idxFileId = CreateKeyIndexFile(LastCommited, cancellation);
            MarkAsUnknown(_fileInfos.Where(p => p.Value.FileType == KV2FileType.KeyIndex && p.Key != idxFileId).Select(p => p.Key));
        }

        internal void DeleteAllUnknownFiles()
        {
            foreach (var fileId in _fileInfos.Where(fi => fi.Value.FileType == KV2FileType.Unknown).Select(fi => fi.Key).ToArray())
            {
                FileCollection.GetFile(fileId).Remove();
                _fileInfos.TryRemove(fileId);
            }
        }

        bool LoadKeyIndex(uint fileId, IKeyIndex info)
        {
            try
            {
                var reader = FileCollection.GetFile(fileId).GetExclusiveReader();
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
            var inlineValueBuf = new byte[MaxValueSizeInlineInMemory];
            var stack = new List<NodeIdxPair>();
            var reader = FileCollection.GetFile(fileId).GetExclusiveReader();
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
                                if (valueLen <= MaxValueSizeInlineInMemory && (command & KV2CommandType.SecondParamCompressed) == 0)
                                {
                                    reader.ReadBlock(inlineValueBuf, 0, valueLen);
                                    StoreValueInlineInMemory(ByteBuffer.NewSync(inlineValueBuf, 0, valueLen), out ctx.ValueOfs, out ctx.ValueSize);
                                    ctx.ValueFileId = 0;
                                }
                                else
                                {
                                    reader.SkipBlock(valueLen);
                                }
                                _nextRoot.CreateOrUpdate(ctx);
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

        void StoreValueInlineInMemory(ByteBuffer value, out uint valueOfs, out int valueSize)
        {
            var inlineValueBuf = value.Buffer;
            var valueLen = value.Length;
            var ofs = value.Offset;
            switch (valueLen)
            {
                case 0:
                    valueOfs = 0;
                    valueSize = 0;
                    break;
                case 1:
                    valueOfs = 0;
                    valueSize = 0x1000000 | (inlineValueBuf[ofs] << 16);
                    break;
                case 2:
                    valueOfs = 0;
                    valueSize = 0x2000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8);
                    break;
                case 3:
                    valueOfs = 0;
                    valueSize = 0x3000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 4:
                    valueOfs = inlineValueBuf[ofs + 3];
                    valueSize = 0x4000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 5:
                    valueOfs = (uint)(inlineValueBuf[ofs + 3] | (inlineValueBuf[ofs + 4] << 8));
                    valueSize = 0x5000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 6:
                    valueOfs = (uint)(inlineValueBuf[ofs + 3] | (inlineValueBuf[ofs + 4] << 8) | (inlineValueBuf[ofs + 5] << 16));
                    valueSize = 0x6000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 7:
                    valueOfs = (uint)(inlineValueBuf[ofs + 3] | (inlineValueBuf[ofs + 4] << 8) | (inlineValueBuf[ofs + 5] << 16) | ((uint)inlineValueBuf[ofs + 6] << 24));
                    valueSize = 0x7000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
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
            if (_compactorScheduler != null) _compactorScheduler.Dispose();
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
            var sb = new StringBuilder("KeyValueCount:" + LastCommited.CalcKeyCount() + Environment.NewLine
                                       + "FileCount:" + FileCollection.GetCount() + Environment.NewLine
                                       + "FileGeneration:" + _fileGeneration + Environment.NewLine);
            foreach (var file in _fileCollection.Enumerate())
            {
                IFileInfo fileInfo;
                _fileInfos.TryGetValue(file.Index, out fileInfo);
                sb.AppendFormat("{0} Size:{1} Type:{2} Gen:{3}{4}", file, file.GetSize(),
                                fileInfo.FileType, fileInfo.Generation, Environment.NewLine);
            }
            return sb.ToString();
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
            UpdateTransactionLogInBTreeRoot(btreeRoot);
            //_fileWithTransactionLog.HardFlush();
            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = btreeRoot;
                TryDequeWaiterForWrittingTransaction();
            }
        }

        void UpdateTransactionLogInBTreeRoot(IBTreeRootNode btreeRoot)
        {
            if (btreeRoot.TrLogFileId != _fileIdWithTransactionLog)
            {
                _compactorScheduler.AdviceRunning();
            }
            btreeRoot.TrLogFileId = _fileIdWithTransactionLog;
            btreeRoot.TrLogOffset = (uint)_writerWithTransactionLog.GetCurrentPosition();
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
            UpdateTransactionLogInBTreeRoot(_lastCommited);
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
                    _writerWithTransactionLog = _fileWithTransactionLog.GetAppenderWriter();
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
            _fileWithTransactionLog = FileCollection.AddFile("trl");
            _fileIdWithTransactionLog = _fileWithTransactionLog.Index;
            var transactionLog = new FileTransactionLog(NextGeneration(), _fileIdWithPreviousTransactionLog);
            _writerWithTransactionLog = _fileWithTransactionLog.GetAppenderWriter();
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
            if (valueSize != 0)
            {
                if (valueSize > 0 && valueSize < MaxValueSizeInlineInMemory)
                {
                    StoreValueInlineInMemory(value, out valueOfs, out valueSize);
                    valueFileId = 0;
                }
                else
                {
                    valueFileId = _fileIdWithTransactionLog;
                    valueOfs = (uint)_writerWithTransactionLog.GetCurrentPosition();
                }
                _writerWithTransactionLog.WriteBlock(value);
            }
            else
            {
                valueFileId = 0;
                valueOfs = 0;
            }
        }

        public ByteBuffer ReadValue(uint valueFileId, uint valueOfs, int valueSize)
        {
            if (valueSize == 0) return ByteBuffer.NewEmpty();
            if (valueFileId == 0)
            {
                var len = valueSize >> 24;
                var buf = new byte[len];
                switch (len)
                {
                    case 7:
                        buf[6] = (byte)(valueOfs >> 24);
                        goto case 6;
                    case 6:
                        buf[5] = (byte)(valueOfs >> 16);
                        goto case 5;
                    case 5:
                        buf[4] = (byte)(valueOfs >> 8);
                        goto case 4;
                    case 4:
                        buf[3] = (byte)valueOfs;
                        goto case 3;
                    case 3:
                        buf[2] = (byte)valueSize;
                        goto case 2;
                    case 2:
                        buf[1] = (byte)(valueSize >> 8);
                        goto case 1;
                    case 1:
                        buf[0] = (byte)(valueSize >> 16);
                        break;
                    default:
                        throw new BTDBException("Corrupted DB");
                }
                return ByteBuffer.NewAsync(buf);
            }
            var compressed = false;
            if (valueSize < 0)
            {
                compressed = true;
                valueSize = -valueSize;
            }
            var result = ByteBuffer.NewAsync(new byte[valueSize]);
            var file = FileCollection.GetFile(valueFileId);
            file.RandomRead(result.Buffer, 0, valueSize, valueOfs);
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

        uint CreateKeyIndexFile(IBTreeRootNode root, CancellationToken cancellation)
        {
            var file = FileCollection.AddFile("kvi");
            var writer = file.GetAppenderWriter();
            var keyCount = root.CalcKeyCount();
            var keyIndex = new FileKeyIndex(NextGeneration(), root.TrLogFileId, root.TrLogOffset, keyCount);
            keyIndex.WriteHeader(writer);
            if (keyCount > 0)
            {
                var stack = new List<NodeIdxPair>();
                root.FillStackByIndex(stack, 0);
                do
                {
                    cancellation.ThrowIfCancellationRequested();
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
            file.HardFlush();
            _fileInfos.TryAdd(file.Index, keyIndex);
            return file.Index;
        }

        internal void Compact()
        {
            _compactorScheduler.AdviceRunning();
        }

        internal bool ContainsValuesAndDoesNotTouchGeneration(uint fileId, long dontTouchGeneration)
        {
            IFileInfo info;
            if (!_fileInfos.TryGetValue(fileId, out info)) return false;
            if (info.Generation >= dontTouchGeneration) return false;
            return (info.FileType == KV2FileType.TransactionLog || info.FileType == KV2FileType.PureValues);
        }

        internal AbstractBufferedWriter StartPureValuesFile(out uint fileId)
        {
            var fId = FileCollection.AddFile("pvl");
            fileId = fId.Index;
            var pureValues = new FilePureValues(NextGeneration());
            var writer = fId.GetAppenderWriter();
            _fileInfos[fId.Index] = pureValues;
            pureValues.WriteHeader(writer);
            return writer;
        }

        internal long AtomicallyChangeBTree(Action<IBTreeRootNode> action)
        {
            using (var tr = StartWritingTransaction().Result)
            {
                var newRoot = (tr as KeyValue2DBTransaction).BtreeRoot;
                action(newRoot);
                lock (_writeLock)
                {
                    _lastCommited = newRoot;
                }
                return newRoot.TransactionId;
            }
        }

        internal void MarkAsUnknown(IEnumerable<uint> fileIds)
        {
            foreach (var fileId in fileIds)
            {
                _fileInfos[fileId] = UnknownFile.Instance;
            }
        }

        internal long GetGeneration(uint fileId)
        {
            IFileInfo fileInfo;
            if (fileId == 0) return -1;
            if (!_fileInfos.TryGetValue(fileId, out fileInfo))
            {
                throw new ArgumentOutOfRangeException("fileId");
            }
            return fileInfo.Generation;
        }

        internal void StartedUsingBTreeRoot(IBTreeRootNode btreeRoot)
        {
            lock (_usedBTreeRootNodesLock)
            {
                var uses = btreeRoot.UseCount;
                uses++;
                btreeRoot.UseCount = uses;
                if (uses == 1)
                {
                    _usedBTreeRootNodes.Add(btreeRoot.TransactionId, btreeRoot);
                }
            }
        }

        internal void FinishedUsingBTreeRoot(IBTreeRootNode btreeRoot)
        {
            lock (_usedBTreeRootNodesLock)
            {
                var uses = btreeRoot.UseCount;
                uses--;
                btreeRoot.UseCount = uses;
                if (uses == 0)
                {
                    _usedBTreeRootNodes.Remove(btreeRoot.TransactionId);
                    Monitor.PulseAll(_usedBTreeRootNodesLock);
                }
            }
        }

        internal void WaitForFinishingTransactionsBefore(long transactionId, CancellationToken cancellation)
        {
            lock (_usedBTreeRootNodesLock)
            {
                while (true)
                {
                    cancellation.ThrowIfCancellationRequested();
                    var oldStillRuns = false;
                    foreach (var usedTransactionId in _usedBTreeRootNodes.Keys)
                    {
                        if (usedTransactionId - transactionId >= 0) continue;
                        oldStillRuns = true;
                        break;
                    }
                    if (!oldStillRuns) return;
                    Monitor.Wait(_usedBTreeRootNodesLock, 100);
                }
            }
        }

        internal ulong DistanceFromLastKeyIndex(IBTreeRootNode root)
        {
            var keyIndex = _fileInfos.Where(p => p.Value.FileType == KV2FileType.KeyIndex).Select(p => (IKeyIndex)p.Value).FirstOrDefault();
            if (keyIndex == null)
            {
                if (_fileInfos.Count > 1) return ulong.MaxValue;
                return root.TrLogOffset;
            }
            if (root.TrLogFileId != keyIndex.TrLogFileId) return ulong.MaxValue;
            return root.TrLogOffset - keyIndex.TrLogOffset;
        }
    }
}
