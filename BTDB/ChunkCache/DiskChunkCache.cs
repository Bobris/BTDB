using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KV2DBLayer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ChunkCache
{
    class DiskChunkCache : IChunkCache, IDisposable
    {
        readonly IFileCollection _fileCollection;
        readonly int _keySize;
        readonly long _cacheCapacity;
        readonly int _sizeLimitOfOneValueFile;
        readonly int _maxValueFileCount;
        readonly ConcurrentDictionary<ByteStructs.Key20, CacheValue> _cache = new ConcurrentDictionary<ByteStructs.Key20, CacheValue>(new ByteStructs.Key20EqualityComparer());
        readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
        uint _cacheValueFileId;
        IFileCollectionFile _cacheValueFile;
        AbstractBufferedWriter _cacheValueWriter;
        long _fileGeneration;
        Task _compactionTask;
        CancellationTokenSource _compactionCts;
        readonly object _startNewValueFileLocker = new object();

        internal static readonly byte[] MagicStartOfFile = new[] { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'C', (byte)'h', (byte)'u', (byte)'n', (byte)'k', (byte)'C', (byte)'a', (byte)'c', (byte)'h', (byte)'e', (byte)'1' };

        struct CacheValue
        {
            internal uint AccessRate;
            internal uint FileId;
            internal uint FileOfs;
            internal uint ContentLength;
        }

        public DiskChunkCache(IFileCollection fileCollection, int keySize, long cacheCapacity)
        {
            if (keySize != 20) throw new NotSupportedException("Only keySize of 20 (Usefull for SHA1) is supported for now");
            if (cacheCapacity < 1000) throw new ArgumentOutOfRangeException("cacheCapacity", "Minimum for cache capacity is 1kB");
            _fileCollection = fileCollection;
            _keySize = keySize;
            _cacheCapacity = cacheCapacity;
            cacheCapacity = cacheCapacity / 1000 * (980 - keySize); // decrease for size of HashIndex
            if (cacheCapacity / 8 > int.MaxValue)
            {
                _maxValueFileCount = checked((int)(cacheCapacity / int.MaxValue));
                _sizeLimitOfOneValueFile = int.MaxValue;
            }
            else
            {
                _maxValueFileCount = 8;
                _sizeLimitOfOneValueFile = (int)(cacheCapacity / 8);
            }
            try
            {
                LoadContent();
            }
            catch
            {
                _cache.Clear();
            }
            if (_cache.Count == 0)
            {
                foreach (var collectionFile in _fileInfos.Keys)
                {
                    _fileCollection.GetFile(collectionFile).Remove();
                }
                _fileInfos.Clear();
                _fileGeneration = 0;
            }
        }

        void LoadContent()
        {
            AbstractBufferedReader reader;
            foreach (var collectionFile in _fileCollection.Enumerate())
            {
                reader = collectionFile.GetExclusiveReader();
                if (!reader.CheckMagic(MagicStartOfFile)) continue; // Don't touch files alien files
                var fileType = (DiskChunkFileType)reader.ReadUInt8();
                IFileInfo fileInfo;
                switch (fileType)
                {
                    case DiskChunkFileType.HashIndex:
                        fileInfo = new FileHashIndex(reader);
                        break;
                    case DiskChunkFileType.PureValues:
                        fileInfo = new FilePureValues(reader);
                        break;
                    default:
                        fileInfo = UnknownFile.Instance;
                        break;
                }
                if (_fileGeneration < fileInfo.Generation) _fileGeneration = fileInfo.Generation;
                _fileInfos.TryAdd(collectionFile.Index, fileInfo);
            }
            var hashFilePair =
                _fileInfos.Where(f => f.Value.FileType == DiskChunkFileType.HashIndex).OrderByDescending(
                    f => f.Value.Generation).FirstOrDefault();
            if (hashFilePair.Value == null) return;
            reader = _fileCollection.GetFile(hashFilePair.Key).GetExclusiveReader();
            FileHashIndex.SkipHeader(reader);
            if (((FileHashIndex)hashFilePair.Value).KeySize != _keySize) return;
            var keyBuf = ByteBuffer.NewSync(new byte[_keySize]);
            while (true)
            {
                var cacheValue = new CacheValue();
                cacheValue.FileOfs = reader.ReadVUInt32();
                if (cacheValue.FileOfs == 0) break;
                cacheValue.FileId = reader.ReadVUInt32();
                cacheValue.AccessRate = reader.ReadVUInt32();
                cacheValue.ContentLength = reader.ReadVUInt32();
                reader.ReadBlock(keyBuf);
                _cache.TryAdd(new ByteStructs.Key20(keyBuf), cacheValue);
            }
        }

        public void Put(ByteBuffer key, ByteBuffer content)
        {
            if (key.Length != _keySize) throw new ArgumentException("Key has wrong Length not equal to KeySize");
            if (content.Length == 0) throw new ArgumentException("Empty Content cannot be stored");
            var k = new ByteStructs.Key20(key);
            CacheValue cacheValue;
            if (_cache.TryGetValue(k, out cacheValue))
            {
                return;
            }
            cacheValue.AccessRate = 1;
        again:
            var writer = _cacheValueWriter;
            while (writer == null || writer.GetCurrentPosition() + content.Length > _sizeLimitOfOneValueFile)
            {
                StartNewValueFile();
                writer = _cacheValueWriter;
            }
            lock (writer)
            {
                if (writer != _cacheValueWriter) goto again;
                cacheValue.FileId = _cacheValueFileId;
                cacheValue.FileOfs = (uint)_cacheValueWriter.GetCurrentPosition();
                _cacheValueWriter.WriteBlock(content);
            }
            cacheValue.ContentLength = (uint)content.Length;
            _cache.TryAdd(k, cacheValue);
        }

        void StartNewValueFile()
        {
            lock (_startNewValueFileLocker)
            {
                QuickFinishCompaction();
                var fileInfo = new FilePureValues(AllocNewFileGeneration());
                if (_cacheValueWriter != null)
                {
                    lock (_cacheValueWriter)
                    {
                        _cacheValueFile.HardFlush();
                        SetNewValueFile();
                    }
                }
                else
                {
                    SetNewValueFile();
                }
                fileInfo.WriteHeader(_cacheValueWriter);
                _fileInfos.TryAdd(_cacheValueFileId, fileInfo);
                _compactionCts = new CancellationTokenSource();
                _compactionTask = Task.Factory.StartNew(CompactionCore, _compactionCts.Token,
                                                        TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }
        }

        void SetNewValueFile()
        {
            _cacheValueFile = _fileCollection.AddFile("cav");
            _cacheValueFileId = _cacheValueFile.Index;
            _cacheValueWriter = _cacheValueFile.GetAppenderWriter();
        }

        internal struct RateFilePair
        {
            internal RateFilePair(ulong accessRate, uint fileId)
            {
                AccessRate = accessRate;
                FileId = fileId;
            }

            internal ulong AccessRate;
            internal uint FileId;
        }

        void CompactionCore()
        {
            var token = _compactionCts.Token;
            var usage = new Dictionary<uint, ulong>();
            var finishedUsageStats = true;
            uint maxAccessRate = 0;
            foreach (var cacheValue in _cache.Values)
            {
                if (token.IsCancellationRequested)
                {
                    finishedUsageStats = false;
                    break;
                }
                ulong accessRateRunningTotal;
                usage.TryGetValue(cacheValue.FileId, out accessRateRunningTotal);
                uint accessRate = cacheValue.AccessRate;
                if (maxAccessRate < accessRate) maxAccessRate = accessRate;
                accessRateRunningTotal += accessRate;
                usage[cacheValue.FileId] = accessRateRunningTotal;
            }
            var usageList = new List<RateFilePair>();
            var fileIdsToRemove = new List<uint>();
            foreach (var fileInfo in _fileInfos)
            {
                if (fileInfo.Value.FileType != DiskChunkFileType.PureValues) continue;
                if (fileInfo.Key == _cacheValueFileId) continue;
                ulong accessRate;
                if (!usage.TryGetValue(fileInfo.Key, out accessRate) && finishedUsageStats)
                {
                    fileIdsToRemove.Add(fileInfo.Key);
                    continue;
                }
                usageList.Add(new RateFilePair(accessRate, fileInfo.Key));
            }
            usageList.Sort((a, b) => a.AccessRate > b.AccessRate ? -1 : a.AccessRate < b.AccessRate ? 1 : 0);
            while (usageList.Count >= _maxValueFileCount)
            {
                var fileId = usageList.Last().FileId;
                if (usageList.Count == _maxValueFileCount)
                    PreserveJustMostOftenUsed(fileId);
                else
                    ClearFileFromCache(fileId);
                fileIdsToRemove.Add(fileId);
                usageList.RemoveAt(usageList.Count - 1);
            }
            FlushCurrentValueFile();
            StoreHashIndex();
            foreach (var fileid in fileIdsToRemove)
            {
                _fileCollection.GetFile(fileid).Remove();
                _fileInfos.TryRemove(fileid);
            }
        }

        void FlushCurrentValueFile()
        {
            var writer = _cacheValueWriter;
            if (writer != null)
                lock (writer)
                {
                    _cacheValueFile.HardFlush();
                }
        }

        void PreserveJustMostOftenUsed(uint fileId)
        {
            var freqencies = new List<uint>();
            foreach (var itemPair in _cache)
            {
                if (itemPair.Value.FileId == fileId)
                {
                    freqencies.Add(itemPair.Value.AccessRate);
                }
            }
            var preserveRate = freqencies.OrderByDescending(r => r).Skip(freqencies.Count / 5).FirstOrDefault();
            foreach (var itemPair in _cache)
            {
                if (itemPair.Value.FileId == fileId)
                {
                    if (preserveRate<itemPair.Value.AccessRate)
                    {
                        var cacheValue = itemPair.Value;
                        var content = new byte[cacheValue.ContentLength];
                        _fileCollection.GetFile(cacheValue.FileId).RandomRead(content, 0, (int)cacheValue.ContentLength,
                                                                              cacheValue.FileOfs);
                        var writer = _cacheValueWriter;
                        if (writer == null) goto remove;
                        lock (writer)
                        {
                            if (writer != _cacheValueWriter) goto remove;
                            if (writer.GetCurrentPosition() + cacheValue.ContentLength > _sizeLimitOfOneValueFile)
                                goto remove;
                            cacheValue.FileId = _cacheValueFileId;
                            cacheValue.FileOfs = (uint)_cacheValueWriter.GetCurrentPosition();
                            _cacheValueWriter.WriteBlock(content);
                        }
                        _cache.TryUpdate(itemPair.Key,cacheValue,itemPair.Value);
                        continue;
                    }
                remove:
                    _cache.TryRemove(itemPair.Key);
                }
            }
        }

        void ClearFileFromCache(uint fileId)
        {
            foreach (var itemPair in _cache)
            {
                if (itemPair.Value.FileId == fileId)
                {
                    _cache.TryRemove(itemPair.Key);
                }
            }
        }

        void QuickFinishCompaction()
        {
            var compactionCTS = _compactionCts;
            if (compactionCTS != null) compactionCTS.Cancel();
            var task = _compactionTask;
            if (task != null)
            {
                try
                {
                    task.Wait();
                }
                catch { }
            }
        }

        public Task<ByteBuffer> Get(ByteBuffer key)
        {
            if (key.Length != _keySize) throw new ArgumentException("Key has wrong Length not equal to KeySize");
            var tcs = new TaskCompletionSource<ByteBuffer>();
            try
            {
                var k = new ByteStructs.Key20(key);
                CacheValue cacheValue;
                if (_cache.TryGetValue(k, out cacheValue))
                {
                    var newCacheValue = cacheValue;
                    newCacheValue.AccessRate = cacheValue.AccessRate + 1;
                    _cache.TryUpdate(k, newCacheValue, cacheValue);
                    // It is not problem if update fails, it will have just lower access rate then real
                    var result = new byte[cacheValue.ContentLength];
                    _fileCollection.GetFile(cacheValue.FileId).RandomRead(result, 0, (int)cacheValue.ContentLength,
                                                                          cacheValue.FileOfs);
                    tcs.SetResult(ByteBuffer.NewAsync(result));
                    return tcs.Task;
                }
            }
            catch { } // It is better to return nothing than throw exception
            tcs.SetResult(ByteBuffer.NewEmpty());
            return tcs.Task;
        }

        public string CalcStats()
        {
            var res = new StringBuilder();
            res.AppendFormat("Files {0} FileInfos {1} FileGeneration {2} Cached items {3}{4}", _fileCollection.GetCount(),
                             _fileInfos.Count, _fileGeneration, _cache.Count, Environment.NewLine);
            var totalSize = 0UL;
            var totalControledSize = 0UL;
            foreach (var fileCollectionFile in _fileCollection.Enumerate())
            {
                IFileInfo fileInfo;
                _fileInfos.TryGetValue(fileCollectionFile.Index, out fileInfo);
                var size = fileCollectionFile.GetSize();
                totalSize += size;
                if (fileInfo == null)
                {
                    res.AppendFormat("{0} Size: {1} Unknown to cache{2}", fileCollectionFile.Index,
                                     size, Environment.NewLine);
                }
                else
                {
                    res.AppendFormat("{0} Size: {1} Type: {2} {3}", fileCollectionFile.Index,
                                     size, fileInfo.FileType, Environment.NewLine);
                    totalControledSize += size;
                }
            }
            res.AppendFormat("TotalSize {0} TotalControledSize {1} Limit {2}{3}", totalSize, totalControledSize,
                             _cacheCapacity, Environment.NewLine);
            Debug.Assert(totalControledSize <= (ulong)_cacheCapacity);
            return res.ToString();
        }

        public void Dispose()
        {
            lock (_startNewValueFileLocker)
            {
                QuickFinishCompaction();
                FlushCurrentValueFile();
                StoreHashIndex();
            }
        }

        void StoreHashIndex()
        {
            RemoveAllHashIndexAndUnknownFiles();
            var file = _fileCollection.AddFile("chi");
            var writer = file.GetAppenderWriter();
            var keyCount = _cache.Count;
            var fileInfo = new FileHashIndex(AllocNewFileGeneration(), _keySize, keyCount);
            _fileInfos.TryAdd(file.Index, fileInfo);
            fileInfo.WriteHeader(writer);
            var keyBuf = ByteBuffer.NewSync(new byte[_keySize]);
            foreach (var cachePair in _cache)
            {
                cachePair.Key.FillBuffer(keyBuf);
                writer.WriteVUInt32(cachePair.Value.FileOfs);
                writer.WriteVUInt32(cachePair.Value.FileId);
                writer.WriteVUInt32(cachePair.Value.AccessRate);
                writer.WriteVUInt32(cachePair.Value.ContentLength);
                writer.WriteBlock(keyBuf);
            }
            writer.WriteVUInt32(0); // Zero FileOfs as end of file mark
            file.HardFlush();
        }

        void RemoveAllHashIndexAndUnknownFiles()
        {
            foreach (var infoPair in _fileInfos)
            {
                if (infoPair.Value.FileType == DiskChunkFileType.HashIndex ||
                    infoPair.Value.FileType == DiskChunkFileType.Unknown)
                {
                    var fileId = infoPair.Key;
                    _fileCollection.GetFile(fileId).Remove();
                    _fileInfos.TryRemove(fileId);
                }
            }
        }

        long AllocNewFileGeneration()
        {
            return Interlocked.Increment(ref _fileGeneration);
        }
    }
}