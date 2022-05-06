using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ChunkCache;

public class DiskChunkCache : IChunkCache, IDisposable
{
    readonly IFileCollection _fileCollection;
    readonly int _keySize;
    readonly long _cacheCapacity;
    readonly int _sizeLimitOfOneValueFile;
    readonly int _maxValueFileCount;
    readonly ConcurrentDictionary<ByteStructs.Key20, CacheValue> _cache = new ConcurrentDictionary<ByteStructs.Key20, CacheValue>(new ByteStructs.Key20EqualityComparer());
    readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
    uint _cacheValueFileId;
    IFileCollectionFile? _cacheValueFile;
    ISpanWriter? _cacheValueWriter;
    long _fileGeneration;
    Task? _compactionTask;
    internal Task? CurrentCompactionTask() => _compactionTask;
    CancellationTokenSource? _compactionCts;
    readonly object _startNewValueFileLocker = new object();

    internal static readonly byte[] MagicStartOfFile = { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'C', (byte)'h', (byte)'u', (byte)'n', (byte)'k', (byte)'C', (byte)'a', (byte)'c', (byte)'h', (byte)'e', (byte)'1' };

    struct CacheValue
    {
        internal uint AccessRate;
        internal uint FileId;
        internal uint FileOfs;
        internal uint ContentLength;
    }

    public DiskChunkCache(IFileCollection fileCollection, int keySize, long cacheCapacity)
    {
        if (keySize != 20) throw new NotSupportedException("Only keySize of 20 (Useful for SHA1) is supported for now");
        if (cacheCapacity < 1000) throw new ArgumentOutOfRangeException(nameof(cacheCapacity), "Minimum for cache capacity is 1kB");
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

        if (_cache.Count != 0) return;
        foreach (var collectionFile in _fileInfos.Keys)
        {
            _fileCollection.GetFile(collectionFile).Remove();
        }
        _fileInfos.Clear();
        _fileGeneration = 0;
    }

    void LoadContent()
    {
        SpanReader reader;
        foreach (var collectionFile in _fileCollection.Enumerate())
        {
            reader = new SpanReader(collectionFile.GetExclusiveReader());
            if (!reader.CheckMagic(MagicStartOfFile)) continue; // Don't touch files alien files
            var fileType = (DiskChunkFileType)reader.ReadUInt8();
            var fileInfo = fileType switch
            {
                DiskChunkFileType.HashIndex => new FileHashIndex(ref reader),
                DiskChunkFileType.PureValues => new FilePureValues(ref reader),
                _ => UnknownFile.Instance
            };
            if (_fileGeneration < fileInfo.Generation) _fileGeneration = fileInfo.Generation;
            _fileInfos.TryAdd(collectionFile.Index, fileInfo);
        }
        var hashFilePair =
            _fileInfos.Where(f => f.Value.FileType == DiskChunkFileType.HashIndex).OrderByDescending(
                f => f.Value.Generation).FirstOrDefault();
        if (hashFilePair.Value == null) return;
        reader = new SpanReader(_fileCollection.GetFile(hashFilePair.Key).GetExclusiveReader());
        FileHashIndex.SkipHeader(ref reader);
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
        if (_cache.TryGetValue(k, out var cacheValue))
        {
            return;
        }
        cacheValue.AccessRate = 1;
    again:
        var writer = _cacheValueWriter;
        while (writer == null || writer.GetCurrentPositionWithoutWriter() + content.Length > _sizeLimitOfOneValueFile)
        {
            StartNewValueFile();
            writer = _cacheValueWriter;
        }
        lock (writer)
        {
            if (writer != _cacheValueWriter) goto again;
            cacheValue.FileId = _cacheValueFileId;
            cacheValue.FileOfs = (uint)writer.GetCurrentPositionWithoutWriter();
            var trueWriter = new SpanWriter(writer);
            trueWriter.WriteBlock(content);
            trueWriter.Sync();
            _cacheValueFile!.Flush();
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
                    _cacheValueFile!.HardFlush();
                    SetNewValueFile();
                }
            }
            else
            {
                SetNewValueFile();
            }
            var writer = new SpanWriter(_cacheValueWriter!);
            fileInfo.WriteHeader(ref writer);
            writer.Sync();
            _fileInfos.TryAdd(_cacheValueFileId, fileInfo);
            _compactionCts = new CancellationTokenSource();
            _compactionTask = Task.Factory.StartNew(CompactionCore, _compactionCts!.Token,
                                                    TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
    }

    void SetNewValueFile()
    {
        _cacheValueFile = _fileCollection.AddFile("cav");
        _cacheValueFileId = _cacheValueFile.Index;
        _cacheValueWriter = _cacheValueFile.GetAppenderWriter();
    }

    readonly struct RateFilePair
    {
        internal RateFilePair(ulong accessRate, uint fileId)
        {
            AccessRate = accessRate;
            FileId = fileId;
        }

        internal readonly ulong AccessRate;
        internal readonly uint FileId;
    }

    void CompactionCore()
    {
        var token = _compactionCts!.Token;
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

            usage.TryGetValue(cacheValue.FileId, out var accessRateRunningTotal);
            var accessRate = cacheValue.AccessRate;
            if (maxAccessRate < accessRate) maxAccessRate = accessRate;
            accessRateRunningTotal += accessRate;
            usage[cacheValue.FileId] = accessRateRunningTotal;
        }
        var usageList = new List<RateFilePair>();
        var fileIdsToRemove = new StructList<uint>();
        foreach (var fileInfo in _fileInfos)
        {
            if (fileInfo.Value.FileType != DiskChunkFileType.PureValues) continue;
            if (fileInfo.Key == _cacheValueFileId) continue;
            if (!usage.TryGetValue(fileInfo.Key, out var accessRate) && finishedUsageStats)
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
        foreach (var fileId in fileIdsToRemove)
        {
            _fileCollection.GetFile(fileId).Remove();
            _fileInfos.TryRemove(fileId);
        }
    }

    void FlushCurrentValueFile()
    {
        var writer = _cacheValueWriter;
        if (writer != null)
            lock (writer)
            {
                _cacheValueFile!.HardFlush();
            }
    }

    void PreserveJustMostOftenUsed(uint fileId)
    {
        var frequencies = new List<uint>();
        foreach (var itemPair in _cache)
        {
            if (itemPair.Value.FileId == fileId)
            {
                frequencies.Add(itemPair.Value.AccessRate);
            }
        }
        var preserveRate = frequencies.OrderByDescending(r => r).Skip(frequencies.Count / 5).FirstOrDefault();
        foreach (var itemPair in _cache)
        {
            if (itemPair.Value.FileId == fileId)
            {
                if (preserveRate < itemPair.Value.AccessRate)
                {
                    var cacheValue = itemPair.Value;
                    var content = new byte[cacheValue.ContentLength];
                    _fileCollection.GetFile(cacheValue.FileId).RandomRead(content.AsSpan(0, (int)cacheValue.ContentLength),
                                                                          cacheValue.FileOfs, true);
                    var writer = _cacheValueWriter;
                    if (writer == null)
                    {
                        goto remove;
                    }
                    lock (writer)
                    {
                        if (writer != _cacheValueWriter)
                        {
                            goto remove;
                        }
                        if (writer.GetCurrentPositionWithoutWriter() + cacheValue.ContentLength > _sizeLimitOfOneValueFile)
                        {
                            goto remove;
                        }
                        cacheValue.FileId = _cacheValueFileId;
                        cacheValue.FileOfs = (uint)writer.GetCurrentPositionWithoutWriter();
                        var trueWriter = new SpanWriter(writer);
                        trueWriter.WriteBlock(content);
                        trueWriter.Sync();
                    }
                    _cache.TryUpdate(itemPair.Key, cacheValue, itemPair.Value);
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
        _compactionCts?.Cancel();
        var task = _compactionTask;
        if (task == null) return;
        try
        {
            task.Wait();
        }
        catch
        {
            // ignored because any error is irrelevant due to canceling
        }
    }

    public Task<ByteBuffer> Get(ByteBuffer key)
    {
        if (key.Length != _keySize) throw new ArgumentException("Key has wrong Length not equal to KeySize");
        var tcs = new TaskCompletionSource<ByteBuffer>();
        try
        {
            var k = new ByteStructs.Key20(key);
            if (_cache.TryGetValue(k, out var cacheValue))
            {
                var newCacheValue = cacheValue;
                newCacheValue.AccessRate = cacheValue.AccessRate + 1;
                _cache.TryUpdate(k, newCacheValue, cacheValue);
                // It is not problem if update fails, it will have just lower access rate then real
                var result = new byte[cacheValue.ContentLength];
                _fileCollection.GetFile(cacheValue.FileId).RandomRead(
                    result.AsSpan(0, (int)cacheValue.ContentLength),
                    cacheValue.FileOfs, false);
                tcs.SetResult(ByteBuffer.NewAsync(result));
                return tcs.Task;
            }
        }
        catch
        {
            // It is better to return nothing than throw exception
        }
        tcs.SetResult(ByteBuffer.NewEmpty());
        return tcs.Task;
    }

    public string CalcStats()
    {
        var res = new StringBuilder();
        res.AppendFormat("Files {0} FileInfos {1} FileGeneration {2} Cached items {3}{4}", _fileCollection.GetCount(),
                         _fileInfos.Count, _fileGeneration, _cache.Count, Environment.NewLine);
        var totalSize = 0UL;
        var totalControlledSize = 0UL;
        foreach (var fileCollectionFile in _fileCollection.Enumerate())
        {
            _fileInfos.TryGetValue(fileCollectionFile.Index, out var fileInfo);
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
                totalControlledSize += size;
            }
        }
        res.AppendFormat("TotalSize {0} TotalControlledSize {1} Limit {2}{3}", totalSize, totalControlledSize,
                         _cacheCapacity, Environment.NewLine);
        Debug.Assert(totalControlledSize <= (ulong)_cacheCapacity);
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
        var writerController = file.GetExclusiveAppenderWriter();
        var writer = new SpanWriter(writerController);
        var keyCount = _cache.Count;
        var fileInfo = new FileHashIndex(AllocNewFileGeneration(), _keySize, keyCount);
        _fileInfos.TryAdd(file.Index, fileInfo);
        fileInfo.WriteHeader(ref writer);
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
        writer.WriteVUInt32(0); // Zero FileOfs as End of file mark
        writer.Sync();
        file.HardFlushTruncateSwitchToDisposedMode();
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
