using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KV2DBLayer;
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
        readonly ConcurrentDictionary<Key20, CacheValue> _cache = new ConcurrentDictionary<Key20, CacheValue>(new Key20EqualityComparer());
        readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
        uint _cacheValueFileId;
        IFileCollectionFile _cacheValueFile;
        AbstractBufferedWriter _cacheValueWriter;
        long _fileGeneration;

        internal static readonly byte[] MagicStartOfFile = new[] { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'C', (byte)'h', (byte)'u', (byte)'n', (byte)'k', (byte)'C', (byte)'a', (byte)'c', (byte)'h', (byte)'e', (byte)'1' };

        class Key20EqualityComparer : IEqualityComparer<Key20>
        {
            public bool Equals(Key20 x, Key20 y)
            {
                return x.V1 == y.V1 && x.V2 == y.V2 && x.V3 == y.V3;
            }

            public int GetHashCode(Key20 obj)
            {
                return (int)obj.V1;
            }
        }

        struct Key20
        {
            internal Key20(ByteBuffer value)
            {
                V1 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset);
                V2 = PackUnpack.UnpackUInt64LE(value.Buffer, value.Offset + 8);
                V3 = PackUnpack.UnpackUInt32LE(value.Buffer, value.Offset + 16);
            }

            internal void FillBuffer(ByteBuffer buf)
            {
                var o = buf.Offset;
                PackUnpack.PackUInt64LE(buf.Buffer, o, V1);
                PackUnpack.PackUInt64LE(buf.Buffer, o + 8, V2);
                PackUnpack.PackUInt32LE(buf.Buffer, o + 16, V3);
            }

            internal readonly ulong V1;
            internal readonly ulong V2;
            internal readonly uint V3;
        }

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
                _sizeLimitOfOneValueFile = (int) (cacheCapacity / 8);
            }
            try
            {
                LoadContent();
            }
            catch
            {
                _fileInfos.Clear();
                // Corrupted cache storage better to clear everything and start clean
                foreach (var collectionFile in _fileCollection.Enumerate())
                {
                    collectionFile.Remove();
                }
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
                _cache.TryAdd(new Key20(keyBuf), cacheValue);
            }
        }

        public void Put(ByteBuffer key, ByteBuffer content)
        {
            if (key.Length != _keySize) throw new ArgumentException("Key has wrong Length not equal to KeySize");
            if (content.Length == 0) throw new ArgumentException("Empty Content cannot be stored");
            var k = new Key20(key);
            CacheValue cacheValue;
            if (_cache.TryGetValue(k, out cacheValue))
            {
                return;
            }
            cacheValue.AccessRate = 1;
            if (_cacheValueWriter == null)
            {
                StartNewValueFile();
            }
            cacheValue.FileId = _cacheValueFileId;
            cacheValue.FileOfs = (uint)_cacheValueWriter.GetCurrentPosition();
            _cacheValueWriter.WriteBlock(content);
            cacheValue.ContentLength = (uint)content.Length;
            _cache.TryAdd(k, cacheValue);
        }

        void StartNewValueFile()
        {
            var fileInfo = new FilePureValues(AllocNewFileGeneration());
            _cacheValueFile = _fileCollection.AddFile("cav");
            _cacheValueFileId = _cacheValueFile.Index;
            _cacheValueWriter = _cacheValueFile.GetAppenderWriter();
            fileInfo.WriteHeader(_cacheValueWriter);
            _fileInfos.TryAdd(_cacheValueFileId, fileInfo);
        }

        public Task<ByteBuffer> Get(ByteBuffer key)
        {
            if (key.Length != _keySize) throw new ArgumentException("Key has wrong Length not equal to KeySize");
            var tcs = new TaskCompletionSource<ByteBuffer>();
            try
            {
                var k = new Key20(key);
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

        public void Dispose()
        {
            if (_cacheValueWriter != null)
            {
                _cacheValueWriter.FlushBuffer();
            }
            StoreHashIndex();
        }

        void StoreHashIndex()
        {
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
            writer.FlushBuffer();
            file.HardFlush();
        }

        long AllocNewFileGeneration()
        {
            return Interlocked.Increment(ref _fileGeneration);
        }
    }
}