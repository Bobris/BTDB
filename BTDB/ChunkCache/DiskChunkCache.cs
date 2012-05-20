using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        readonly ConcurrentDictionary<Key20, CacheValue> _cache = new ConcurrentDictionary<Key20, CacheValue>(new Key20EqualityComparer());
        IFileCollectionFile _cacheValueFile;
        AbstractBufferedWriter _cacheValueWriter;

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

            internal readonly ulong V1;
            internal readonly ulong V2;
            internal readonly uint V3;
        }

        struct CacheValue
        {
            internal DateTime LastAccess;
            internal uint FileId;
            internal uint FileOfs;
            internal uint ContentLength;
        }

        public DiskChunkCache(IFileCollection fileCollection, int keySize, long cacheCapacity)
        {
            if (keySize != 20) throw new NotSupportedException("Only keySize of 20 (Usefull for SHA1) is supported for now");
            _fileCollection = fileCollection;
            _keySize = keySize;
            _cacheCapacity = cacheCapacity;
            _cacheValueFile = _fileCollection.AddFile("cav");
            _cacheValueWriter = _cacheValueFile.GetAppenderWriter();
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
            cacheValue.LastAccess = DateTime.UtcNow;
            cacheValue.FileId = 0;
            cacheValue.FileOfs = (uint)_cacheValueWriter.GetCurrentPosition();
            _cacheValueWriter.WriteBlock(content);
            cacheValue.ContentLength = (uint)content.Length;
            _cache.TryAdd(k, cacheValue);
        }

        public Task<ByteBuffer> Get(ByteBuffer key)
        {
            if (key.Length != _keySize) throw new ArgumentException("Key has wrong Length not equal to KeySize");
            var tcs = new TaskCompletionSource<ByteBuffer>();
            var k = new Key20(key);
            CacheValue cacheValue;
            if (!_cache.TryGetValue(k, out cacheValue))
            {
                tcs.SetResult(ByteBuffer.NewEmpty());
            }
            else
            {
                cacheValue.LastAccess = DateTime.UtcNow;
                _cache[k] = cacheValue;
                var result = new byte[cacheValue.ContentLength];
                _cacheValueFile.RandomRead(result, 0, (int)cacheValue.ContentLength, cacheValue.FileOfs);
                tcs.SetResult(ByteBuffer.NewAsync(result));
            }
            return tcs.Task;
        }

        public void Dispose()
        {
        }
    }
}