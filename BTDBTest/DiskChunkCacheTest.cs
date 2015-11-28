using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using BTDB.Buffer;
using BTDB.ChunkCache;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class DiskChunkCacheTest
    {
        readonly ThreadLocal<HashAlgorithm> _hashAlg = new ThreadLocal<HashAlgorithm>(() => new SHA1CryptoServiceProvider());

        ByteBuffer CalcHash(byte[] bytes)
        {
            return ByteBuffer.NewAsync(_hashAlg.Value.ComputeHash(bytes));
        }

        [Fact]
        public void CreateEmptyCache()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (new DiskChunkCache(fileCollection, 20, 1000))
            {
            }
        }

        [Fact]
        public void GetFromEmptyCacheReturnsEmptyByteBuffer()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
            {
                Assert.Equal(0, cache.Get(CalcHash(new byte[] { 0 })).Result.Length);
            }
        }

        [Fact]
        public void WhatIPutICanGet()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
            {
                cache.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }));
                Assert.Equal(new byte[] { 1 }, cache.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
            }
        }

        [Fact]
        public void ItRemebersContentAfterReopen()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
                {
                    cache.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }));
                }
                using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
                {
                    Assert.Equal(new byte[] { 1 }, cache.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
                }
            }
        }

        [Fact]
        public void SizeDoesNotGrowOverLimit()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                const int cacheCapacity = 50000;
                using (var cache = new DiskChunkCache(fileCollection, 20, cacheCapacity))
                {
                    for (var i = 0; i < 80; i++)
                    {
                        Put(cache, i);
                        Assert.True(fileCollection.Enumerate().Sum(f => (long)f.GetSize()) <= cacheCapacity);
                    }
                }
            }
        }

        void Put(IChunkCache cache, int i)
        {
            var content = new byte[1024];
            PackUnpack.PackInt32BE(content, 0, i);
            cache.Put(CalcHash(content), ByteBuffer.NewAsync(content));
        }

        bool Get(IChunkCache cache, int i)
        {
            var content = new byte[1024];
            PackUnpack.PackInt32BE(content, 0, i);
            return cache.Get(CalcHash(content)).Result.Length == 1024;
        }

        [Fact]
        public void GettingContentMakesItStayLongerIncreasingRate()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                const int cacheCapacity = 50000;
                using (var cache = new DiskChunkCache(fileCollection, 20, cacheCapacity))
                {
                    for (var i = 0; i < 80; i++)
                    {
                        Put(cache, i);
                        for (var j = 0; j < i; j++)
                            Get(cache, i);
                        Assert.True(fileCollection.Enumerate().Sum(f => (long)f.GetSize()) <= cacheCapacity);
                    }
                    Assert.True(Get(cache, 79));
                    Assert.False(Get(cache, 0));
                }
            }
        }

        [Fact]
        public void GettingContentMakesItStayLongerDecreasingRate()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                const int cacheCapacity = 50000;
                using (var cache = new DiskChunkCache(fileCollection, 20, cacheCapacity))
                {
                    for (var i = 0; i < 80; i++)
                    {
                        Put(cache, i);
                        for (var j = 0; j < 79 - i; j++)
                            Get(cache, i);
                        Assert.True(fileCollection.Enumerate().Sum(f => (long)f.GetSize()) <= cacheCapacity);
                    }
                    Console.WriteLine(cache.CalcStats());
                    Assert.True(Get(cache, 0));
                    Assert.False(Get(cache, 60));
                }
            }
        }

        [Fact]
        public void AccessEveryTenthTenTimesMoreMakesItStay()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                const int cacheCapacity = 50000;
                using (var cache = new DiskChunkCache(fileCollection, 20, cacheCapacity))
                {
                    for (var i = 0; i < 46; i++)
                    {
                        Put(cache, i);
                        for (var j = 0; j < (i % 5 == 0 ? 10 + i : 1); j++)
                            Get(cache, i);
                        if (i==42) Thread.Sleep(500);
                        Assert.True(fileCollection.Enumerate().Sum(f => (long)f.GetSize()) <= cacheCapacity);
                    }
                    Console.WriteLine(cache.CalcStats());
                    Assert.True(Get(cache, 0));
                    Assert.False(Get(cache, 1));
                }
            }
        }

    }
}