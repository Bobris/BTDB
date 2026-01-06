using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.ChunkCache;
using BTDB.KVDBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class DiskChunkCacheTest
{
    readonly ITestOutputHelper _output;
    readonly ThreadLocal<HashAlgorithm> _hashAlg = new(SHA1.Create);

    public DiskChunkCacheTest(ITestOutputHelper output)
    {
        _output = output;
    }

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
    public async Task GetFromEmptyCacheReturnsEmptyByteBuffer()
    {
        using (var fileCollection = new InMemoryFileCollection())
        using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
        {
            Assert.Equal(0, (await cache.Get(CalcHash(new byte[] { 0 }))).Length);
        }
    }

    [Fact]
    public async Task WhatIPutICanGet()
    {
        using (var fileCollection = new InMemoryFileCollection())
        using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
        {
            cache.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }));
            Assert.Equal(new byte[] { 1 }, (await cache.Get(CalcHash(new byte[] { 0 }))).ToByteArray());
        }
    }

    [Fact]
    public async Task ItRemebersContentAfterReopen()
    {
        using (var fileCollection = new InMemoryFileCollection())
        {
            using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
            {
                cache.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }));
            }
            using (var cache = new DiskChunkCache(fileCollection, 20, 1000))
            {
                Assert.Equal(new byte[] { 1 }, (await cache.Get(CalcHash(new byte[] { 0 }))).ToByteArray());
            }
        }
    }

    [Fact]
    public async Task SizeDoesNotGrowOverLimit()
    {
        using (var fileCollection = new InMemoryFileCollection())
        {
            const int cacheCapacity = 50000;
            using (var cache = new DiskChunkCache(fileCollection, 20, cacheCapacity))
            {
                for (var i = 0; i < 80; i++)
                {
                    Put(cache, i);
                    if (CalcLength(fileCollection) <= cacheCapacity) continue;
                    await FinishCompactTask(cache);
                    Assert.True(CalcLength(fileCollection) <= cacheCapacity);
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

    async Task<bool> Get(IChunkCache cache, int i)
    {
        var content = new byte[1024];
        PackUnpack.PackInt32BE(content, 0, i);
        return (await cache.Get(CalcHash(content))).Length == 1024;
    }

    [Fact]
    public async Task GettingContentMakesItStayLongerIncreasingRate()
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
                        await Get(cache, i);
                    if (CalcLength(fileCollection) <= cacheCapacity) continue;
                    await FinishCompactTask(cache);
                    Assert.True(CalcLength(fileCollection) <= cacheCapacity);
                }
                Assert.True(await Get(cache, 79));
                Assert.False(await Get(cache, 0));
            }
        }
    }

    [Fact]
    public async Task GettingContentMakesItStayLongerDecreasingRate()
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
                        await Get(cache, i);
                    if (CalcLength(fileCollection) <= cacheCapacity) continue;
                    await FinishCompactTask(cache);
                    Assert.True(CalcLength(fileCollection) <= cacheCapacity);
                }
                _output.WriteLine(cache.CalcStats());
                Assert.True(await Get(cache, 0));
                Assert.False(await Get(cache, 60));
            }
        }
    }

    async Task FinishCompactTask(DiskChunkCache cache)
    {
        var t = cache.CurrentCompactionTask();
        if (t == null) return;
        await t;
    }

    long CalcLength(IFileCollection fileCollection) =>
        fileCollection.Enumerate().Sum(f => (long)f.GetSize());

    [Fact]
    public async Task AccessEveryTenthTenTimesMoreMakesItStay()
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
                        await Get(cache, i);
                    if (i == 42) Thread.Sleep(500);
                    Assert.True(fileCollection.Enumerate().Sum(f => (long)f.GetSize()) <= cacheCapacity);
                }
                _output.WriteLine(cache.CalcStats());
                Assert.True(await Get(cache, 0));
                Assert.False(await Get(cache, 1));
            }
        }
    }

}
