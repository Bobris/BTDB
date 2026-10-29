using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.ChunkCache;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
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

    // Value files written before values had checksums are dropped: the cache starts empty instead of trusting them.
    [Fact]
    public void ValueFilesWithoutChecksumsAreDropped()
    {
        using var fileCollection = new InMemoryFileCollection();
        var valueFile = fileCollection.AddFile("cav");
        var valueWriter = new MemWriter(valueFile.GetAppenderWriter());
        valueWriter.WriteByteArrayRaw(DiskChunkCache.MagicStartOfFile);
        valueWriter.WriteUInt8((byte)DiskChunkFileType.PureValues);
        valueWriter.WriteVInt64(1);
        var contentOfs = (uint)valueWriter.GetCurrentPosition();
        valueWriter.WriteBlock(new byte[] { 7, 8, 9 });
        valueWriter.Flush();
        valueFile.HardFlushTruncateSwitchToDisposedMode();

        var key = CalcHash([7, 8, 9]);
        var indexFile = fileCollection.AddFile("chi");
        var indexWriter = new MemWriter(indexFile.GetExclusiveAppenderWriter());
        new FileHashIndex(2, 20, 1).WriteHeader(ref indexWriter);
        indexWriter.WriteVUInt32(contentOfs);
        indexWriter.WriteVUInt32(valueFile.Index);
        indexWriter.WriteVUInt32(1);
        indexWriter.WriteVUInt32(3);
        indexWriter.WriteBlock(key.AsSyncReadOnlySpan());
        indexWriter.WriteVUInt32(0);
        indexWriter.Flush();
        indexFile.HardFlushTruncateSwitchToDisposedMode();

        using var cache = new DiskChunkCache(fileCollection, 20, 100000);
        Assert.Equal(0, cache.Get(key).Result.Length);
        Assert.Equal(0u, fileCollection.GetCount());
    }

    // Before the fix entries of a damaged hash index were loaded as they were: a damaged length made Get allocate it,
    // and an entry pointing past its value file or into an unknown file was kept.
    [Fact]
    public void DamagedHashIndexEntriesAreSkipped()
    {
        using var fileCollection = new InMemoryFileCollection();
        var valueFile = fileCollection.AddFile("cav");
        var valueWriter = new MemWriter(valueFile.GetAppenderWriter());
        new BTDB.ChunkCache.FilePureValues(1).WriteHeader(ref valueWriter);
        var contentOfs = (uint)valueWriter.GetCurrentPosition();
        valueWriter.WriteBlock(new byte[] { 7, 8, 9 });
        valueWriter.WriteUInt64LE(XxHash64.Hash([7, 8, 9]));
        valueWriter.Flush();
        valueFile.HardFlushTruncateSwitchToDisposedMode();

        var keys = Enumerable.Range(0, 4).Select(i => CalcHash([(byte)i])).ToArray();
        var indexFile = fileCollection.AddFile("chi");
        var indexWriter = new MemWriter(indexFile.GetExclusiveAppenderWriter());
        new FileHashIndex(2, 20, keys.Length).WriteHeader(ref indexWriter);
        foreach (var (key, ofs, fileId, length) in new[]
                 {
                     (keys[0], contentOfs, valueFile.Index, 3u), (keys[1], contentOfs, valueFile.Index, 0x7FFFFFF0u),
                     (keys[2], contentOfs + 2, valueFile.Index, 3u), (keys[3], contentOfs, 999u, 3u)
                 })
        {
            indexWriter.WriteVUInt32(ofs);
            indexWriter.WriteVUInt32(fileId);
            indexWriter.WriteVUInt32(1);
            indexWriter.WriteVUInt32(length);
            indexWriter.WriteBlock(key.AsSyncReadOnlySpan());
        }

        indexWriter.WriteVUInt32(0);
        indexWriter.Flush();
        indexFile.HardFlushTruncateSwitchToDisposedMode();

        using var cache = new DiskChunkCache(fileCollection, 20, 100000);
        Assert.Equal(new byte[] { 7, 8, 9 }, cache.Get(keys[0]).Result.AsSyncReadOnlySpan().ToArray());
        var before = GC.GetTotalAllocatedBytes(true);
        for (var i = 1; i < keys.Length; i++) Assert.Equal(0, cache.Get(keys[i]).Result.Length);
        Assert.True(GC.GetTotalAllocatedBytes(true) - before < 1_000_000);
    }

    // Before the fix content larger than one value file made Put start new value files forever.
    [Fact]
    public void ContentLargerThanValueFileIsNotCached()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var cache = new DiskChunkCache(fileCollection, 20, 1000);
        var key = CalcHash([1]);
        var put = Task.Run(() => cache.Put(key, ByteBuffer.NewAsync(new byte[200])));
        Assert.True(put.Wait(TimeSpan.FromSeconds(10)));
        Assert.Equal(0, cache.Get(key).Result.Length);
        cache.Put(key, ByteBuffer.NewAsync(new byte[50]));
        Assert.Equal(50, cache.Get(key).Result.Length);
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
