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

namespace BTDBTest;

public class DiskChunkCacheCoverageTest
{
    readonly ThreadLocal<HashAlgorithm> _sha1 = new(SHA1.Create);

    ByteBuffer Hash(byte[] content) => ByteBuffer.NewAsync(_sha1.Value!.ComputeHash(content));

    static IFileCollectionFile AddRawFile(IFileCollection files, byte[] content)
    {
        var file = files.AddFile("x");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(content);
        writer.Flush();
        file.HardFlush();
        return file;
    }

    [Fact]
    public void ConstructorValidatesArguments()
    {
        using var files = new InMemoryFileCollection();
        Assert.Throws<NotSupportedException>(() => new DiskChunkCache(files, 32, 100000));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DiskChunkCache(files, 20, 999));
        // Very big capacity splits into more value files, but allocates nothing upfront
        using var big = new DiskChunkCache(files, 20, 100_000_000_000);
        Assert.Contains("Files 0", big.CalcStats());
    }

    [Fact]
    public async Task PutValidatesArgumentsAndIgnoresDuplicates()
    {
        using var files = new InMemoryFileCollection();
        using var cache = new DiskChunkCache(files, 20, 100000);
        var content = new byte[100];
        new Random(1).NextBytes(content);
        Assert.Throws<ArgumentException>(() => cache.Put(ByteBuffer.NewAsync(new byte[19]), ByteBuffer.NewAsync(content)));
        Assert.Throws<ArgumentException>(() => cache.Put(Hash(content), ByteBuffer.NewEmpty()));
        cache.Put(Hash(content), ByteBuffer.NewAsync(content));
        var other = new byte[100];
        cache.Put(Hash(content), ByteBuffer.NewAsync(other));
        Assert.Equal(content, (await cache.Get(Hash(content))).ToByteArray());
    }

    [Fact]
    public void AlienFilesAreUntouchedAndUnknownCacheFilesAreRemoved()
    {
        using var files = new InMemoryFileCollection();
        var alien = AddRawFile(files, "Not a cache file at all"u8.ToArray());
        // Cache magic with unknown type
        AddRawFile(files, [.. DiskChunkCache.MagicStartOfFile, 99]);
        using (var cache = new DiskChunkCache(files, 20, 100000))
        {
            var stats = cache.CalcStats();
            Assert.Contains("Unknown to cache", stats);
            Assert.Equal(1u, files.GetCount());
            Assert.Same(alien, files.GetFile(alien.Index));
        }
    }

    [Fact]
    public async Task FileWithCorruptedHeaderDoesNotDisableCachePersistence()
    {
        using var files = new InMemoryFileCollection();
        var content = new byte[100];
        new Random(2).NextBytes(content);
        var content2 = new byte[200];
        new Random(4).NextBytes(content2);
        using (var cache = new DiskChunkCache(files, 20, 100000))
        {
            cache.Put(Hash(content), ByteBuffer.NewAsync(content));
        }

        // Truncated cache file header (for example after crash during its creation)
        AddRawFile(files, [.. DiskChunkCache.MagicStartOfFile, 0, 1]);
        using (var cache = new DiskChunkCache(files, 20, 100000))
        {
            Assert.Equal(content, (await cache.Get(Hash(content))).ToByteArray());
            cache.Put(Hash(content2), ByteBuffer.NewAsync(content2));
        }

        using (var cache = new DiskChunkCache(files, 20, 100000))
        {
            Assert.Equal(content, (await cache.Get(Hash(content))).ToByteArray());
            Assert.Equal(content2, (await cache.Get(Hash(content2))).ToByteArray());
        }
    }

    [Fact]
    public async Task CorruptedHashIndexStartsWithEmptyCache()
    {
        using var files = new InMemoryFileCollection();
        var content = new byte[100];
        new Random(2).NextBytes(content);
        using (var cache = new DiskChunkCache(files, 20, 100000))
        {
            cache.Put(Hash(content), ByteBuffer.NewAsync(content));
        }

        // Replace hash index by its truncated copy, header stays readable, but entries are incomplete
        foreach (var file in files.Enumerate().ToArray())
        {
            var data = new byte[file.GetSize()];
            file.RandomRead(data, 0, false);
            if (data[DiskChunkCache.MagicStartOfFile.Length] != 0) continue;
            file.Remove();
            AddRawFile(files, data[..^3]);
        }

        using (var cache = new DiskChunkCache(files, 20, 100000))
        {
            Assert.Equal(0, (await cache.Get(Hash(content))).Length);
        }
    }

    [Fact]
    public async Task GetOfValueFromRemovedFileReturnsEmpty()
    {
        using var files = new InMemoryFileCollection();
        var content = new byte[100];
        new Random(3).NextBytes(content);
        using var cache = new DiskChunkCache(files, 20, 100000);
        cache.Put(Hash(content), ByteBuffer.NewAsync(content));
        foreach (var file in files.Enumerate().ToArray()) file.Remove();
        Assert.Equal(0, (await cache.Get(Hash(content))).Length);
    }
}
