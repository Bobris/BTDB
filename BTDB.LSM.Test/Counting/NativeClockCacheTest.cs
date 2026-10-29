using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.LSM.Format;
using Xunit;

namespace BTDB.LSM.Test.Counting;

public class NativeClockCacheTest
{
    static byte[] Block(ulong file, long offset, int size)
    {
        var data = new byte[size];
        BitConverter.TryWriteBytes(data, file);
        BitConverter.TryWriteBytes(data.AsSpan(8), offset);
        return data;
    }

    [Fact]
    public void EvictsUnpinnedEntriesWithinBudget()
    {
        using var cache = new NativeClockCache(64 * 1024, 4);
        var loads = 0;
        for (var i = 0; i < 1000; i++)
        {
            using var pin = cache.Pin(new(0, 1), i, () =>
            {
                loads++;
                return Block(1, i, 1024);
            });
            Assert.Equal(i, BitConverter.ToInt64(pin.Span[8..]));
        }

        Assert.Equal(1000, loads);
        Assert.True(cache.SizeBytes <= 64 * 1024 + 1024 * 4, $"{cache.SizeBytes}");
    }

    // Before the fix skips past pinned blocks used up the eviction budget: with blocks 1 and 3 pinned the hand cleared
    // the references of 0 and 2, evicted 0 and stopped, leaving the shard over capacity with block 2 unpinned.
    [Fact]
    public void EvictionPastPinnedBlocksReachesCapacity()
    {
        using var cache = new NativeClockCache(2 * 1024, 1);
        var pin0 = cache.Pin(new(0, 1), 0, () => Block(1, 0, 1024));
        using var pin1 = cache.Pin(new(0, 1), 1, () => Block(1, 1, 1024));
        var pin2 = cache.Pin(new(0, 1), 2, () => Block(1, 2, 1024));
        pin0.Dispose();
        pin2.Dispose();
        using var pin3 = cache.Pin(new(0, 1), 3, () => Block(1, 3, 1024));
        Assert.Equal(2 * 1024, cache.SizeBytes);
        Assert.False(cache.Contains(new(0, 1), 2));
    }

    // Before the fix a block loaded while the cache was disposed was added to it anyway, so it was never freed.
    [Fact]
    public void BlockLoadedWhileDisposingIsNotKept()
    {
        var cache = new NativeClockCache(64 * 1024, 1);
        var pin = cache.Pin(new(0, 1), 0, () =>
        {
            cache.Dispose();
            return Block(1, 0, 1024);
        });
        Assert.Equal(1ul, BitConverter.ToUInt64(pin.Span));
        Assert.False(cache.Contains(new(0, 1), 0));
        Assert.Equal(0, cache.SizeBytes);
        pin.Dispose();
    }

    [Fact]
    public void PinnedEntryStaysReadableAfterEvictionOfItsFile()
    {
        using var cache = new NativeClockCache(16 * 1024, 1);
        var pin = cache.Pin(new(0, 7), 0, () => Block(7, 0, 4096));
        cache.Evict(new(0, 7));
        for (var i = 1; i < 100; i++)
            using (cache.Pin(new(0, 8), i, () => Block(8, i, 4096)))
            {
            }

        Assert.Equal(7ul, BitConverter.ToUInt64(pin.Span));
        pin.Dispose();
        var reloaded = false;
        using (cache.Pin(new(0, 7), 0, () =>
               {
                   reloaded = true;
                   return Block(7, 0, 4096);
               }))
        {
        }

        Assert.True(reloaded);
    }

    [Fact]
    public void ConcurrentReadersSeeConsistentBlocks()
    {
        using var cache = new NativeClockCache(256 * 1024, 8);
        var errors = 0;
        Parallel.For(0, 8, worker =>
        {
            var random = new Random(worker);
            for (var i = 0; i < 20_000; i++)
            {
                var file = (ulong)random.Next(4);
                var offset = random.Next(200);
                using var pin = cache.Pin(new(0, file), offset, () => Block(file, offset, 2048));
                if (BitConverter.ToUInt64(pin.Span) != file || BitConverter.ToInt64(pin.Span[8..]) != offset)
                    Interlocked.Increment(ref errors);
                if (i % 5000 == 0) cache.Evict(new(0, (ulong)random.Next(4)));
            }
        });
        Assert.Equal(0, errors);
        Assert.True(cache.Hits > 0 && cache.Misses > 0);
    }
}
