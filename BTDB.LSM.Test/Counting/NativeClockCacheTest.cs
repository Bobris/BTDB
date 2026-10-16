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
