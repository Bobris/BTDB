using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Counts ranged reads of tables, split into reads on the cursor's thread and readahead reads on the dedicated read
/// threads.
sealed class CountingLsmStorage(ILsmStorage inner) : ILsmStorage
{
    int _cursorTableReads;
    int _readaheadTableReads;

    public int CursorTableReads => Volatile.Read(ref _cursorTableReads);
    public int ReadaheadTableReads => Volatile.Read(ref _readaheadTableReads);

    void Count(string key)
    {
        if (!key.StartsWith("tables/")) return;
        if (Thread.CurrentThread.Name?.StartsWith("BTDB.LSM read") == true) Interlocked.Increment(ref _readaheadTableReads);
        else Interlocked.Increment(ref _cursorTableReads);
    }

    public ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
        CancellationToken cancellation) => inner.CreateAsync(key, content, cancellation);

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) =>
        inner.TryReadAsync(key, cancellation);

    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation)
    {
        Count(key);
        return inner.ReadRangeAsync(key, offset, destination, cancellation);
    }

    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        Count(key);
        inner.ReadRange(key, offset, destination);
    }

    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) =>
        inner.ListAsync(prefix, cancellation);

    public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);

    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);

    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);

    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) =>
        inner.ReleaseLeaseAsync(key, leaseId, cancellation);
}

/// Sequential scans read ahead asynchronously instead of missing the cache on every block (section 12.2).
public class LsmReadaheadTest
{
    const int KeyCount = 30_000;

    static byte[] Key(int i)
    {
        var key = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(key, i);
        return key;
    }

    static async Task<InMemoryLsmStorage> Populate()
    {
        var storage = new InMemoryLsmStorage();
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false, BlockSize = 1024
        });
        var random = new Random(1);
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            var value = new byte[40];
            for (var i = 0; i < KeyCount; i++)
            {
                random.NextBytes(value);
                cursor.CreateOrUpdateKeyValue(Key(i), value);
            }

            tr.Commit();
        }

        db.CreateKvi(default);
        await db.Compact(default);
        return storage;
    }

    static LsmKeyValueDBOptions ReaderOptions(ILsmStorage storage) => new()
    {
        Storage = storage, BlockSize = 1024, BlockCache = null, BlockCacheSize = 8 * 1024 * 1024
    };

    [Fact]
    public async Task ForwardScanReadsAhead()
    {
        var storage = new CountingLsmStorage(await Populate());
        using var db = await LsmKeyValueDB.OpenAsync(ReaderOptions(storage));
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var expected = 0;
        if (cursor.FindFirstKey([]))
            do Assert.Equal(Key(expected++), cursor.SlowGetKey());
            while (cursor.FindNextKey([]));
        Assert.Equal(KeyCount, expected);
        Assert.True(storage.ReadaheadTableReads > 0);
        Assert.True(storage.CursorTableReads < 20, $"{storage.CursorTableReads} table reads on the cursor's thread");
    }

    [Fact]
    public async Task BackwardScanReadsAhead()
    {
        var storage = new CountingLsmStorage(await Populate());
        using var db = await LsmKeyValueDB.OpenAsync(ReaderOptions(storage));
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var expected = KeyCount;
        if (cursor.FindLastKey([]))
            do Assert.Equal(Key(--expected), cursor.SlowGetKey());
            while (cursor.FindPreviousKey([]));
        Assert.Equal(0, expected);
        Assert.True(storage.ReadaheadTableReads > 0);
        Assert.True(storage.CursorTableReads < 20, $"{storage.CursorTableReads} table reads on the cursor's thread");
    }

    [Fact]
    public async Task ReadaheadIsCorrectWithATinyCache()
    {
        var storage = new CountingLsmStorage(await Populate());
        using var cache = new LsmBlockCache(64 * 1024);
        using var db = await LsmKeyValueDB.OpenAsync(ReaderOptions(storage) with { BlockCache = cache });
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var expected = 0;
        if (cursor.FindFirstKey([]))
            do Assert.Equal(Key(expected++), cursor.SlowGetKey());
            while (cursor.FindNextKey([]));
        Assert.Equal(KeyCount, expected);
    }
}
