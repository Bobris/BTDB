using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Counts whole-object reads per object kind.
sealed class AccessCountingStorage(InMemoryLsmStorage inner) : ILsmStorage
{
    readonly Dictionary<string, int> _reads = new();

    static string Kind(string key) => key[..(key.IndexOf('/') + 1)];

    void Count(string key)
    {
        lock (_reads) _reads[Kind(key)] = _reads.GetValueOrDefault(Kind(key)) + 1;
    }

    public int Reads(string kind)
    {
        lock (_reads) return _reads.GetValueOrDefault(kind);
    }

    public ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation) => inner.CreateAsync(key, content, cancellation);

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation)
    {
        Count(key);
        return inner.TryReadAsync(key, cancellation);
    }

    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);
    public void ReadRange(string key, long offset, Span<byte> destination) => inner.ReadRange(key, offset, destination);

    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);

    public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
}

/// Handing the writer role between instances (ImplementationPlan.md, section 14).
public class LsmWriterHandoffTest
{
    static LsmKeyValueDBOptions Options(ILsmStorage storage) => new()
    {
        Storage = storage, MemtableSize = 1 << 20, BlockSize = 512, WalChunkTargetSize = 2048,
        WalFlushInterval = TimeSpan.FromMilliseconds(1), RefreshRetryInterval = TimeSpan.FromMilliseconds(5),
        LeaseDuration = TimeSpan.FromSeconds(30), LeaseRetryInterval = TimeSpan.FromSeconds(1)
    };

    static byte[] Key(int i) => [(byte)(i >> 8), (byte)i];

    static async Task Write(LsmKeyValueDB db, int from, int count, ulong commitUlong)
    {
        using var tr = await db.StartWritingTransaction();
        using var cursor = tr.CreateCursor();
        for (var i = from; i < from + count; i++) cursor.CreateOrUpdateKeyValue(Key(i), new byte[100]);
        tr.SetCommitUlong(commitUlong);
        tr.Commit();
    }

    static (long Count, ulong CommitUlong) Read(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        return (tr.GetKeyValueCount(), tr.GetCommitUlong());
    }

    [Fact]
    public async Task ReleasedLeaseIsTakenOverAtOnceWithTheLastCommit()
    {
        var storage = new InMemoryLsmStorage();
        using var first = await LsmKeyValueDB.OpenAsync(Options(storage) with { AcquireWriterLeaseOnOpen = true });
        using var second = await LsmKeyValueDB.OpenAsync(Options(storage));
        await Write(first, 0, 100, 1);
        await first.ReleaseWriterLeaseAsync();
        Assert.False(first.HoldsWriterLease);
        Assert.Equal((100, 1UL), Read(first));

        // The lease lasts 30 s, so only the release lets the second instance write this soon.
        var stopwatch = Stopwatch.StartNew();
        await Write(second, 100, 100, 2);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(10), stopwatch.Elapsed.ToString());
        Assert.Equal((200, 2UL), Read(second));

        // And back: the first instance catches up from storage once, then takes over from the second.
        await first.WaitForCommitUlongAsync(2);
        Assert.Equal((200, 2UL), Read(first));
        await second.ReleaseWriterLeaseAsync();
        await Write(first, 200, 100, 3);
        Assert.Equal((300, 3UL), Read(first));
        await second.WaitForCommitUlongAsync(3);
        Assert.Equal((300, 3UL), Read(second));
    }

    [Fact]
    public async Task TakeoverContinuesTheReaderReplayInsteadOfReloading()
    {
        var storage = new AccessCountingStorage(new InMemoryLsmStorage());
        using var first = await LsmKeyValueDB.OpenAsync(Options(storage) with { AcquireWriterLeaseOnOpen = true });
        using var second = await LsmKeyValueDB.OpenAsync(Options(storage));
        for (var commit = 0; commit < 50; commit++) await Write(first, commit * 10, 10, (ulong)commit + 1);
        await first.FlushAsync();
        await second.WaitForCommitUlongAsync(50);
        await Write(first, 1000, 10, 51);
        await first.ReleaseWriterLeaseAsync();

        var walReads = storage.Reads("wal/");
        using (var tr = await second.StartWritingTransaction())
        {
            // Only the chunk of the last commit is new to the second instance; the other WAL was replayed before.
            Assert.InRange(storage.Reads("wal/") - walReads, 1, 3);
            Assert.Equal(51UL, tr.GetCommitUlong());
            Assert.Equal(510, tr.GetKeyValueCount());
        }
    }

    [Fact]
    public async Task WritersWaitingDuringReleaseTakeTheLeaseAgain()
    {
        var storage = new InMemoryLsmStorage();
        using var db = await LsmKeyValueDB.OpenAsync(Options(storage) with { AcquireWriterLeaseOnOpen = true });
        var running = await db.StartWritingTransaction();
        var release = db.ReleaseWriterLeaseAsync().AsTask();
        var waiting = db.StartWritingTransaction().AsTask();
        using (var cursor = running.CreateCursor()) cursor.CreateOrUpdateKeyValue(Key(1), new byte[10]);
        Assert.False(release.IsCompleted);
        running.Commit();
        running.Dispose();
        await release;
        using (var tr = await waiting)
        {
            Assert.True(db.HoldsWriterLease);
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key(2), new byte[10]);
            tr.Commit();
        }

        Assert.Equal(2, Read(db).Count);
    }

    [Fact]
    public async Task DisposedWriterLeavesFlushesToTheNextWriter()
    {
        var storage = new InMemoryLsmStorage();
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage));
        using (var writer = await LsmKeyValueDB.OpenAsync(Options(storage) with { AcquireWriterLeaseOnOpen = true }))
        {
            for (var commit = 0; commit < 20; commit++) await Write(writer, commit * 10, 10, (ulong)commit + 1);
        }

        Assert.Empty(storage.Keys("tables/"));
        await Write(reader, 1000, 10, 21);
        reader.CreateKvi(default);
        Assert.NotEmpty(storage.Keys("tables/"));
        Assert.Equal((210, 21UL), Read(reader));
    }
}
