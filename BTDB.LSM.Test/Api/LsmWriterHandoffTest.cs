using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
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

/// Runs a hook before each object creation, e.g. to let a predecessor's delayed request land first.
sealed class CreateHookingStorage(InMemoryLsmStorage inner) : ILsmStorage
{
    public Func<string, ValueTask>? BeforeCreate { get; set; }

    public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation)
    {
        if (BeforeCreate is { } hook) await hook(key);
        return await inner.CreateAsync(key, content, cancellation);
    }

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) => inner.TryReadAsync(key, cancellation);
    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);
    public void ReadRange(string key, long offset, Span<byte> destination) => inner.ReadRange(key, offset, destination);
    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
    public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
}

/// Grants every lease, as a lease that two instances believe they hold, and runs a hook before each object creation.
sealed class OverlappingLeaseStorage(InMemoryLsmStorage inner) : ILsmStorage
{
    public Func<string, ValueTask>? BeforeCreate { get; set; }

    public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation)
    {
        if (BeforeCreate is { } hook) await hook(key);
        return await inner.CreateAsync(key, content, cancellation);
    }

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) => inner.TryReadAsync(key, cancellation);
    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);
    public void ReadRange(string key, long offset, Span<byte> destination) => inner.ReadRange(key, offset, destination);
    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
    public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => ValueTask.FromResult(true);
    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => ValueTask.FromResult(true);
    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => ValueTask.CompletedTask;
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

    static string WalKey(long chunk) => $"wal/{chunk:D16}";

    static bool Exists(IKeyValueDB db, int key)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        return cursor.FindExactKey(Key(key));
    }

    /// Writes chunk 0 (the first writer's epoch marker) and one chunk per commit; commit i writes keys 10i..10i+9.
    static async Task WriteChunks(InMemoryLsmStorage storage, int commits)
    {
        using var first = await LsmKeyValueDB.OpenAsync(Options(storage) with { AcquireWriterLeaseOnOpen = true });
        for (var i = 0; i < commits; i++)
        {
            await Write(first, i * 10, 10, (ulong)i + 1);
            await first.FlushAsync();
        }

        Assert.Equal(Enumerable.Range(0, commits + 1).Select(n => WalKey(n)), storage.Keys("wal/"));
    }

    [Fact]
    public async Task TakeoversFromTheSameStateGetDifferentEpochs()
    {
        var storage = new InMemoryLsmStorage();
        using (var first = await LsmKeyValueDB.OpenAsync(Options(storage) with { AcquireWriterLeaseOnOpen = true }))
        {
            await Write(first, 0, 10, 1);
            await first.FlushAsync();
        }

        var before = storage.Keys("manifest/").Count();
        // Two instances believe they hold the lease, and both read the same state before either creates its fencing
        // manifest.
        var arrived = 0;
        var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        OverlappingLeaseStorage Overlapping() => new(storage)
        {
            BeforeCreate = async key =>
            {
                if (!key.StartsWith("manifest/", StringComparison.Ordinal)) return;
                if (Interlocked.Increment(ref arrived) == 2) both.SetResult();
                await both.Task;
            }
        };
        // Manifests are created synchronously, so each takeover needs its own thread to wait at the barrier.
        var dbs = await Task.WhenAll(new[] { Overlapping(), Overlapping() }.Select(s =>
            Task.Run(() => LsmKeyValueDB.OpenAsync(Options(s) with { AcquireWriterLeaseOnOpen = true }).AsTask())));
        try
        {
            // Before the fix both fencing manifests were equal, so the second takeover took the first one's manifest
            // for its own and both wrote with one epoch. Now the second is fenced and takes over again after it.
            var fences = storage.Keys("manifest/").Skip(before)
                .Select(key => LsmManifest.Decode(storage.Peek(key)!, key)).ToArray();
            Assert.Equal(2, fences.Length);
            Assert.True(fences[0].WriterEpoch < fences[1].WriterEpoch);
        }
        finally
        {
            foreach (var db in dbs) db.Dispose();
        }
    }

    [Fact]
    public async Task TakeoverFencesEveryHoleSoAcknowledgedCommitsSurviveTheNextOpen()
    {
        var storage = new InMemoryLsmStorage();
        await WriteChunks(storage, 6);
        // The first writer crashed during its last parallel round: chunks 3 and 5 never landed.
        await storage.DeleteAsync(WalKey(3), default);
        await storage.DeleteAsync(WalKey(5), default);

        using (var second = await LsmKeyValueDB.OpenAsync(Options(storage)))
        {
            await Write(second, 1000, 10, 100);
            await second.FlushAsync();
        }

        // Before the fix the second writer fenced only name 3; replay stopped at the hole at 5 and lost its
        // acknowledged commit.
        using var third = await LsmKeyValueDB.OpenAsync(Options(storage));
        Assert.Equal((30, 100UL), Read(third));
        Assert.True(Exists(third, 1000));
    }

    [Fact]
    public async Task PredecessorChunkInANewWritersNameEndsThatWritersHistory()
    {
        var storage = new InMemoryLsmStorage();
        await WriteChunks(storage, 6);
        // The first writer's last round, chunks 4 to 6, is still in flight; chunk 6 lands late.
        var stale = storage.Peek(WalKey(6))!;
        foreach (var chunk in new[] { 4, 5, 6 }) await storage.DeleteAsync(WalKey(chunk), default);

        var hooked = new CreateHookingStorage(storage);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        hooked.BeforeCreate = async key =>
        {
            if (key == WalKey(5)) await gate.Task;
            if (key == WalKey(6)) await storage.CreateAsync(key, stale, default);
        };
        using (var second = await LsmKeyValueDB.OpenAsync(Options(hooked)))
        {
            // The marker takes name 4, and the first own chunk 5 waits until twenty small commits are queued, so they
            // share the next upload: chunks 6, 7 and so on. The stale chunk takes name 6 and fences this writer.
            await Write(second, 1000, 1, 100);
            for (var i = 0; i < 20; i++) await Write(second, 2000 + i * 3, 3, 101 + (ulong)i);
            gate.SetResult();
            var deadline = Stopwatch.StartNew();
            while (second.HoldsWriterLease && deadline.Elapsed < TimeSpan.FromSeconds(10)) await Task.Delay(10);
            Assert.False(second.HoldsWriterLease);
        }

        Assert.Contains(WalKey(7), storage.Keys("wal/"));
        // Replay keeps the commits in chunk 5 and nothing of the second writer after the stale chunk, so the visible
        // commits are a prefix of the second writer's commits. Before the fix it also applied chunk 7, whose commits
        // follow the missing ones of chunk 6. A few small commits may share chunk 5 with the first one.
        using var third = await LsmKeyValueDB.OpenAsync(Options(storage));
        var visible = Enumerable.Range(0, 20).Select(i => Exists(third, 2000 + i * 3)).ToArray();
        var prefix = visible.TakeWhile(v => v).Count();
        Assert.DoesNotContain(true, visible.Skip(prefix));
        Assert.True(prefix < 20);
        Assert.Equal((31 + prefix * 3, 100UL + (ulong)prefix), Read(third));
    }
}
