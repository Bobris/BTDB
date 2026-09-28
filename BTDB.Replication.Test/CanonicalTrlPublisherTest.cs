using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class CanonicalTrlPublisherTest
{
    internal enum Fault { None, LostResponse, DelayEffect, CancelAfterEffect, Reject }
    internal sealed record Blob(TrlObjectState State, byte[] Bytes);
    internal sealed record Request(TrlWrite Write, byte[] Suffix);

    internal sealed class Storage : IReplicationStorage
    {
        public async IAsyncEnumerable<TrlHead> EnumerateTrlsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellation)
        {
            foreach (var (key, blob) in Blobs.ToArray())
            {
                cancellation.ThrowIfCancellationRequested();
                yield return new(TrlFileName.FileIdFromKey(key), key, blob.State);
            }
        }
        public IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation) => throw new NotSupportedException("This test storage only supports canonical TRLs.");
        public ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation) => throw new NotSupportedException("This test storage only supports canonical TRLs.");
        public ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) => throw new NotSupportedException("This test storage only supports canonical TRLs.");
        public ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map, CancellationToken cancellation) => throw new NotSupportedException("This test storage only supports canonical TRLs.");
        public IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync(CancellationToken cancellation) => throw new NotSupportedException("This test storage only supports canonical TRLs.");
        public ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation) => throw new NotSupportedException("This test storage only supports canonical TRLs.");
        public ValueTask<bool> ProtectPureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) => throw new NotSupportedException("This test storage only supports canonical TRLs.");

        public readonly Dictionary<string, Blob> Blobs = new();
        public readonly List<Request> Requests = new();
        public readonly Queue<Action> Delayed = new();
        public Func<int, Fault>? Inject;
        public Action<TrlWrite>? BeforeEffect;
        public Action<uint>? BeforeRangeRead;
        public long RangeBytes;
        int _version;
        public int Applied;
        public int MaximumRangeRead;

        public ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.FromResult(Blobs.GetValueOrDefault(key)?.State);
        }
        public ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            BeforeRangeRead?.Invoke(offset);
            ct.ThrowIfCancellationRequested();
            var blob = Blobs[key];
            if (token != blob.State.Token) throw new IOException("Selected TRL version changed.");
            MaximumRangeRead = Math.Max(MaximumRangeRead, destination.Length);
            RangeBytes += destination.Length;
            blob.Bytes.AsMemory((int)offset, destination.Length).CopyTo(destination);
            return ValueTask.CompletedTask;
        }
        public ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var suffix = new byte[write.AppendLength];
            for (uint offset = 0; offset < suffix.Length;)
            {
                var count = (int)Math.Min(257u, (uint)suffix.Length - offset);
                write.ReadAppend(offset, suffix.AsSpan((int)offset, count));
                offset += (uint)count;
            }
            Requests.Add(new(write, suffix));
            var fault = Inject?.Invoke(Requests.Count) ?? Fault.None;
            if (fault == Fault.DelayEffect)
            {
                Delayed.Enqueue(() => Apply(write, suffix));
                return ValueTask.FromResult(new TrlWriteResult(TrlWriteOutcome.Ambiguous));
            }
            if (fault == Fault.Reject) return ValueTask.FromResult(new TrlWriteResult(TrlWriteOutcome.Rejected));
            var result = Apply(write, suffix);
            if (fault == Fault.CancelAfterEffect) throw new OperationCanceledException("Reply cancelled after effect");
            return ValueTask.FromResult(fault == Fault.LostResponse ? new TrlWriteResult(TrlWriteOutcome.Ambiguous) : result);
        }
        TrlWriteResult Apply(TrlWrite write, byte[] suffix)
        {
            BeforeEffect?.Invoke(write);
            var previous = Blobs.GetValueOrDefault(write.Key);
            if (previous?.State.Token != write.ExpectedToken || (previous?.State.Length ?? 0) != write.ExpectedLength)
                return new(TrlWriteOutcome.Rejected);
            var bytes = new byte[write.Length];
            previous?.Bytes.CopyTo(bytes, 0);
            suffix.CopyTo(bytes, (int)write.ExpectedLength);
            var state = new TrlObjectState((++_version).ToString(), write.Length, write.Sha256);
            Blobs[write.Key] = new(state, bytes);
            Applied++;
            return new(TrlWriteOutcome.Applied, state);
        }
        public void CompleteDelayed() { while (Delayed.TryDequeue(out var effect)) effect(); }
    }

    sealed class TinyLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint fileId) => new(1024, 1536);
    }

    sealed class Fixture : IDisposable
    {
        public readonly InMemoryReplicationFileStorage Files;
        public readonly TransactionLogCapture Capture;
        public readonly BTreeKeyValueDB Db;
        public readonly Storage Remote = new();
        public readonly DeterministicScheduler Clock = new(781);
        public readonly LeaseAuthority Authority;
        public readonly CanonicalTrlPublisher Publisher;
        Fixture(InMemoryReplicationFileStorage files, TransactionLogCapture capture, BTreeKeyValueDB db)
        {
            Files = files;
            Capture = capture;
            Db = db;
            Authority = Lease(Clock);
            Publisher = new(Db, Capture, Remote, Authority, 1, Key);
        }
        public static async Task<Fixture> CreateAsync(bool tinyLogs = true)
        {
            var files = new InMemoryReplicationFileStorage();
            var capture = new TransactionLogCapture();
            try { return new(files, capture, await OpenAsync(files, capture, tinyLogs)); }
            catch { files.Dispose(); throw; }
        }
        public void Dispose()
        {
            Publisher.Dispose();
            Db.Dispose();
            Files.Dispose();
        }
    }

    static string Key(uint id) => $"{id}.trl";
    static LeaseAuthority Lease(DeterministicScheduler clock)
    {
        var authority = new LeaseAuthority(clock.CreateScope(Guid.NewGuid().ToString()), 0, TimeSpan.FromTicks(1));
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromSeconds(10)));
        return authority;
    }
    static ValueTask<BTreeKeyValueDB> OpenAsync(InMemoryReplicationFileStorage files, TransactionLogCapture? capture = null, bool tinyLogs = true) => BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
    {
        FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = capture, Compression = new NoCompressionStrategy(),
        CompactorScheduler = null, FileSplitSize = 1024 * 1024,
        TransactionLogSizeStrategy = tinyLogs ? new TinyLogs() : null
    });
    static async Task Write(Fixture f, ulong id, int count = 1, bool rollback = false, bool batch = false)
    {
        using var tr = await f.Db.StartWritingTransaction(id, batch);
        using var cursor = tr.CreateCursor();
        for (var i = 0; i < count; i++) cursor.CreateOrUpdateKeyValue([(byte)id, (byte)i], Enumerable.Repeat((byte)id, 700).ToArray());
        if (!rollback) tr.Commit();
    }

    static async Task<(ulong EventId, long Keys)> Restore(Storage storage, bool tinyLogs = true)
    {
        using var files = new InMemoryReplicationFileStorage();
        var selected = await CanonicalTrlInventory.DiscoverAsync(storage, new(Key(1), 1));
        await using var collection = new ReplicationFileSet(files, selected);
        await collection.InitializeAsync();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = collection,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null,
            TransactionLogSizeStrategy = tinyLogs ? new TinyLogs() : null
        });
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Span<byte> keyBuffer = default;
        Span<byte> valueBuffer = default;
        for (var found = cursor.FindFirstKey([]); found; found = cursor.FindNextKey([]))
        {
            var keyBytes = cursor.GetKeySpan(ref keyBuffer);
            var value = cursor.GetValueSpan(ref valueBuffer);
            if (keyBytes.Length == 2) Assert.Equal(Enumerable.Repeat(keyBytes[0], 700), value.ToArray());
            else Assert.Equal("metadata"u8.ToArray(), value.ToArray());
        }
        return (tr.GetCommitUlong(), tr.GetKeyValueCount());
    }

    [Fact]
    public async Task InconsistentRestoredTailConflictsInsteadOfShrinkingHistory()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1, 8);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        var tail = f.Publisher.Tail!;
        var ahead = tail with { State = tail.State with { Length = tail.State.Length + 16 } };
        using var restored = new CanonicalTrlPublisher(f.Db, f.Capture, f.Remote, f.Authority, 1, Key, ahead);
        var requests = f.Remote.Requests.Count;
        Assert.Equal(TrlPublishResult.Conflict, await restored.PublishNextAsync());
        Assert.Equal(requests + 1, f.Remote.Requests.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CheckpointCutPublishesWithoutWaitingForLaterLocalTransactions(bool tinyLogs)
    {
        using var f = await Fixture.CreateAsync(tinyLogs);
        await Write(f, 1, 8);
        using var snapshot = f.Db.CaptureKeyIndexSnapshot();
        var cut = new TransactionLogPosition(snapshot.TransactionLogFileId, snapshot.TransactionLogOffset);
        await Write(f, 2, 8);
        var latest = f.Capture.Completed;
        Assert.NotEqual(cut, latest);

        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishThroughAsync(cut));
        Assert.Equal(cut, f.Publisher.PublishedPosition);
        Assert.Equal(cut, f.Capture.Acknowledged);
        Assert.Equal((1ul, 8L), await Restore(f.Remote, tinyLogs));
        var requests = f.Remote.Requests.Count;
        Assert.Equal(TrlPublishResult.Idle, await f.Publisher.PublishThroughAsync(cut));
        Assert.Equal(requests, f.Remote.Requests.Count);

        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Equal(latest, f.Publisher.PublishedPosition);
        Assert.Equal((2ul, 16L), await Restore(f.Remote, tinyLogs));
    }

    [Fact]
    public async Task CheckpointCutFinishesEarlierAmbiguityBeforePublishingItsOwnPrefix()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1, 8);
        f.Remote.Inject = n => n == 1 ? Fault.DelayEffect : Fault.None;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        await Write(f, 2, 8);
        var cut = f.Capture.Completed;
        await Write(f, 3);
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishThroughAsync(cut));
        Assert.Single(f.Remote.Requests);
        f.Remote.CompleteDelayed();

        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishThroughAsync(cut));
        Assert.Equal(cut, f.Publisher.PublishedPosition);
        Assert.Equal((2ul, 16L), await Restore(f.Remote));
    }

    [Fact]
    public async Task CheckpointCutDoesNotReplaceAnAlreadyDispatchedLaterPrefix()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1);
        var cut = f.Capture.Completed;
        await Write(f, 2);
        var dispatched = f.Capture.Completed;
        f.Remote.Inject = n => n == 1 ? Fault.DelayEffect : Fault.None;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        await Write(f, 3);

        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishThroughAsync(cut, retryPending: true));
        Assert.Equal(dispatched, f.Publisher.PublishedPosition);
        Assert.Equal(f.Remote.Requests[0].Write, f.Remote.Requests[1].Write);
        f.Remote.CompleteDelayed();
        Assert.Equal((2ul, 2L), await Restore(f.Remote, false));
    }

    [Fact]
    public async Task CheckpointCutDoesNotReportSuccessWhenAuthorityIsLostWithPreparedSuccessors()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        var published = f.Publisher.PublishedPosition;
        await Write(f, 2, 8);
        var cut = f.Capture.Completed;
        f.Remote.BeforeEffect = _ => f.Authority.Fence();

        Assert.Equal(TrlPublishResult.AuthorityLost, await f.Publisher.PublishThroughAsync(cut));
        Assert.Equal(published, f.Publisher.PublishedPosition);
        Assert.Equal((1ul, 1L), await Restore(f.Remote));
    }

    [Fact]
    public async Task CheckpointCutAdoptsTheRestoredTailBeforeAppendingInTheSelectedTerm()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        var selected = f.Publisher.Tail!;
        await Write(f, 2);
        var cut = f.Capture.Completed;
        await Write(f, 3);
        using var next = new CanonicalTrlPublisher(f.Db, f.Capture, f.Remote, Lease(f.Clock), 2, Key, selected);

        Assert.Equal(TrlPublishResult.Published, await next.PublishThroughAsync(cut));
        Assert.Equal(0u, f.Remote.Requests[1].Write.AppendLength);
        Assert.NotNull(f.Remote.Requests[1].Write.ExpectedToken);
        Assert.Equal(cut, next.PublishedPosition);
        Assert.Equal((2ul, 2L), await Restore(f.Remote, false));
    }

    [Fact]
    public async Task CancelledCheckpointReplyRetainsTheOriginalCutWhileLocalWorkContinues()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1);
        var cut = f.Capture.Completed;
        f.Remote.Inject = _ => Fault.CancelAfterEffect;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => f.Publisher.PublishThroughAsync(cut).AsTask());
        await Write(f, 2);

        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishThroughAsync(cut));
        Assert.Single(f.Remote.Requests);
        Assert.Equal(cut, f.Capture.Acknowledged);
        Assert.Equal((1ul, 1L), await Restore(f.Remote, false));
    }

    [Fact]
    public async Task CheckpointCutRejectsUnknownOrFuturePositionsBeforeDispatch()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        var completed = f.Capture.Completed;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => f.Publisher.PublishThroughAsync(default).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            f.Publisher.PublishThroughAsync(completed with { Offset = completed.Offset + 1 }).AsTask());
        Assert.Empty(f.Remote.Requests);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PublishesRealNativeCommitsAndRollbackAcrossFiles(bool batch, bool rollback)
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1, batch: batch);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        var prefix = f.Remote.Blobs[Key(1)].Bytes.ToArray();
        await Write(f, 2, 10, rollback, batch);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.True(f.Remote.Requests.Count > 3);
        Assert.Equal(prefix, f.Remote.Blobs[Key(1)].Bytes[..prefix.Length]);
        Assert.Equal((rollback ? 1ul : 2ul, rollback ? 1L : 11L), await Restore(f.Remote));
        Assert.Equal(f.Capture.Completed, f.Publisher.PublishedPosition);
        Assert.Equal(TrlPublishResult.Idle, await f.Publisher.PublishNextAsync());
        // A later transaction may start in another file without an in-transaction rotation record.
        await Write(f, 3);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Equal((3ul, rollback ? 2L : 12L), await Restore(f.Remote));
    }

    static async Task PublishEachOf(Fixture f, int transactions)
    {
        var rotations = 0;
        for (ulong id = 1; id <= (ulong)transactions; id++)
        {
            var published = f.Publisher.PublishedPosition;
            await Write(f, id);
            if (published.FileId != 0 && f.Capture.Completed.FileId != published.FileId) rotations++;
            Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        }
        Assert.True(rotations > 0);
    }

    [Fact]
    public async Task RotationSealsEveryPredecessorWithItsWholeFileChecksum()
    {
        using var f = await Fixture.CreateAsync();
        await PublishEachOf(f, 12);
        // A tail already published to its end gets one metadata-only write: the one that seals it.
        Assert.All(f.Remote.Requests.Where(r => r.Write.ExpectedToken != null && r.Write.AppendLength == 0),
            r => Assert.NotNull(r.Write.Sha256));
        var sealing = f.Remote.Requests.Where(r => r.Write.Sha256 != null).Select(r => r.Write.FileId).ToList();
        Assert.Equal(sealing.Count, sealing.Distinct().Count());
        var inventory = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
        var files = new List<RemoteFile>();
        await foreach (var file in inventory.EnumerateAsync(default)) files.Add(file);
        Assert.True(files.Count > 1);
        foreach (var file in files[..^1])
            Assert.Equal(Convert.ToHexString(SHA256.HashData(f.Remote.Blobs[Key(file.FileId)].Bytes)), file.Sha256);
        Assert.Null(files[^1].Sha256);
        Assert.Equal((12ul, 12L), await Restore(f.Remote));
    }

    [Fact]
    public async Task WarmRestoreReusesCachedSealedTrlsAndDownloadsOnlyTheTail()
    {
        using var f = await Fixture.CreateAsync();
        await PublishEachOf(f, 12);
        using var cache = new InMemoryReplicationFileStorage();
        for (var pass = 0; pass < 2; pass++)
        {
            var inventory = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
            var before = f.Remote.RangeBytes;
            await using (var collection = new ReplicationFileSet(cache, inventory))
            {
                await collection.InitializeAsync();
                using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
                {
                    FileCollection = collection, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
                    TransactionLogSizeStrategy = new TinyLogs()
                });
                using var tr = db.StartReadOnlyTransaction();
                Assert.Equal(12ul, tr.GetCommitUlong());
            }
            var downloaded = f.Remote.RangeBytes - before;
            if (pass == 0) Assert.True(downloaded > inventory.Tail.State.Length);
            // Only the active tail is read again: its native header (at most 128 B) and then its bytes.
            else Assert.InRange(downloaded, inventory.Tail.State.Length, inventory.Tail.State.Length + 128);
        }
    }

    [Fact]
    public async Task MultiFileGenesisPublishesItsNativePrefixesInOrder()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1, 10);
        f.Remote.BeforeEffect = write =>
        {
            if (write.FileId > 1) Assert.True(f.Remote.Blobs.ContainsKey(Key(write.FileId - 2)));
        };
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Equal(1u, f.Remote.Requests[0].Write.FileId);
        Assert.Equal((1ul, 10L), await Restore(f.Remote));
    }

    [Fact]
    public async Task EveryInterruptedCrossFileWritePreservesSelectedCompleteHistory()
    {
        // Five successor/predecessor boundaries in this native transaction; interruption occurs before each effect.
        for (var stop = 1; stop <= 5; stop++)
        {
            using var f = await Fixture.CreateAsync();
            await Write(f, 1);
            await f.Publisher.PublishNextAsync();
            await Write(f, 2, 8);
            var faultAt = f.Remote.Requests.Count + stop;
            f.Remote.Inject = request => request == faultAt ? Fault.DelayEffect : Fault.None;
            Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
            Assert.Equal((1ul, 1L), await Restore(f.Remote));
            var dispatched = f.Remote.Requests.Count;
            await Write(f, 3); // Local execution continues while remote publication is unresolved.
            Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
            Assert.Equal(dispatched, f.Remote.Requests.Count);
            f.Remote.CompleteDelayed();
            Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
            Assert.Equal((2ul, 9L), await Restore(f.Remote));
            Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
            Assert.Equal((3ul, 10L), await Restore(f.Remote));
        }
    }

    [Fact]
    public async Task ExactRetryCannotDuplicateBytesWhenOriginalRequestLandsLate()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        f.Remote.Inject = request => request == 1 ? Fault.DelayEffect : Fault.None;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync(retryPending: true));
        Assert.Equal(f.Remote.Requests[0].Write, f.Remote.Requests[1].Write);
        f.Remote.CompleteDelayed();
        Assert.Equal(1, f.Remote.Applied);
        Assert.Equal((1ul, 1L), await Restore(f.Remote));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LostOrCancelledReplyReconcilesTheLandedWrite(bool cancel)
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        f.Remote.Inject = _ => cancel ? Fault.CancelAfterEffect : Fault.LostResponse;
        if (cancel)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => f.Publisher.PublishNextAsync().AsTask());
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Single(f.Remote.Requests);
        Assert.True(f.Remote.MaximumRangeRead <= 64 * 1024);
        Assert.Equal((1ul, 1L), await Restore(f.Remote));
    }

    [Fact]
    public async Task AmbiguousAppendReconcilesOnlyTheAppendedBytes()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        var before = f.Publisher.PublishedPosition.Offset;
        await Write(f, 2);
        f.Remote.Inject = _ => Fault.LostResponse;
        f.Remote.RangeBytes = 0;
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Equal(f.Publisher.PublishedPosition.Offset - before, f.Remote.RangeBytes);
        Assert.Equal((2ul, 2L), await Restore(f.Remote, false));
    }

    [Fact]
    public async Task StoppingPublicationLeavesLocalWritesRollbackAndCompactionRunning()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        var tail = f.Publisher.Tail!;
        var blob = f.Remote.Blobs[tail.Key];
        var requests = f.Remote.Requests.Count;
        using var remoteCancellation = new CancellationTokenSource();
        remoteCancellation.Cancel();
        await Write(f, 2);
        await Write(f, 3, rollback: true);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            f.Publisher.PublishNextAsync(remoteCancellation: remoteCancellation.Token).AsTask());
        Assert.True(f.Files.GetFile(tail.FileId)!.GetSize() > (ulong)blob.Bytes.Length);
        await f.Db.Compact(CancellationToken.None);
        using var read = f.Db.StartReadOnlyTransaction();
        Assert.Equal(2ul, read.GetCommitUlong());
        Assert.Equal(2L, read.GetKeyValueCount());
        Assert.Equal(requests, f.Remote.Requests.Count);
        Assert.Same(blob, f.Remote.Blobs[tail.Key]);
        Assert.Equal((1ul, 1L), await Restore(f.Remote, false));
    }

    [Fact]
    public async Task ExpiredAuthorityOnlyReconcilesAndNeverDispatchesLaterWrites()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        f.Remote.Inject = _ => Fault.DelayEffect;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        f.Clock.AdvanceBy(TimeSpan.FromSeconds(11));
        Assert.Equal(TrlPublishResult.AuthorityLost, await f.Publisher.PublishNextAsync(retryPending: true));
        Assert.Single(f.Remote.Requests);
        f.Remote.CompleteDelayed();
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        await Write(f, 2);
        Assert.Equal(TrlPublishResult.AuthorityLost, await f.Publisher.PublishNextAsync());
        Assert.Single(f.Remote.Requests);
    }

    [Fact]
    public async Task AdoptionChangesOnlyVersionAndFencesDelayedOldTermAppend()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        var selected = f.Publisher.Tail!;
        var original = f.Remote.Blobs[selected.Key].Bytes.ToArray();
        await Write(f, 2);
        f.Remote.Inject = n => n == 2 ? Fault.DelayEffect : Fault.None;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        var nextCapture = new TransactionLogCapture();
        using var next = new CanonicalTrlPublisher(f.Db, nextCapture, f.Remote, Lease(f.Clock), 2, Key, selected);
        Assert.Equal(TrlPublishResult.Adopted, await next.PublishNextAsync());
        Assert.Equal(original, f.Remote.Blobs[selected.Key].Bytes);
        Assert.NotEqual(f.Publisher.Tail!.State.Token, next.Tail!.State.Token);
        f.Remote.CompleteDelayed();
        Assert.Equal(TrlPublishResult.Conflict, await f.Publisher.PublishNextAsync());
        Assert.Equal((1ul, 1L), await Restore(f.Remote));
    }

    [Fact]
    public async Task AuthorityLossAfterPreparingASuccessorNeverSelectsIt()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        await Write(f, 2, 8);
        f.Remote.BeforeEffect = _ => f.Authority.Fence();
        Assert.Equal(TrlPublishResult.AuthorityLost, await f.Publisher.PublishNextAsync());
        Assert.Equal(2, f.Remote.Requests.Count); // Genesis plus one in-flight native prefix.
        Assert.Equal(f.Capture.Acknowledged, f.Publisher.PublishedPosition);
        Assert.Equal((1ul, 1L), await Restore(f.Remote));
        await Write(f, 3);
        using var reader = f.Db.StartReadOnlyTransaction();
        Assert.Equal(3ul, reader.GetCommitUlong());
    }

    [Fact]
    public async Task SchemaCommitWithUnchangedEventCursorStillPublishesItsNativeBytes()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        using (var tr = await f.Db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue("schema"u8, "metadata"u8);
            tr.Commit();
        }
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Equal(f.Capture.Completed, f.Publisher.PublishedPosition);
        Assert.Equal((1ul, 2L), await Restore(f.Remote));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task CreateCollisionComparesExistingNativeBytesBeforeContinuing(bool shorter, bool different)
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1, 100);
        f.Remote.BeforeEffect = write =>
        {
            f.Remote.BeforeEffect = null;
            Assert.Null(write.ExpectedToken);
            var length = shorter ? write.Length / 2 : write.Length;
            var bytes = new byte[length];
            write.Source.RandomRead(bytes, 0, false);
            if (different) bytes[^1] ^= 1;
            f.Remote.Blobs[write.Key] = new(new("competing-create", length), bytes);
        };
        var result = await f.Publisher.PublishNextAsync();
        Assert.Equal(different ? TrlPublishResult.Conflict : TrlPublishResult.Published, result);
        Assert.True(f.Remote.RangeBytes > 0);
        if (different)
        {
            Assert.Equal(0, f.Remote.Applied);
            Assert.NotEqual(f.Capture.Completed, f.Capture.Acknowledged);
            Assert.Equal("competing-create", f.Remote.Blobs[Key(1)].State.Token);
        }
        else
        {
            Assert.Equal(f.Capture.Completed, f.Capture.Acknowledged);
            Assert.Equal(shorter ? 1 : 0, f.Remote.Applied);
            Assert.Equal((1ul, 100L), await Restore(f.Remote, false));
        }
    }

    [Theory]
    [InlineData(false, 2)]
    [InlineData(false, 4)]
    [InlineData(true, 2)]
    [InlineData(true, 4)]
    public async Task RestartDuringMultiFilePublicationRegeneratesExactlyTheSameTrls(bool disk, int stopAt)
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        var baseline = f.Capture.Completed;
        await Write(f, 2, 8);
        var stopRequest = f.Remote.Requests.Count + stopAt;
        f.Remote.Inject = n => n == stopRequest ? Fault.DelayEffect : Fault.None;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        f.Authority.Fence();
        var inventory = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
        var directory = Path.Combine(Path.GetTempPath(), "btdb-partial-" + Guid.NewGuid().ToString("N"));
        try
        {
            using IReplicationFileStorage local = disk ? new OnDiskReplicationFileStorage(directory) : new InMemoryReplicationFileStorage();
            await using var files = new ReplicationFileSet(local, inventory);
            await files.InitializeAsync();
            var capture = new TransactionLogCapture();
            using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = files, TransactionLogCapture = capture, Compression = new NoCompressionStrategy(),
                CompactorScheduler = null, TransactionLogSizeStrategy = new TinyLogs()
            });
            Assert.Equal(baseline, db.ReplicationRestoredPosition);
            using (var read = db.StartReadOnlyTransaction()) Assert.Equal(1ul, read.GetCommitUlong());
            var selected = new SelectedLeadership(Lease(f.Clock), 2, "new", ["main"]);
            ActivationDatabase[] databases = [new("main", db, capture, f.Remote, new(Key(1), 1), baseline, Key)];
            Assert.Null(await LeadershipActivation.ActivateAsync(selected, databases));
            using (var tr = await db.StartWritingTransaction(2))
            {
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 8; i++) cursor.CreateOrUpdateKeyValue([2, (byte)i], Enumerable.Repeat((byte)2, 700).ToArray());
                tr.Commit();
            }
            Assert.Equal(f.Capture.Completed, capture.Completed);
            foreach (var original in f.Files.Enumerate())
            {
                var regenerated = local.GetFile(original.Index);
                Assert.Equal(original.GetSize(), regenerated.GetSize());
                var expected = new byte[original.GetSize()];
                var actual = new byte[expected.Length];
                original.RandomRead(expected, 0, false);
                regenerated.RandomRead(actual, 0, false);
                Assert.Equal(expected, actual);
            }
            f.Remote.Inject = null;
            var publishers = await LeadershipActivation.ActivateAsync(selected, databases);
            using var publisher = Assert.Single(publishers!);
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
            f.Remote.CompleteDelayed(); // An old create/CAS cannot replace the adopted or newly published bytes.
            Assert.Equal((2ul, 9L), await Restore(f.Remote));
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task CleanRejectionStopsTheLaneWithoutReleasingOrRetryingCapture()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        f.Remote.Inject = _ => Fault.Reject;
        Assert.Equal(TrlPublishResult.Conflict, await f.Publisher.PublishNextAsync());
        Assert.Equal(default, f.Publisher.PublishedPosition);
        Assert.NotEqual(f.Capture.Acknowledged, f.Capture.Completed);
        Assert.Equal(TrlPublishResult.Conflict, await f.Publisher.PublishNextAsync(retryPending: true));
        Assert.Single(f.Remote.Requests);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public async Task ReconciliationRejectsAnyDifferentByteWithoutDecoding(int offset)
    {
        using var f = await Fixture.CreateAsync(tinyLogs: false);
        await Write(f, 1, 100);
        f.Remote.Inject = _ => Fault.DelayEffect;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        f.Remote.CompleteDelayed();
        Assert.Single(f.Remote.Blobs).Value.Bytes[offset] ^= 1;
        Assert.Equal(TrlPublishResult.Conflict, await f.Publisher.PublishNextAsync());
        Assert.Equal(default, f.Publisher.PublishedPosition);
        Assert.Single(f.Remote.Requests);
    }

    [Fact]
    public async Task ReconciliationReadsLargeNativePrefixesInBoundedChunks()
    {
        using var f = await Fixture.CreateAsync(tinyLogs: false);
        await Write(f, 1, 100);
        f.Remote.Inject = _ => Fault.LostResponse;
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Single(f.Remote.Requests);
        Assert.Equal(64 * 1024, f.Remote.MaximumRangeRead);
        Assert.Equal((1ul, 100L), await Restore(f.Remote, tinyLogs: false));
    }

    [Fact]
    public async Task CoalescesSeveralTransactionsIncludingRollbackIntoOnePrefix()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1);
        await Write(f, 2, rollback: true);
        await Write(f, 3);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Single(f.Remote.Requests);
        Assert.Equal((3ul, 2L), await Restore(f.Remote, false));
        Assert.Equal(f.Capture.Completed, f.Capture.Acknowledged);
        Assert.Equal(TrlPublishResult.Idle, await f.Publisher.PublishNextAsync());
    }

    [Fact]
    public async Task OldAppendWinningFirstForcesTheAdopterToRediscoverInsteadOfDroppingHistory()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        var oldTail = f.Publisher.Tail!;
        await Write(f, 2);
        await f.Publisher.PublishNextAsync();
        var unusedCapture = new TransactionLogCapture();
        using var next = new CanonicalTrlPublisher(f.Db, unusedCapture, f.Remote, Lease(f.Clock), 2, Key, oldTail);
        Assert.Equal(TrlPublishResult.Conflict, await next.PublishNextAsync());
        Assert.Equal((2ul, 2L), await Restore(f.Remote));
    }

    [Fact]
    public async Task ChangedContentNeverCountsAsAnExactResultOrAllowsLaterMutation()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        f.Remote.Inject = _ => Fault.DelayEffect;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        f.Remote.CompleteDelayed();
        var blob = f.Remote.Blobs[Key(1)];
        blob.Bytes[^1] ^= 1;
        Assert.Equal(TrlPublishResult.Conflict, await f.Publisher.PublishNextAsync());
        Assert.Equal(default, f.Publisher.PublishedPosition);
        Assert.Equal(TrlPublishResult.Conflict, await f.Publisher.PublishNextAsync(retryPending: true));
        Assert.Single(f.Remote.Requests);
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DiscoveredRestoreResumesPublicationInANewTerm(bool rollback)
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1, 8);
        await Write(f, 2, 8, rollback);
        await f.Publisher.PublishNextAsync();
        var selected = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
        using var local = new InMemoryReplicationFileStorage();
        await using var files = new ReplicationFileSet(local, selected);
        await files.InitializeAsync();
        var capture = new TransactionLogCapture();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = capture,
            CompactorScheduler = null, Compression = new NoCompressionStrategy(), TransactionLogSizeStrategy = new TinyLogs()
        });
        Assert.Null(local.GetFile(999));
        using var publisher = new CanonicalTrlPublisher(db, capture, f.Remote, Lease(f.Clock), 2, Key, selected.Tail);
        Assert.Equal(TrlPublishResult.Adopted, await publisher.PublishNextAsync());
        using (var tr = await db.StartWritingTransaction(3))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([3, 0], Enumerable.Repeat((byte)3, 700).ToArray());
            tr.Commit();
        }
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        Assert.Equal((3ul, rollback ? 9L : 17L), await Restore(f.Remote));
    }

    [Fact]
    public async Task MissingNativePredecessorCannotOpenAsAnEmptyDatabase()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1, 8);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.True(f.Remote.Blobs.Remove(Key(3)));
        await Assert.ThrowsAsync<InvalidDataException>(() => Restore(f.Remote));
    }

    [Fact]
    public async Task EveryTermUsesTheSameNativeFileNames()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1, 8);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.All(f.Remote.Requests, r => Assert.Equal(TrlFileName.Key(r.Write.FileId), r.Write.Key));
        var inventory = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
        using var next = new CanonicalTrlPublisher(f.Db, f.Capture, f.Remote, f.Authority, 2, Key, inventory.Tail);
        Assert.Equal(TrlPublishResult.Adopted, await next.AdoptAsync());
        await Write(f, 2, 8);
        Assert.Equal(TrlPublishResult.Published, await next.PublishNextAsync());
        Assert.All(f.Remote.Blobs.Keys, key => Assert.DoesNotContain("/", key));
        Assert.Equal((2ul, 16L), await Restore(f.Remote));
    }

    [Fact]
    public async Task PublisherRejectsAnObjectNameWithAnotherNativeIdBeforeWriting()
    {
        using var f = await Fixture.CreateAsync();
        await Write(f, 1);
        using var publisher = new CanonicalTrlPublisher(f.Db, f.Capture, f.Remote, f.Authority, 1, _ => "99.trl");
        await Assert.ThrowsAsync<InvalidDataException>(() => publisher.PublishNextAsync().AsTask());
        Assert.Empty(f.Remote.Requests);
    }

    [Fact]
    public async Task MissingPublishedGenesisDoesNotBecomeAnEmptyDatabase()
    {
        await Assert.ThrowsAsync<FileNotFoundException>(() =>
            CanonicalTrlInventory.DiscoverAsync(new Storage(), new(Key(1), 1)).AsTask());
    }

    [Fact]
    public async Task AppendedTailStillRestoresTheSelectedPrefixAndRediscoveryUsesTheNewHistory()
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        var selected = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
        await Write(f, 2);
        await f.Publisher.PublishNextAsync();
        using var local = new InMemoryReplicationFileStorage();
        await using (var files = new ReplicationFileSet(local, selected))
        {
            await files.InitializeAsync();
            // A leader publishing during a restore must not fail it: the append-only tail keeps the selected bytes.
            using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = files, CompactorScheduler = null
            });
            using var transaction = db.StartReadOnlyTransaction();
            Assert.Equal(1ul, transaction.GetCommitUlong());
        }
        Assert.Equal((2ul, 2L), await Restore(f.Remote, false));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InterruptedRestoreRetriesWithoutUsingPartialCache(bool cancel)
    {
        using var f = await Fixture.CreateAsync(false);
        await Write(f, 1, 7000); // Several TRL files; each is one download block.
        await f.Publisher.PublishNextAsync();
        var published = f.Remote.Requests.Count;
        var selected = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
        using var local = new InMemoryReplicationFileStorage();
        using var cancellation = new CancellationTokenSource();
        var reads = 0;
        f.Remote.BeforeRangeRead = _ =>
        {
            if (++reads == 1) return;
            if (cancel) cancellation.Cancel();
            else throw new IOException("Transfer interrupted in a later file.");
        };
        await using (var files = new ReplicationFileSet(local, selected))
        {
            var options = new KeyValueDBOptions
            {
                FileCollection = files,
                Compression = new NoCompressionStrategy(), CompactorScheduler = null
            };
            await files.InitializeAsync();
            if (cancel) await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                BTreeKeyValueDB.OpenAsync(options, cancellation.Token).AsTask());
            else await Assert.ThrowsAsync<IOException>(() => BTreeKeyValueDB.OpenAsync(options).AsTask());
        }
        // A canceled waiter does not cancel a shared transfer. It may have completed synchronously;
        // disposal drains it, and a later initialization still revalidates/redownloads the active tail.
        foreach (var completed in local.Enumerate())
            Assert.Equal((ulong)selected.GetHead(completed.Index).State.Length, completed.GetSize());
        f.Remote.BeforeRangeRead = null;
        selected = await CanonicalTrlInventory.DiscoverAsync(f.Remote, new(Key(1), 1));
        await using var retryFiles = new ReplicationFileSet(local, selected);
        await retryFiles.InitializeAsync();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = retryFiles,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null
        });
        using var read = db.StartReadOnlyTransaction();
        Assert.Equal(1ul, read.GetCommitUlong());
        Assert.Equal(256L, read.GetKeyValueCount());
        Assert.Equal(published, f.Remote.Requests.Count); // Restore never publishes anything.
        Assert.InRange(f.Remote.MaximumRangeRead, 1, ReplicationFileSet.DownloadBlockSize);
    }

}
