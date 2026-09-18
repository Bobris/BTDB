using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class CanonicalTrlPublisherTest
{
    enum Fault { None, LostResponse, DelayEffect, CancelAfterEffect, Reject }
    sealed record Blob(TrlObjectState State, byte[] Bytes);
    sealed record Request(TrlWrite Write, byte[] Suffix);

    sealed class Storage : ICanonicalTrlStorage
    {
        public readonly Dictionary<string, Blob> Blobs = new();
        public readonly List<Request> Requests = new();
        public readonly Queue<Action> Delayed = new();
        public Func<int, Fault>? Inject;
        public Action<TrlWrite>? BeforeEffect;
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
            var blob = Blobs[key];
            Assert.Equal(token, blob.State.Token);
            MaximumRangeRead = Math.Max(MaximumRangeRead, destination.Length);
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
            var state = new TrlObjectState((++_version).ToString(), write.Length, write.Metadata);
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
        public readonly InMemoryFileCollection Files = new();
        public readonly TransactionLogCapture Capture;
        public readonly BTreeKeyValueDB Db;
        public readonly Storage Remote = new();
        public readonly DeterministicScheduler Clock = new(781);
        public readonly LeaseAuthority Authority;
        public readonly CanonicalTrlPublisher Publisher;
        public Fixture(bool tinyLogs = true)
        {
            Capture = new();
            Db = Open(Files, Capture, tinyLogs);
            Authority = Lease(Clock);
            Publisher = new(Db, Capture, Remote, Authority, 1, Key);
        }
        public void Dispose()
        {
            Publisher.Dispose();
            Db.Dispose();
            Files.Dispose();
        }
    }

    static string Key(uint id) => $"trl/{id}";
    static LeaseAuthority Lease(DeterministicScheduler clock)
    {
        var authority = new LeaseAuthority(clock.CreateScope(Guid.NewGuid().ToString()), 0, TimeSpan.FromTicks(1));
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromSeconds(10)));
        return authority;
    }
    static BTreeKeyValueDB Open(IFileCollection files, TransactionLogCapture? capture = null, bool tinyLogs = true) => new(new KeyValueDBOptions
    {
        FileCollection = files, TransactionLogCapture = capture, Compression = new NoCompressionStrategy(),
        CompactorScheduler = null, UseOddTransactionLogIds = true, FileSplitSize = 1024 * 1024,
        TransactionLogSizeStrategy = tinyLogs ? new TinyLogs() : null
    });
    static async Task Write(Fixture f, ulong id, int count = 1, bool rollback = false, bool batch = false)
    {
        using var tr = await f.Db.StartWritingTransaction(id, batch);
        using var cursor = tr.CreateCursor();
        for (var i = 0; i < count; i++) cursor.CreateOrUpdateKeyValue([(byte)id, (byte)i], Enumerable.Repeat((byte)id, 700).ToArray());
        if (!rollback) tr.Commit();
    }

    static (ulong EventId, long Keys) Restore(Storage storage, bool tinyLogs = true)
    {
        using var files = new InMemoryFileCollection();
        var key = Key(1);
        var id = 1u;
        var closure = new Dictionary<uint, Blob>();
        while (storage.Blobs.TryGetValue(key, out var blob))
        {
            Assert.True(closure.TryAdd(id, blob));
            if (blob.State.Metadata.Next is not { } next) break;
            (id, key) = (next.FileId, next.Key);
            Assert.True(storage.Blobs.ContainsKey(key));
        }
        foreach (var (fileId, blob) in closure.OrderBy(p => p.Key))
        {
            var file = files.AddFile("trl", FileIdParity.Any, fileId - 1);
            Assert.Equal(fileId, file.Index);
            var writer = new MemWriter(file.GetAppenderWriter());
            writer.WriteBlock(blob.Bytes);
            writer.Flush();
        }
        using var db = Open(files, tinyLogs: tinyLogs);
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

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PublishesRealNativeCommitsAndRollbackAcrossFiles(bool batch, bool rollback)
    {
        using var f = new Fixture();
        await Write(f, 1, batch: batch);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        var prefix = f.Remote.Blobs[Key(1)].Bytes.ToArray();
        await Write(f, 2, 10, rollback, batch);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.True(f.Remote.Requests.Count > 3);
        Assert.Equal(prefix, f.Remote.Blobs[Key(1)].Bytes[..prefix.Length]);
        Assert.Equal((rollback ? 1ul : 2ul, rollback ? 1L : 11L), Restore(f.Remote));
        Assert.Equal(f.Capture.Completed, f.Publisher.PublishedPosition);
        Assert.Equal(TrlPublishResult.Idle, await f.Publisher.PublishNextAsync());
        // A later transaction may start in another file without an in-transaction rotation record.
        await Write(f, 3);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Equal((3ul, rollback ? 2L : 12L), Restore(f.Remote));
    }

    [Fact]
    public async Task MultiFileGenesisSelectsItsRootOnlyAfterSuccessors()
    {
        using var f = new Fixture();
        await Write(f, 1, 10);
        f.Remote.BeforeEffect = write =>
        {
            if (write.Metadata.Next is { } next) Assert.True(f.Remote.Blobs.ContainsKey(next.Key));
            if (write.FileId != 1) Assert.False(f.Remote.Blobs.ContainsKey(Key(1)));
        };
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Equal(1u, f.Remote.Requests[^1].Write.FileId);
        Assert.Equal((1ul, 10L), Restore(f.Remote));
    }

    [Fact]
    public async Task EveryInterruptedCrossFileWritePreservesSelectedCompleteHistory()
    {
        // Five successor/predecessor boundaries in this native transaction; interruption occurs before each effect.
        for (var stop = 1; stop <= 5; stop++)
        {
            using var f = new Fixture();
            await Write(f, 1);
            await f.Publisher.PublishNextAsync();
            await Write(f, 2, 8);
            var faultAt = f.Remote.Requests.Count + stop;
            f.Remote.Inject = request => request == faultAt ? Fault.DelayEffect : Fault.None;
            Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
            Assert.Equal((1ul, 1L), Restore(f.Remote));
            var dispatched = f.Remote.Requests.Count;
            await Write(f, 3); // Local execution continues while remote publication is unresolved.
            Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
            Assert.Equal(dispatched, f.Remote.Requests.Count);
            f.Remote.CompleteDelayed();
            Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
            Assert.Equal((2ul, 9L), Restore(f.Remote));
            Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
            Assert.Equal((3ul, 10L), Restore(f.Remote));
        }
    }

    [Fact]
    public async Task ExactRetryCannotDuplicateBytesWhenOriginalRequestLandsLate()
    {
        using var f = new Fixture();
        await Write(f, 1);
        f.Remote.Inject = request => request == 1 ? Fault.DelayEffect : Fault.None;
        Assert.Equal(TrlPublishResult.Pending, await f.Publisher.PublishNextAsync());
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync(retryPending: true));
        Assert.Equal(f.Remote.Requests[0].Write, f.Remote.Requests[1].Write);
        f.Remote.CompleteDelayed();
        Assert.Equal(1, f.Remote.Applied);
        Assert.Equal((1ul, 1L), Restore(f.Remote));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LostOrCancelledReplyReconcilesTheLandedWrite(bool cancel)
    {
        using var f = new Fixture();
        await Write(f, 1);
        f.Remote.Inject = _ => cancel ? Fault.CancelAfterEffect : Fault.LostResponse;
        if (cancel)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => f.Publisher.PublishNextAsync().AsTask());
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Single(f.Remote.Requests);
        Assert.True(f.Remote.MaximumRangeRead <= 64 * 1024);
        Assert.Equal((1ul, 1L), Restore(f.Remote));
    }

    [Fact]
    public async Task StoppingPublicationLeavesLocalWritesRollbackAndCompactionRunning()
    {
        using var f = new Fixture(false);
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
        Assert.Equal((1ul, 1L), Restore(f.Remote, false));
    }

    [Fact]
    public async Task ExpiredAuthorityOnlyReconcilesAndNeverDispatchesLaterWrites()
    {
        using var f = new Fixture();
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
    public async Task AdoptionChangesOnlyMetadataAndFencesDelayedOldTermAppend()
    {
        using var f = new Fixture();
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
        Assert.Equal(2ul, next.Tail!.State.Metadata.Term);
        f.Remote.CompleteDelayed();
        Assert.Equal(TrlPublishResult.Conflict, await f.Publisher.PublishNextAsync());
        Assert.Equal((1ul, 1L), Restore(f.Remote));
    }

    [Fact]
    public async Task AuthorityLossAfterPreparingASuccessorNeverSelectsIt()
    {
        using var f = new Fixture();
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        await Write(f, 2, 8);
        f.Remote.BeforeEffect = _ => f.Authority.Fence();
        Assert.Equal(TrlPublishResult.AuthorityLost, await f.Publisher.PublishNextAsync());
        Assert.Equal(2, f.Remote.Requests.Count); // Genesis plus one prepared successor, no predecessor CAS.
        Assert.Equal(f.Capture.Acknowledged, f.Publisher.PublishedPosition);
        Assert.Equal((1ul, 1L), Restore(f.Remote));
        await Write(f, 3);
        using var reader = f.Db.StartReadOnlyTransaction();
        Assert.Equal(3ul, reader.GetCommitUlong());
    }

    [Fact]
    public async Task SchemaCommitWithUnchangedEventCursorStillPublishesItsNativeBytes()
    {
        using var f = new Fixture();
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
        Assert.Equal((1ul, 2L), Restore(f.Remote));
    }

    [Fact]
    public async Task CleanRejectionStopsTheLaneWithoutReleasingOrRetryingCapture()
    {
        using var f = new Fixture();
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
        using var f = new Fixture(tinyLogs: false);
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
        using var f = new Fixture(tinyLogs: false);
        await Write(f, 1, 100);
        f.Remote.Inject = _ => Fault.LostResponse;
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Single(f.Remote.Requests);
        Assert.Equal(64 * 1024, f.Remote.MaximumRangeRead);
        Assert.Equal((1ul, 100L), Restore(f.Remote, tinyLogs: false));
    }

    [Fact]
    public async Task CoalescesSeveralTransactionsIncludingRollbackIntoOnePrefix()
    {
        using var f = new Fixture(false);
        await Write(f, 1);
        await Write(f, 2, rollback: true);
        await Write(f, 3);
        Assert.Equal(TrlPublishResult.Published, await f.Publisher.PublishNextAsync());
        Assert.Single(f.Remote.Requests);
        Assert.Equal((3ul, 2L), Restore(f.Remote, false));
        Assert.Equal(f.Capture.Completed, f.Capture.Acknowledged);
        Assert.Equal(TrlPublishResult.Idle, await f.Publisher.PublishNextAsync());
    }

    [Fact]
    public async Task OldAppendWinningFirstForcesTheAdopterToRediscoverInsteadOfDroppingHistory()
    {
        using var f = new Fixture();
        await Write(f, 1);
        await f.Publisher.PublishNextAsync();
        var oldTail = f.Publisher.Tail!;
        await Write(f, 2);
        await f.Publisher.PublishNextAsync();
        var unusedCapture = new TransactionLogCapture();
        using var next = new CanonicalTrlPublisher(f.Db, unusedCapture, f.Remote, Lease(f.Clock), 2, Key, oldTail);
        Assert.Equal(TrlPublishResult.Conflict, await next.PublishNextAsync());
        Assert.Equal((2ul, 2L), Restore(f.Remote));
    }

    [Fact]
    public async Task ChangedContentNeverCountsAsAnExactResultOrAllowsLaterMutation()
    {
        using var f = new Fixture();
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
}
