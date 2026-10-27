using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

public class EventLogOwnerLaneTest
{
    const string Topic = "t";
    static readonly EventLogOwner A = new("a", "http://a");
    static readonly EventLogOwner B = new("b", "http://b");

    static EventLogOptions Small(int cap = 512, int free = 64) => new() { SplitCap = cap, SealFreeSpace = free };

    static async Task<EventLogOwnerLane> Lane(IEventLogStorage storage, EventLogOwner self, EventLogOptions? options = null,
        IReplicationScheduler? scheduler = null) =>
        await EventLogOwnerLane.CreateCandidateAsync(storage, new(storage, Topic), Topic, options ?? Small(),
            scheduler ?? new TestScheduler(), self, CancellationToken.None);

    static ValueTask<ulong> Submit(EventLogOwnerLane lane, ulong session, ulong sequence, byte[] record,
        ulong oldest = 1, bool dispatched = false, ulong resumeFrom = 0) =>
        lane.SubmitAsync(session, sequence, record, oldest, dispatched, resumeFrom, CancellationToken.None);

    static async Task AssertHistory(IEventLogStorage storage, int count, int length = 4)
    {
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, count).Select(i => (ulong)i), records.Select(r => r.Offset));
        Assert.Equal(Enumerable.Range(0, count).Select(i => Bytes(i, length)), records.Select(r => r.Payload.ToArray()));
    }

    [Fact]
    public async Task FirstPublicationCreatesTheTopicAndOffsetsAreContiguous()
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A);
        Assert.True(lane.IsInstalled);
        var offsets = await Task.WhenAll(Enumerable.Range(0, 5).Select(i => Submit(lane, 7, (ulong)i + 1, Bytes(i)).AsTask()));
        Assert.Equal([0ul, 1, 2, 3, 4], offsets);
        await AssertHistory(storage, 5);
        Assert.Equal(A, (await storage.ReadAsync(EventLogFormat.SplitKey(Topic, 1), default))!.Owner);
    }

    [Fact]
    public async Task RotationUsesProactiveSealsWithoutSealOnlyWrites()
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A, Small(512, 128));
        for (var i = 0; i < 200; i++) Assert.Equal((ulong)i, await Submit(lane, 7, (ulong)i + 1, Bytes(i, 20)));
        await AssertHistory(storage, 200, 20);
        var splits = storage.Inner.Keys.ToArray();
        Assert.True(splits.Length > 10);
        foreach (var key in splits[..^1])
        {
            var blob = await storage.ReadAsync(key, default);
            var parsed = EventLogFormat.Parse(blob!.Content.Span, Topic);
            Assert.NotNull(parsed.Seal);
            Assert.True(blob.Content.Length <= 512);
        }
        // Every write carried records: one mutation per commit plus the topic creation.
        Assert.Equal(200 + 1, storage.Mutations("write") + storage.Mutations("create"));
    }

    [Fact]
    public async Task BurstLargerThanTheTailRoomFillsAndSealsTheTailInsteadOfASealOnlyWrite()
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A, Small(512, 64));
        Assert.Equal(0ul, await Submit(lane, 7, 1, Bytes(0, 20)));
        var frames = new Dictionary<string, int>();
        var sealOnly = 0;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        storage.BeforeMutation = _ => new(gate.Task);
        storage.AfterMutation = async operation =>
        {
            var blob = await storage.Inner.ReadAsync(operation.Key, default);
            var count = EventLogFormat.Parse(blob!.Content.Span, Topic).Frames.Count;
            lock (frames)
            {
                if (frames.TryGetValue(operation.Key, out var before) && before == count) sealOnly++;
                frames[operation.Key] = count;
            }
        };
        // The first record of the burst starts a blocked write; the rest queue up beyond the tail's remaining room.
        var tasks = Enumerable.Range(1, 40).Select(i => Submit(lane, 7, (ulong)i + 1, Bytes(i, 20)).AsTask()).ToArray();
        gate.SetResult();
        Assert.Equal(Enumerable.Range(1, 40).Select(i => (ulong)i), await Task.WhenAll(tasks));
        await AssertHistory(storage, 41, 20);
        Assert.Equal(0, sealOnly);
        foreach (var key in storage.Inner.Keys.ToArray()[..^1])
        {
            var blob = await storage.ReadAsync(key, default);
            Assert.NotNull(EventLogFormat.Parse(blob!.Content.Span, Topic).Seal);
            Assert.True(blob.Content.Length <= 512);
        }
    }

    [Fact]
    public async Task CommitLingerGathersRecordsReachingAnIdleOwnerIntoOneWrite()
    {
        var storage = new FaultyEventLogStorage();
        var scheduler = new TestScheduler(manual: true);
        var lane = await Lane(storage, A, Small(4096, 512) with { CommitLinger = TimeSpan.FromMilliseconds(5) }, scheduler);
        var before = storage.Mutations("write");
        var first = Submit(lane, 7, 1, Bytes(0)).AsTask();
        var second = Submit(lane, 7, 2, Bytes(1)).AsTask();
        await Task.Delay(50);
        Assert.False(first.IsCompleted);
        Assert.Equal(before, storage.Mutations("write"));
        scheduler.Advance(TimeSpan.FromMilliseconds(5));
        var offsets = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { 0ul, 1ul }, offsets);
        Assert.Equal(before + 1, storage.Mutations("write"));
    }

    [Fact]
    public async Task CommitLingerAlsoPrecedesWritesOfABusyOwner()
    {
        var storage = new FaultyEventLogStorage();
        var scheduler = new TestScheduler(manual: true);
        var lane = await Lane(storage, A, Small(4096, 512) with { CommitLinger = TimeSpan.FromMilliseconds(5) }, scheduler);
        var before = storage.Mutations("write");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        storage.BeforeMutation = _ =>
        {
            writing.TrySetResult();
            return new(gate.Task);
        };
        var first = Submit(lane, 7, 1, Bytes(0)).AsTask();
        await Task.Delay(20);
        scheduler.Advance(TimeSpan.FromMilliseconds(5));
        await writing.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var second = Submit(lane, 7, 2, Bytes(1)).AsTask(); // queued while the first write is in flight
        gate.SetResult();
        Assert.Equal(0ul, await first.WaitAsync(TimeSpan.FromSeconds(10)));
        await Task.Delay(50);
        Assert.False(second.IsCompleted); // the next write waits for the window
        var third = Submit(lane, 7, 3, Bytes(2)).AsTask();
        scheduler.Advance(TimeSpan.FromMilliseconds(5));
        var offsets = await Task.WhenAll(second, third).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { 1ul, 2ul }, offsets);
        Assert.Equal(before + 2, storage.Mutations("write"));
    }

    [Fact]
    public async Task AdaptiveLingerOpensForPublishersAnsweringCommits()
    {
        var storage = new FaultyEventLogStorage();
        var scheduler = new TestScheduler(manual: true);
        var options = Small(1 << 20, 1 << 16) with { AdaptiveCommitLingerMax = TimeSpan.FromMilliseconds(3) };
        var lane = await Lane(storage, A, options, scheduler);
        // Every write takes 9 ms of virtual time.
        storage.BeforeMutation = _ =>
        {
            scheduler.Advance(TimeSpan.FromMilliseconds(9));
            return default;
        };
        Task<ulong>? remote = null;
        for (var i = 0; i < 100; i++)
        {
            // A local answer starts the write at once; a remote one arrives 0.3 ms later, during that write.
            var sequence = (ulong)i + 1;
            scheduler.Schedule(TimeSpan.FromMilliseconds(0.3), () => remote = Submit(lane, 8, sequence, Bytes(i)).AsTask(),
                "remote answer");
            await Submit(lane, 7, sequence, Bytes(i)).AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            await remote!.WaitAsync(TimeSpan.FromSeconds(10));
        }
        Assert.InRange(lane.CommitLinger, TimeSpan.FromMilliseconds(0.3), TimeSpan.FromMilliseconds(0.6));
    }

    [Fact]
    public async Task AdaptiveLingerStaysClosedForAPeriodicSourceReachingAnIdleOwner()
    {
        var storage = new FaultyEventLogStorage();
        var scheduler = new TestScheduler(manual: true);
        var options = Small(1 << 20, 1 << 16) with { AdaptiveCommitLingerMax = TimeSpan.FromMilliseconds(3) };
        var lane = await Lane(storage, A, options, scheduler);
        storage.BeforeMutation = _ =>
        {
            scheduler.Advance(TimeSpan.FromMilliseconds(9.5));
            return default;
        };
        for (var i = 0; i < 200; i++)
        {
            scheduler.Advance(TimeSpan.FromMilliseconds(0.5));
            await Submit(lane, 7, (ulong)i + 1, Bytes(i)).AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
        Assert.Equal(TimeSpan.Zero, lane.CommitLinger);
    }

    [Fact]
    public async Task LingerEndsOnceTheQueueFillsTheNextBatch()
    {
        var storage = new FaultyEventLogStorage();
        var scheduler = new TestScheduler(manual: true);
        var lane = await Lane(storage, A, Small(512, 64) with { CommitLinger = TimeSpan.FromMilliseconds(50) }, scheduler);
        // Twenty 40-byte records fill three 512-byte splits (about six each). Without the window's timer advancing, the
        // full batches are written at once; only the remainder waits for the window.
        var tasks = Enumerable.Range(0, 20).Select(i => Submit(lane, 7, (ulong)i + 1, Bytes(i, 40)).AsTask()).ToArray();
        await Task.WhenAll(tasks[..^4]).WaitAsync(TimeSpan.FromSeconds(10));
        scheduler.Advance(TimeSpan.FromSeconds(1));
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));
        await AssertHistory(storage, 20, 40);
    }

    [Fact]
    public async Task SubMillisecondLingerDelaysTheWriteUntilTheWindowEnds()
    {
        var storage = new FaultyEventLogStorage();
        var linger = TimeSpan.FromMilliseconds(1.5);
        var lane = await Lane(storage, A, Small(4096, 512) with { CommitLinger = linger }, new SystemReplicationScheduler());
        var started = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        var submitted = System.Diagnostics.Stopwatch.StartNew();
        storage.BeforeMutation = _ =>
        {
            started.TrySetResult(submitted.Elapsed);
            return default;
        };
        Assert.Equal(0ul, await Submit(lane, 7, 1, Bytes(0)).AsTask().WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(await started.Task >= linger);
    }

    [Fact]
    public async Task LargeRecordGetsItsOwnSealedSplit()
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A, Small(512, 16));
        Assert.Equal(0ul, await Submit(lane, 7, 1, Bytes(0)));
        var large = new byte[2000];
        large[0] = 42;
        Assert.Equal(1ul, await Submit(lane, 7, 2, large));
        Assert.Equal(2ul, await Submit(lane, 7, 3, Bytes(2)));
        var reader = new EventLogStorageReader(storage, Topic);
        var records = await reader.ReadAllAsync();
        Assert.Equal(large, records[1].Payload.ToArray());
        var second = EventLogFormat.Parse((await storage.ReadAsync(EventLogFormat.SplitKey(Topic, 2), default))!.Content.Span, Topic);
        Assert.Equal((1ul, 2ul), (second.FirstOffset, second.NextOffset));
        Assert.NotNull(second.Seal);
        Assert.Equal(2ul, (await reader.FindTailAsync(default))!.Object.FirstOffset);
    }

    [Fact]
    public async Task LargeRecordFillsAnEmptyTailInsteadOfSealingItEmpty()
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A, Small(512, 16));
        Assert.Equal(0ul, await Submit(lane, 7, 1, new byte[2000]));
        var first = EventLogFormat.Parse((await storage.ReadAsync(EventLogFormat.SplitKey(Topic, 1), default))!.Content.Span, Topic);
        Assert.Equal(1ul, first.NextOffset);
        Assert.NotNull(first.Seal);
    }

    [Theory]
    [InlineData(StorageFault.LoseResponse)]
    [InlineData(StorageFault.TimeoutWithoutEffect)]
    public async Task AmbiguousWritesAreRepeatedAndNeverDuplicate(StorageFault fault)
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A, Small(4096));
        var injected = 0;
        storage.Fault = o => o.Kind == "write" && Interlocked.Increment(ref injected) % 2 == 1 ? fault : StorageFault.None;
        for (var i = 0; i < 20; i++) Assert.Equal((ulong)i, await Submit(lane, 7, (ulong)i + 1, Bytes(i)));
        await AssertHistory(storage, 20);
        Assert.False(lane.IsStopped);
    }

    [Fact]
    public async Task CandidateFencesOwnerAndPreservesItsHistory()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(4096));
        Assert.Equal(0ul, await Submit(a, 1, 1, Bytes(0)));
        var b = await Lane(storage, B, Small(4096));
        Assert.False(b.IsInstalled);
        Assert.Equal(1ul, await Submit(b, 2, 1, Bytes(1)));
        Assert.True(b.IsInstalled);
        await Assert.ThrowsAsync<EventLogOwnerLostException>(async () => await Submit(a, 1, 2, Bytes(9)));
        Assert.True(a.IsStopped);
        await AssertHistory(storage, 2);
    }

    [Fact]
    public async Task PredecessorAppendWinningTheRaceFencesTheCandidate()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(4096));
        Assert.Equal(0ul, await Submit(a, 1, 1, Bytes(0)));
        var b = await Lane(storage, B, Small(4096));
        Assert.Equal(1ul, await Submit(a, 1, 2, Bytes(1)));
        await Assert.ThrowsAsync<EventLogOwnerLostException>(async () => await Submit(b, 2, 1, Bytes(9)));
        Assert.Equal(2ul, await Submit(a, 1, 3, Bytes(2)));
        await AssertHistory(storage, 3);
    }

    [Fact]
    public async Task ResubmissionToANewOwnerReturnsTheOriginalOffsets()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(300, 32));
        for (var i = 0; i < 10; i++) await Submit(a, 5, (ulong)i + 1, Bytes(i));
        // The publisher lost the receipts of 6..10 and resubmits them after an owner change.
        var b = await Lane(storage, B, Small(300, 32));
        for (var i = 5; i < 10; i++)
            Assert.Equal((ulong)i, await Submit(b, 5, (ulong)i + 1, Bytes(i), oldest: 6, dispatched: true, resumeFrom: 3));
        Assert.Equal(10ul, await Submit(b, 5, 11, Bytes(10), oldest: 6, dispatched: true, resumeFrom: 3));
        await AssertHistory(storage, 11);
    }

    [Fact]
    public async Task DelayedFirstDispatchAtANewOwnerReturnsTheOffsetOfItsCommittedRetry()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(4096));
        // The retry of sequence 1 commits through A while its first dispatch is still on the way to B.
        Assert.Equal(0ul, await Submit(a, 5, 1, Bytes(0), dispatched: true));
        var b = await Lane(storage, B, Small(4096));
        Assert.Equal(0ul, await Submit(b, 5, 1, Bytes(0)));
        Assert.Equal(1ul, await Submit(b, 5, 2, Bytes(1), oldest: 2, resumeFrom: 1));
        await AssertHistory(storage, 2);
    }

    [Fact]
    public async Task NewSessionAtTheNextOffsetTakesOverWithItsFirstCommitWithoutReading()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(4096));
        await Submit(a, 5, 1, Bytes(0));
        var b = await Lane(storage, B, Small(4096));
        var reads = storage.Inner.Reads;
        Assert.Equal(1ul, await Submit(b, 6, 1, Bytes(1), resumeFrom: 1));
        Assert.Equal(reads, storage.Inner.Reads);
        Assert.Equal(0, storage.Mutations("owner"));
        Assert.True(b.IsInstalled);
        await AssertHistory(storage, 2);
    }

    [Fact]
    public async Task LandedWriteWhoseResponseWasLostIsCommittedEvenWhenATakeoverFollowsIt()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(4096));
        await Submit(a, 5, 1, Bytes(0));
        var b = await Lane(storage, B, Small(4096)); // discovered before A's next write
        storage.Fault = o => o.Kind == "write" ? StorageFault.LoseResponse : StorageFault.None;
        storage.AfterMutation = async o =>
        {
            if (o.Kind != "write") return;
            storage.AfterMutation = null;
            storage.Fault = null;
            // B's discovered version is stale; it rediscovers and takes over the tail that includes A's write.
            b = await Lane(storage, B, Small(4096));
            await b.InstallAsync();
        };
        Assert.Equal(1ul, await Submit(a, 5, 2, Bytes(1)));
        Assert.True(a.IsStopped);
        Assert.Equal(1ul, await Submit(b, 5, 2, Bytes(1), oldest: 2, dispatched: true, resumeFrom: 1));
        Assert.Equal(2ul, await Submit(b, 5, 3, Bytes(2), oldest: 2, dispatched: true, resumeFrom: 1));
        await AssertHistory(storage, 3);
    }

    [Fact]
    public async Task PipelinedSequencesCommitInPublisherOrder()
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A, Small(4096), new TestScheduler(manual: true));
        var second = Submit(lane, 9, 2, Bytes(1)).AsTask();
        var third = Submit(lane, 9, 3, Bytes(2)).AsTask();
        Assert.False(second.IsCompleted);
        Assert.Equal(0ul, await Submit(lane, 9, 1, Bytes(0)));
        Assert.Equal([1ul, 2], await Task.WhenAll(second, third));
        Assert.Equal(1ul, await Submit(lane, 9, 2, Bytes(1)));
        await AssertHistory(storage, 3);
    }

    [Fact]
    public async Task GapThatIsNeverFilledFailsAfterTheOwnerTimeout()
    {
        var storage = new FaultyEventLogStorage();
        var scheduler = new TestScheduler(manual: true);
        var lane = await Lane(storage, A, Small(4096), scheduler);
        var orphan = Submit(lane, 9, 3, Bytes(2)).AsTask();
        scheduler.Advance(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<EventLogSequenceGapException>(() => orphan);
    }

    [Fact]
    public async Task IdleOwnerValidationDetectsATakeover()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(4096));
        await Submit(a, 1, 1, Bytes(0));
        Assert.Equal(1ul, await a.ValidateAsync(default));
        var b = await Lane(storage, B, Small(4096));
        await b.InstallAsync();
        await Assert.ThrowsAsync<EventLogOwnerLostException>(async () => await a.ValidateAsync(default));
        Assert.True(a.IsStopped);
        Assert.Equal(1ul, await b.ValidateAsync(default));
    }

    [Fact]
    public async Task TakeoverOfSealedIdleTailCreatesHeaderOnlySuccessorWithInheritedOwner()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(400, 300));
        await Submit(a, 1, 1, Bytes(0)); // proactively sealed at once
        Assert.NotNull((await new EventLogStorageReader(storage, Topic).FindTailAsync(default))!.Object.Seal);
        var b = await Lane(storage, B, Small(400, 300));
        var successor = await storage.ReadAsync(EventLogFormat.SplitKey(Topic, 2), default);
        Assert.Equal(A, successor!.Owner);
        Assert.Equal(1ul, await Submit(b, 2, 1, Bytes(1)));
        await Assert.ThrowsAsync<EventLogOwnerLostException>(async () => await Submit(a, 1, 2, Bytes(9)));
        await AssertHistory(storage, 2);
    }

    [Fact]
    public async Task OwnerWhoseSuccessorWasCreatedByAHelperStillAppends()
    {
        var storage = new FaultyEventLogStorage();
        var a = await Lane(storage, A, Small(400, 300));
        await Submit(a, 1, 1, Bytes(0));
        await Lane(storage, B, Small(400, 300)); // a candidate that never writes, like a helper
        Assert.Equal(1ul, await a.ValidateAsync(default));
        Assert.Equal(1ul, await Submit(a, 1, 2, Bytes(1)));
        await AssertHistory(storage, 2);
    }

    [Fact]
    public async Task TooLargeRecordIsRejectedBeforeAcceptance()
    {
        var storage = new FaultyEventLogStorage();
        var lane = await Lane(storage, A, new() { MaxRecordSize = 10 });
        await Assert.ThrowsAsync<EventLogRecordTooLargeException>(async () => await Submit(lane, 1, 1, new byte[11]));
    }
}
