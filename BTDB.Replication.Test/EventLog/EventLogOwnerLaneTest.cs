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
