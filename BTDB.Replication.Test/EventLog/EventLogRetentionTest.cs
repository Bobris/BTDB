using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

public class EventLogRetentionTest
{
    const string Topic = "t";
    static readonly EventLogOwner A = new("a", "http://a");

    sealed class Fixture
    {
        public readonly ManualTimeProvider Time = new(DateTimeOffset.UnixEpoch);
        public readonly InMemoryEventLogStorage Storage;
        public readonly EventLogOptions Options;
        EventLogOwnerLane _lane = null!;
        ulong _sequence;

        public Fixture(EventLogRetention? retention, int fanOut = 2)
        {
            Storage = new(Time);
            // One 30-byte record per split with this cap and threshold.
            Options = new()
            {
                SplitCap = 256, SealFreeSpace = 200, MergeFanOut = fanOut, MergeLevels = fanOut == 0 ? 0 : 2,
                DeletionDelay = TimeSpan.FromHours(1), TimeProvider = Time, Retention = _ => retention
            };
        }

        public async Task Start() => _lane = await EventLogOwnerLane.CreateCandidateAsync(Storage,
            new(Storage, Topic), Topic, Options, new TestScheduler(), A, default);

        public async Task Publish(int count)
        {
            for (var i = 0; i < count; i++)
            {
                _sequence++;
                Assert.Equal(_sequence - 1, await _lane.SubmitAsync(1, _sequence, Bytes((int)_sequence - 1, 30), 1, false, 0, default));
            }
        }

        public EventLogMerger Merger() => new(Storage, Topic, Options, new TestScheduler());

        public ValueTask<ulong> First() => new EventLogStorageReader(Storage, Topic).FirstOffsetAsync(default);

        public long Size => Storage.Keys.Where(k => k.StartsWith(Topic + "/")).Sum(k => Storage.ReadAsync(k, default).AsTask().Result!.Content.Length);
    }

    static async Task AssertReadsFrom(InMemoryEventLogStorage storage, ulong first, int count)
    {
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync(first);
        Assert.Equal(Enumerable.Range((int)first, count - (int)first).Select(i => (ulong)i), records.Select(r => r.Offset));
        Assert.Equal(Enumerable.Range((int)first, count - (int)first).Select(i => Bytes(i, 30)), records.Select(r => r.Payload.ToArray()));
    }

    [Fact]
    public async Task WithoutRetentionTheWholeHistoryStays()
    {
        var fixture = new Fixture(null);
        await fixture.Start();
        await fixture.Publish(20);
        for (var i = 0; i < 3; i++)
        {
            await fixture.Merger().RunPassAsync(default);
            fixture.Time.Advance(TimeSpan.FromDays(400));
        }
        Assert.Equal(0ul, await fixture.First());
        await AssertReadsFrom(fixture.Storage, 0, 20);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(0)] // retention runs without merging too
    public async Task SizeRetentionRemovesTheOldestHistoryAndKeepsTheRest(int fanOut)
    {
        var fixture = new Fixture(new(MaxBytes: 2000), fanOut);
        await fixture.Start();
        await fixture.Publish(40);
        var before = fixture.Size;
        Assert.True(before > 4000);
        var result = await fixture.Merger().RunPassAsync(default);
        Assert.True(result.Marked > 0);
        var first = await fixture.First();
        Assert.True(first > 0);
        // Condemned history leaves the bounds at once but stays readable for the deletion delay.
        await AssertReadsFrom(fixture.Storage, 0, 40);
        await AssertReadsFrom(fixture.Storage, first, 40);

        fixture.Time.Advance(TimeSpan.FromHours(2));
        Assert.True((await fixture.Merger().RunPassAsync(default)).Deleted > 0);
        Assert.Equal(first, await fixture.First());
        Assert.True(fixture.Size <= 2000 + 256, $"{fixture.Size} bytes remain");
        await AssertReadsFrom(fixture.Storage, first, 40);
        await Assert.ThrowsAsync<EventLogOffsetNotAvailableException>(() =>
            new EventLogStorageReader(fixture.Storage, Topic).ReadAllAsync(0));

        // Writing, merging and cleanup continue above the cut, and the cut follows the growth.
        await fixture.Publish(40);
        for (var i = 0; i < 3; i++)
        {
            await fixture.Merger().RunPassAsync(default);
            fixture.Time.Advance(TimeSpan.FromHours(2));
        }
        var later = await fixture.First();
        Assert.True(later > first);
        Assert.True(fixture.Size <= 2000 + 256, $"{fixture.Size} bytes remain");
        await AssertReadsFrom(fixture.Storage, later, 80);
        Assert.Equal(80ul, (await new EventLogStorageReader(fixture.Storage, Topic).FindTailAsync(default))!.NextOffset);
    }

    [Fact]
    public async Task AgeRetentionWaitsUntilTheOldestObjectExpiresAndAlwaysKeepsTheLatestSplit()
    {
        var fixture = new Fixture(new(MaxAge: TimeSpan.FromHours(10)));
        await fixture.Start();
        await fixture.Publish(10);
        fixture.Time.Advance(TimeSpan.FromHours(5));
        await fixture.Publish(10);
        var result = await fixture.Merger().RunPassAsync(default);
        Assert.Equal(0ul, await fixture.First());
        // An idle topic wakes when its oldest retained object expires.
        Assert.NotNull(result.NextDeadline);
        Assert.True(result.NextDeadline <= DateTimeOffset.UnixEpoch + TimeSpan.FromHours(10));

        // Merged objects count from their merge, so the first ten records expire 10 hours after it.
        fixture.Time.Advance(TimeSpan.FromHours(11));
        await fixture.Merger().RunPassAsync(default);
        var first = await fixture.First();
        Assert.True(first > 0);
        await AssertReadsFrom(fixture.Storage, first, 20);

        // Everything is old now: all history except the latest split goes.
        fixture.Time.Advance(TimeSpan.FromDays(10));
        for (var i = 0; i < 3; i++)
        {
            await fixture.Merger().RunPassAsync(default);
            fixture.Time.Advance(TimeSpan.FromHours(2));
        }
        var tail = (await new EventLogStorageReader(fixture.Storage, Topic).FindTailAsync(default))!;
        Assert.Contains(EventLogFormat.SplitKey(Topic, tail.SplitId), fixture.Storage.Keys);
        first = await fixture.First();
        Assert.True(first <= tail.Object.FirstOffset);
        Assert.True(first >= 18, $"first {first}");
        await AssertReadsFrom(fixture.Storage, first, 20);
        // Once the history is trimmed and cleaned, an idle pass has nothing to do before new history.
        var idle = await fixture.Merger().RunPassAsync(default);
        Assert.Equal((0, 0, 0), (idle.Created, idle.Marked, idle.Deleted));
    }

    [Fact]
    public async Task ServiceReportsTheFirstRetainedOffsetAndKeepsTopicsWithoutRetention()
    {
        var time = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var storage = new InMemoryEventLogStorage(time);
        var transport = new InProcessEventLogTransport();
        await using var service = new EventLogService(storage, transport.From("a"), "a", new SystemReplicationScheduler(),
            new()
            {
                SplitCap = 256, SealFreeSpace = 200, MergeFanOut = 2, MergeLevels = 2, TimeProvider = time,
                MergeInterval = TimeSpan.FromHours(1), DeletionDelay = TimeSpan.FromHours(1),
                Retention = name => name == "coordination" ? new(MaxBytes: 1000) : null
            });
        transport.Register("a", service);
        var coordination = (EventLogTopic)service.GetTopic("coordination");
        var metadata = (EventLogTopic)service.GetTopic("metadata");
        for (var i = 0; i < 30; i++)
        {
            await coordination.PublishAsync(Bytes(i, 30));
            await metadata.PublishAsync(Bytes(i, 30));
        }
        Assert.Equal(new EventLogBounds(0, 30), await coordination.GetBoundsAsync());
        Assert.NotNull(await coordination.MergeNowAsync());
        Assert.NotNull(await metadata.MergeNowAsync());
        var bounds = await coordination.GetBoundsAsync();
        Assert.True(bounds.First > 0);
        Assert.Equal(30ul, bounds.Next);
        Assert.Equal(new EventLogBounds(0, 30), await metadata.GetBoundsAsync());
        await Assert.ThrowsAsync<EventLogOffsetNotAvailableException>(async () =>
        {
            await foreach (var _ in coordination.ReadAsync(0, 30)) { }
        });
        var records = new List<ulong>();
        await foreach (var record in coordination.ReadAsync(bounds.First, 30)) records.Add(record.Offset);
        Assert.Equal(Enumerable.Range((int)bounds.First, 30 - (int)bounds.First).Select(i => (ulong)i), records);
        records.Clear();
        await foreach (var record in metadata.ReadAsync(0, 30)) records.Add(record.Offset);
        Assert.Equal(30, records.Count);
    }

    [Fact]
    public void InvalidRetentionIsRejected()
    {
        var options = new EventLogOptions { Retention = _ => new(MaxBytes: 0) };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate(Topic));
        options = new() { Retention = _ => new(MaxAge: TimeSpan.Zero) };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate(Topic));
        new EventLogOptions { Retention = _ => EventLogRetention.Forever }.Validate(Topic);
    }
}
