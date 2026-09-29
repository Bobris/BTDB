using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

public class EventLogMergerTest
{
    const string Topic = "t";
    static readonly EventLogOwner A = new("a", "http://a");

    sealed class Fixture
    {
        public readonly ManualTimeProvider Time = new(DateTimeOffset.UnixEpoch);
        public readonly FaultyEventLogStorage Storage;
        public readonly EventLogOptions Options;
        public EventLogOwnerLane Lane = null!;
        ulong _sequence;

        public Fixture(int fanOut = 2)
        {
            Storage = new(Time);
            Options = new()
            {
                SplitCap = 256, SealFreeSpace = 200, MergeFanOut = fanOut, MergeLevels = 2,
                DeletionDelay = TimeSpan.FromHours(1), TimeProvider = Time
            };
        }

        public async Task Start() => Lane = await EventLogOwnerLane.CreateCandidateAsync(Storage,
            new(Storage, Topic), Topic, Options, new TestScheduler(), A, default);

        public async Task Publish(int count)
        {
            for (var i = 0; i < count; i++)
            {
                _sequence++;
                Assert.Equal(_sequence - 1, await Lane.SubmitAsync(1, _sequence, Bytes((int)_sequence - 1, 30), 1, false, 0, default));
            }
        }

        public EventLogMerger Merger() => new(Storage, Topic, Options, new TestScheduler());

        public string[] Keys => Storage.Inner.Keys.ToArray();
    }

    static async Task AssertReadable(IEventLogStorage storage, int count)
    {
        var reader = new EventLogStorageReader(storage, Topic);
        var all = await reader.ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, count).Select(i => Bytes(i, 30)), all.Select(r => r.Payload.ToArray()));
        foreach (var start in new[] { 0, 1, 5, count / 2, count - 1 })
        {
            var from = await new EventLogStorageReader(storage, Topic).ReadAllAsync((ulong)start);
            Assert.Equal(Enumerable.Range(start, count - start).Select(i => (ulong)i), from.Select(r => r.Offset));
        }
    }

    [Fact]
    public async Task SplitsMergeIntoTwoLevelsNamedByFirstOffsetAndStayReadable()
    {
        var fixture = new Fixture();
        await fixture.Start();
        await fixture.Publish(20); // one record per split with this cap and threshold
        var splits = fixture.Keys.Length;
        Assert.True(splits >= 20);
        var result = await fixture.Merger().RunPassAsync(default);
        Assert.True(result.Created > 0);
        var keys = fixture.Keys;
        Assert.Contains(EventLogFormat.MergedKey(Topic, 1, 0), keys);
        Assert.Contains(EventLogFormat.MergedKey(Topic, 1, 2), keys);
        Assert.Contains(EventLogFormat.MergedKey(Topic, 2, 0), keys);
        Assert.Contains(EventLogFormat.MergedKey(Topic, 2, 4), keys);
        await AssertReadable(fixture.Storage, 20);
        // Nothing is deleted before the deadline; the covered objects are only marked.
        Assert.Equal(0, result.Deleted);
        Assert.True(result.Marked > 0);
        fixture.Time.Advance(TimeSpan.FromHours(2));
        var cleanup = await fixture.Merger().RunPassAsync(default);
        Assert.True(cleanup.Deleted > 0);
        await AssertReadable(fixture.Storage, 20);
        Assert.True(fixture.Keys.Length < splits / 2);
        // The owner keeps appending after its history was merged and cleaned up.
        await fixture.Publish(5);
        await AssertReadable(fixture.Storage, 25);
        Assert.Equal(25ul, (await new EventLogStorageReader(fixture.Storage, Topic).FindTailAsync(default))!.NextOffset);
    }

    [Fact]
    public async Task TailAndItsGroupAreNeverMerged()
    {
        var fixture = new Fixture();
        await fixture.Start();
        await fixture.Publish(2); // splits 1, 2 and maybe the empty-successor tail
        var tail = (await new EventLogStorageReader(fixture.Storage, Topic).FindTailAsync(default))!;
        await fixture.Merger().RunPassAsync(default);
        foreach (var key in fixture.Keys)
            if (EventLogFormat.TryParseKey(Topic, key, out var level, out var number) && level == 1)
                Assert.True(number < tail.Object.FirstOffset);
        Assert.Contains(EventLogFormat.SplitKey(Topic, tail.SplitId), fixture.Keys);
    }

    [Fact]
    public async Task ConcurrentAndRepeatedMergersProduceIdenticalObjects()
    {
        var fixture = new Fixture();
        await fixture.Start();
        await fixture.Publish(18);
        await Task.WhenAll(fixture.Merger().RunPassAsync(default).AsTask(), fixture.Merger().RunPassAsync(default).AsTask());
        var contents = new Dictionary<string, byte[]>();
        foreach (var key in fixture.Keys.Where(k => k.Contains("/l")))
            contents[key] = (await fixture.Storage.ReadAsync(key, default))!.Content.ToArray();
        await fixture.Merger().RunPassAsync(default);
        foreach (var (key, content) in contents)
            Assert.Equal(content, (await fixture.Storage.ReadAsync(key, default))!.Content.ToArray());
        await AssertReadable(fixture.Storage, 18);
    }

    [Fact]
    public async Task MergedObjectCreatedBeforeACrashIsVerifiedAndCleanedByTheNextPass()
    {
        var fixture = new Fixture();
        await fixture.Start();
        await fixture.Publish(12);
        // The first merger crashes right after creating objects: every deletion mark fails.
        fixture.Storage.Fault = o => StorageFault.None;
        var crashing = new CrashAfterCreateStorage(fixture.Storage);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await new EventLogMerger(crashing, Topic, fixture.Options, new TestScheduler()).RunPassAsync(default));
        Assert.Contains(fixture.Keys, k => k.Contains("/l1/"));
        var next = await fixture.Merger().RunPassAsync(default);
        Assert.True(next.Marked > 0);
        fixture.Time.Advance(TimeSpan.FromHours(2));
        Assert.True((await fixture.Merger().RunPassAsync(default)).Deleted > 0);
        await AssertReadable(fixture.Storage, 12);
    }

    [Fact]
    public async Task ReadingFromEveryOffsetUsesTheHeaderIndexOfMergedObjects()
    {
        var fixture = new Fixture(fanOut: 4);
        await fixture.Start();
        await fixture.Publish(40);
        await fixture.Merger().RunPassAsync(default);
        fixture.Time.Advance(TimeSpan.FromHours(2));
        await fixture.Merger().RunPassAsync(default);
        Assert.Contains(fixture.Keys, k => k.Contains("/l2/"));
        for (var start = 0; start < 40; start++)
        {
            var from = await new EventLogStorageReader(fixture.Storage, Topic).ReadAllAsync((ulong)start);
            Assert.Equal(Enumerable.Range(start, 40 - start).Select(i => Bytes(i, 30)), from.Select(r => r.Payload.ToArray()));
        }
    }

    [Fact]
    public async Task OwnerServiceRunsMergesInTheBackground()
    {
        var time = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var storage = new InMemoryEventLogStorage(time);
        var transport = new InProcessEventLogTransport();
        await using var service = new EventLogService(storage, transport.From("a"), "a", new SystemReplicationScheduler(),
            new() { SplitCap = 256, SealFreeSpace = 200, MergeFanOut = 2, MergeLevels = 2, TimeProvider = time,
                MergeInterval = TimeSpan.FromMilliseconds(20), DeletionDelay = TimeSpan.FromHours(1) });
        transport.Register("a", service);
        var topic = service.GetTopic(Topic);
        for (var i = 0; i < 12; i++) await topic.PublishAsync(Bytes(i, 30));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!storage.Keys.Any(k => k.Contains("/l2/")) && DateTime.UtcNow < deadline) await Task.Delay(10);
        Assert.Contains(storage.Keys, k => k.Contains("/l2/"));
        var records = new List<ulong>();
        await foreach (var record in topic.ReadAsync(0, 12)) records.Add(record.Offset);
        Assert.Equal(Enumerable.Range(0, 12).Select(i => (ulong)i), records);
    }

    /// <summary>Lets creates through, then fails the first deletion mark like a crash.</summary>
    sealed class CrashAfterCreateStorage(IEventLogStorage inner) : IEventLogStorage
    {
        public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken c) => inner.ReadAsync(key, c);
        public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken c) => inner.GetPropertiesAsync(key, c);
        public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination, CancellationToken c) =>
            inner.ReadRangeAsync(key, version, offset, destination, c);
        public ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
            EventLogOwner? owner, string? sha256, CancellationToken c) => inner.WriteAsync(key, expectedVersion, content, owner, sha256, c);
        public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner, CancellationToken c) =>
            inner.SetOwnerAsync(key, expectedVersion, owner, c);
        public IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken c) => inner.ListAsync(prefix, c);
        public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay, CancellationToken c) =>
            throw new InvalidOperationException("Crash before marking.");
        public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken c) => inner.DeleteAsync(key, version, c);
    }
}
