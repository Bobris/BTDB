using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

/// <summary>Storage whose writes complete synchronously and take a while, like <see cref="FileSystemEventLogStorage"/>
/// (a flushed file write under a lock). Counts the writes that carry records.</summary>
sealed class SlowSynchronousStorage(IEventLogStorage inner, TimeSpan writeDuration) : IEventLogStorage
{
    int _writes;
    public int Writes => Volatile.Read(ref _writes);

    public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken c) => inner.ReadAsync(key, c);
    public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken c) => inner.GetPropertiesAsync(key, c);
    public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination, CancellationToken c) =>
        inner.ReadRangeAsync(key, version, offset, destination, c);

    public ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
        EventLogOwner? owner, string? sha256, CancellationToken c)
    {
        Interlocked.Increment(ref _writes);
        Thread.Sleep(writeDuration);
        return inner.WriteAsync(key, expectedVersion, content, owner, sha256, c);
    }

    public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner, CancellationToken c) =>
        inner.SetOwnerAsync(key, expectedVersion, owner, c);
    public IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken c) => inner.ListAsync(prefix, c);
    public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay, CancellationToken c) =>
        inner.ScheduleDeletionAsync(key, version, delay, c);
    public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken c) => inner.DeleteAsync(key, version, c);
}

public class EventLogSynchronousStorageTest
{
    const int Records = 200;

    static async Task<(EventLogService Service, SlowSynchronousStorage Storage, InProcessEventLogTransport Transport)> Start()
    {
        var storage = new SlowSynchronousStorage(new InMemoryEventLogStorage(), TimeSpan.FromMilliseconds(10));
        var transport = new InProcessEventLogTransport();
        var service = new EventLogService(storage, transport.From("a"), "a", new SystemReplicationScheduler(),
            new() { SplitCap = 64 * 1024, SealFreeSpace = 1024, AdaptiveCommitLingerMax = TimeSpan.Zero });
        transport.Register("a", service);
        await service.GetTopic("t").PublishAsync(Bytes(-1)); // takes the topic over before measuring
        return (service, storage, transport);
    }

    [Fact]
    public async Task OneThreadPublishingWithoutAwaitingSharesWrites()
    {
        var (service, storage, _) = await Start();
        await using var _service = service;
        var topic = service.GetTopic("t");
        var before = storage.Writes;
        // Like EventPublisher's single publication lane: it hands records over and awaits the receipts later.
        var receipts = new List<ValueTask<ulong>>();
        for (var i = 0; i < Records; i++) receipts.Add(topic.PublishAsync(Bytes(i)));
        var offsets = new List<ulong>();
        foreach (var receipt in receipts) offsets.Add(await receipt);
        Assert.Equal(Enumerable.Range(1, Records).Select(i => (ulong)i), offsets);
        // Before the fix the publishing thread ran each commit itself, so every record took its own write.
        Assert.True(storage.Writes - before < Records / 4, $"{storage.Writes - before} writes for {Records} records");
    }

    [Fact]
    public async Task ConcurrentPublishersShareWrites()
    {
        var (service, storage, _) = await Start();
        await using var _service = service;
        var topic = service.GetTopic("t");
        var before = storage.Writes;
        var offsets = await Task.WhenAll(Enumerable.Range(0, Records)
            .Select(i => Task.Run(async () => await topic.PublishAsync(Bytes(i)))));
        Assert.Equal(Enumerable.Range(1, Records).Select(i => (ulong)i), offsets.Order());
        Assert.True(storage.Writes - before < Records / 4, $"{storage.Writes - before} writes for {Records} records");
    }
}
