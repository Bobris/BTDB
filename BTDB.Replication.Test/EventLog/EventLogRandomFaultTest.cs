using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

/// One publisher session submits records through owner lanes while storage mutations randomly time out without
/// effect or lose their response, owners take over and merges run. Every receipt names the record's offset, and the
/// history holds each record exactly once, in order.
public class EventLogRandomFaultTest
{
    const string Topic = "t";
    static readonly EventLogOwner[] Owners = [new("a", "http://a"), new("b", "http://b")];

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task RecordsCommitOnceInOrderUnderFaults(int seed)
    {
        var random = new Random(seed);
        var storage = new FaultyEventLogStorage();
        var options = new EventLogOptions { SplitCap = 256, SealFreeSpace = 64, MergeFanOut = 2, MergeLevels = 2 };
        var faultsOn = false;
        storage.Fault = _ =>
        {
            if (!faultsOn) return StorageFault.None;
            lock (random)
                return random.Next(10) switch
                {
                    0 => StorageFault.TimeoutWithoutEffect,
                    1 => StorageFault.LoseResponse,
                    _ => StorageFault.None
                };
        };

        async Task<EventLogOwnerLane> NewLane() => await EventLogOwnerLane.CreateCandidateAsync(storage,
            new(storage, Topic), Topic, options, new TestScheduler(), Owners[random.Next(Owners.Length)], default);

        var lane = await NewLane();
        faultsOn = true;
        var log = new List<string>();
        ulong resumeFrom = 0;
        try
        {
            for (var i = 0; i < 60; i++)
            {
                var sequence = (ulong)i + 1;
                var dispatched = false;
                for (var attempt = 0; ; attempt++)
                {
                    Assert.True(attempt < 50, "Too many attempts");
                    if (random.Next(15) == 0)
                    {
                        log.Add($"{i}: takeover");
                        lane = await NewLane();
                    }

                    try
                    {
                        dispatched = true;
                        var offset = await lane.SubmitAsync(1, sequence, Bytes(i, 20), sequence, attempt > 0 || dispatched,
                            resumeFrom, CancellationToken.None);
                        Assert.Equal((ulong)i, offset);
                        resumeFrom = offset + 1;
                        log.Add($"{i}: committed at {offset} after {attempt} retries");
                        break;
                    }
                    catch (EventLogOwnerLostException ex)
                    {
                        log.Add($"{i}: {ex.GetType().Name}");
                        if (lane.IsStopped) lane = await NewLane();
                    }
                }

                if (random.Next(8) == 0)
                {
                    await new EventLogMerger(storage, Topic, options, new TestScheduler()).RunPassAsync(default);
                }
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Seed {seed}:{Environment.NewLine}{string.Join(Environment.NewLine, log.TakeLast(10))}",
                ex);
        }

        faultsOn = false;
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, 60).Select(i => (ulong)i), records.Select(r => r.Offset));
        Assert.Equal(Enumerable.Range(0, 60).Select(i => Bytes(i, 20)), records.Select(r => r.Payload.ToArray()));
    }

    /// Several publisher sessions submit at the same time through lanes of competing owners, with the same faults:
    /// each session's records commit exactly once and in its order, and every receipt names that session's record.
    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task ConcurrentSessionsCommitOnceInTheirOrderUnderFaults(int seed)
    {
        var shared = new Random(seed);
        var storage = new FaultyEventLogStorage();
        var options = new EventLogOptions { SplitCap = 256, SealFreeSpace = 64, MergeFanOut = 2, MergeLevels = 2 };
        var faultsOn = false;
        storage.Fault = _ =>
        {
            if (!faultsOn) return StorageFault.None;
            lock (shared)
                return shared.Next(10) switch
                {
                    0 => StorageFault.TimeoutWithoutEffect,
                    1 => StorageFault.LoseResponse,
                    _ => StorageFault.None
                };
        };

        async Task<EventLogOwnerLane> NewLane(Random random) => await EventLogOwnerLane.CreateCandidateAsync(storage,
            new(storage, Topic), Topic, options, new TestScheduler(), Owners[random.Next(Owners.Length)], default);

        await (await NewLane(shared)).SubmitAsync(9, 1, new byte[] { 9 }, 1, false, 0, CancellationToken.None);
        faultsOn = true;
        const int sessions = 3;
        const int perSession = 25;
        var receipts = new ulong[sessions, perSession];
        await Task.WhenAll(Enumerable.Range(0, sessions).Select(session => Task.Run(async () =>
        {
            var random = new Random(seed * 31 + session);
            var lane = await NewLane(random);
            ulong resumeFrom = 0;
            for (var i = 0; i < perSession; i++)
            {
                var sequence = (ulong)i + 1;
                for (var attempt = 0; ; attempt++)
                {
                    Assert.True(attempt < 100, "Too many attempts");
                    if (random.Next(12) == 0) lane = await NewLane(random);
                    try
                    {
                        var offset = await lane.SubmitAsync((ulong)session + 1, sequence, Payload(session, i), sequence,
                            true, resumeFrom, CancellationToken.None);
                        receipts[session, i] = offset;
                        resumeFrom = offset + 1;
                        break;
                    }
                    catch (EventLogOwnerLostException)
                    {
                        if (lane.IsStopped) lane = await NewLane(random);
                    }
                }
            }
        })));

        faultsOn = false;
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, records.Count).Select(i => (ulong)i), records.Select(r => r.Offset));
        var payloads = records.Skip(1).Select(r => Convert.ToHexString(r.Payload.ToArray())).ToList();
        Assert.Equal(sessions * perSession, payloads.Count);
        Assert.Equal(payloads.Count, payloads.Distinct().Count());
        for (var session = 0; session < sessions; session++)
        {
            var mine = payloads.Where(p => p.StartsWith(Convert.ToHexString([(byte)session]))).ToList();
            Assert.Equal(Enumerable.Range(0, perSession).Select(i => Convert.ToHexString(Payload(session, i))), mine);
            for (var i = 0; i < perSession; i++)
                Assert.Equal(Convert.ToHexString(Payload(session, i)),
                    Convert.ToHexString(records[(int)receipts[session, i]].Payload.ToArray()));
        }
    }

    static byte[] Payload(int session, int i) => [(byte)session, (byte)i, .. Bytes(i, 10)];

    /// Changes a split's version right after every listing, as concurrent appends to it do.
    sealed class VersionBumpingStorage(InMemoryEventLogStorage inner, string bumpedKey) : IEventLogStorage
    {
        public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation) => inner.ReadAsync(key, cancellation);
        public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation) => inner.GetPropertiesAsync(key, cancellation);
        public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, version, offset, destination, cancellation);
        public ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content, EventLogOwner? owner, string? sha256, CancellationToken cancellation) => inner.WriteAsync(key, expectedVersion, content, owner, sha256, cancellation);
        public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner, CancellationToken cancellation) => inner.SetOwnerAsync(key, expectedVersion, owner, cancellation);
        public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay, CancellationToken cancellation) => inner.ScheduleDeletionAsync(key, version, delay, cancellation);
        public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation) => inner.DeleteAsync(key, version, cancellation);

        public async IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellation)
        {
            await foreach (var info in inner.ListAsync(prefix, cancellation)) yield return info;
            var properties = await inner.GetPropertiesAsync(bumpedKey, cancellation);
            await inner.SetOwnerAsync(bumpedKey, properties!.Version, Owners[0], cancellation);
        }
    }

    /// A split whose version changes between a listing and the read of its header still gives its first offset, as
    /// only frames, seals, owners and cleanup marks change it. Before the fix every attempt met a changed version, and
    /// the reader reported a committed offset as not available.
    [Fact]
    public async Task ReaderLocatesOffsetsWhileListedSplitsChange()
    {
        var inner = new InMemoryEventLogStorage();
        var lane = await EventLogOwnerLane.CreateCandidateAsync(inner, new(inner, Topic), Topic,
            new EventLogOptions { SplitCap = 256, SealFreeSpace = 64 }, new TestScheduler(), Owners[0], default);
        for (var i = 0; i < 12; i++)
            Assert.Equal((ulong)i, await lane.SubmitAsync(1, (ulong)i + 1, Bytes(i, 40), (ulong)i + 1, false, 0, default));
        var splits = inner.Keys.Order().ToList();
        Assert.True(splits.Count >= 3);
        var storage = new VersionBumpingStorage(inner, splits[splits.Count / 2]);
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync(5);
        Assert.Equal(Enumerable.Range(5, 7).Select(i => (ulong)i), records.Select(r => r.Offset));
    }
}
