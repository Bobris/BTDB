using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.Test.EventLog;

/// <summary>Nodes publish concurrently and follow the topic live while random nodes lose the peer network or their
/// storage for a while, or restart, and storage mutations fail ambiguously, which moves ownership between them.</summary>
public class EventLogClusterChaosTest
{
    const string Topic = "events";
    static readonly string[] Names = ["a", "b", "c"];

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 12).Select(seed => new object[] { seed });

    static byte[] Payload(int node, int generation, int i) => [(byte)node, (byte)generation, (byte)(i >> 8), (byte)i, 7, 7];

    /// <summary>Storage mutations also randomly time out without effect or lose their response. Every publication
    /// that returned an offset is stored there; a publication cut short by its node's
    /// restart is stored at most once; the records of each publisher session commit in call order; and every live
    /// reader sees a gap-free prefix of the stored history.</summary>
    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task PublicationsCommitOnceInCallOrderUnderPartitionsAndRestarts(int seed)
    {
        const int perNode = 150;
        var random = new Random(seed);
        var storage = new FaultyEventLogStorage();
        var faults = random.Next(4) * 4; // percent of storage mutations that time out or lose their response
        var faultRandom = new Random(-seed);
        storage.Fault = _ =>
        {
            lock (faultRandom)
                return faultRandom.Next(100) switch
                {
                    var n when n < faults / 2 => StorageFault.TimeoutWithoutEffect,
                    var n when n < faults => StorageFault.LoseResponse,
                    _ => StorageFault.None
                };
        };
        await using var cluster = new EventLogCluster(storage);
        var options = EventLogCluster.FastOptions with
        {
            SplitCap = 512, SealFreeSpace = 128, MergeFanOut = 2, MergeLevels = 2,
            MergeInterval = TimeSpan.FromMilliseconds(20), DeletionDelay = TimeSpan.FromMilliseconds(random.Next(0, 50))
        };
        var services = Names.Select(name => cluster.Start(name, options)).ToArray();
        var generations = new int[Names.Length];
        using var stop = new CancellationTokenSource();
        using var readStop = new CancellationTokenSource();
        var pauses = Names.Select(_ => random.Next(1, 4)).ToArray();
        var restarts = random.Next(2) == 0;
        // node, generation, i → offset, or null when the outcome is unknown
        var outcomes = new List<(int Node, int Generation, int I, ulong? Offset)>[Names.Length];
        var publishers = Names.Select((_, node) => Task.Run(async () =>
        {
            var mine = outcomes[node] = [];
            var i = 0;
            while (i < perNode)
            {
                var generation = Volatile.Read(ref generations[node]);
                var pending = new List<(int I, Task<ulong> Offset)>();
                try
                {
                    var topic = Volatile.Read(ref services[node]).GetTopic(Topic);
                    for (; i < perNode; i++)
                    {
                        pending.Add((i, topic.PublishAsync(Payload(node, generation, i)).AsTask()));
                        if (i % pauses[node] == 0) await Task.Delay(1);
                    }
                }
                catch (ObjectDisposedException)
                {
                }
                foreach (var (index, offset) in pending)
                {
                    try
                    {
                        mine.Add((node, generation, index, await offset));
                    }
                    catch (ObjectDisposedException)
                    {
                        mine.Add((node, generation, index, null));
                    }
                }
                if (i < perNode)
                    while (Volatile.Read(ref generations[node]) == generation) await Task.Delay(1);
            }
        })).ToArray();
        var readers = Names.Select((name, node) => Task.Run(async () =>
        {
            var seen = new List<EventLogRecord>();
            while (!readStop.IsCancellationRequested)
            {
                try
                {
                    await foreach (var record in Volatile.Read(ref services[node]).GetTopic(Topic)
                                       .ReadAsync((ulong)seen.Count, null, readStop.Token))
                        seen.Add(new(record.Offset, record.Payload.ToArray()));
                }
                catch (OperationCanceledException) when (readStop.IsCancellationRequested)
                {
                }
                catch (Exception)
                {
                    await Task.Delay(5);
                }
            }
            return seen;
        })).ToArray();
        var chaos = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var victim = random.Next(Names.Length);
                var name = Names[victim];
                switch (random.Next(restarts ? 4 : 3))
                {
                    case 0:
                        cluster.Nodes[name].Storage.Unavailable = true;
                        await Task.Delay(random.Next(5, 400));
                        cluster.Nodes[name].Storage.Unavailable = false;
                        break;
                    case 3:
                        await cluster.Stop(name);
                        await Task.Delay(random.Next(0, 50));
                        Volatile.Write(ref services[victim], cluster.Start(name, options));
                        Interlocked.Increment(ref generations[victim]);
                        break;
                    default:
                        cluster.Transport.Partition(name);
                        await Task.Delay(random.Next(5, 400));
                        cluster.Transport.Heal(name);
                        break;
                }
                await Task.Delay(random.Next(0, 100));
            }
        });
        var all = Task.WhenAll(publishers);
        await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(3)));
        await stop.CancelAsync();
        await chaos;
        await all.WaitAsync(TimeSpan.FromSeconds(30));

        var records = await new EventLogStorageReader(cluster.Storage, Topic, options.MergeLevels).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, records.Count).Select(i => (ulong)i), records.Select(r => r.Offset));
        var stored = records.Select(r => Convert.ToHexString(r.Payload.Span)).ToList();
        Assert.Equal(stored.Count, stored.Distinct().Count());
        var position = stored.Select((payload, offset) => (payload, offset)).ToDictionary(p => p.payload, p => p.offset);
        foreach (var (node, generation, i, offset) in outcomes.SelectMany(o => o))
        {
            var payload = Convert.ToHexString(Payload(node, generation, i));
            if (offset is { } known) Assert.True(position.TryGetValue(payload, out var at) && at == (int)known,
                $"Seed {seed}: {node}/{generation}/{i} returned {known}");
        }
        foreach (var session in records.GroupBy(r => (r.Payload.Span[0], r.Payload.Span[1])))
        {
            var order = session.Select(r => (r.Payload.Span[2] << 8) | r.Payload.Span[3]).ToList();
            Assert.True(order.SequenceEqual(order.Order()), $"Seed {seed}: session {session.Key} out of call order");
        }
        Assert.Equal(outcomes.Sum(o => o.Count(x => x.Offset != null)) +
                     outcomes.Sum(o => o.Count(x => x.Offset == null && position.ContainsKey(
                         Convert.ToHexString(Payload(x.Node, x.Generation, x.I))))), records.Count);

        await Task.Delay(300);
        await readStop.CancelAsync();
        foreach (var reader in readers)
        {
            var seen = await reader.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(seen.Count <= records.Count, $"Seed {seed}: reader saw more than stored");
            for (var i = 0; i < seen.Count; i++)
            {
                Assert.Equal((ulong)i, seen[i].Offset);
                Assert.True(records[i].Payload.Span.SequenceEqual(seen[i].Payload.Span), $"Seed {seed}: read {i}");
            }
        }
    }
}
