using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

// Seeded exploration of whole-node schedules beyond the named cases: three real coordinators under virtual time with
// random input pacing, Blob outages, peer partitions and process pauses. Every step checks that no two nodes hold
// lease authority at once; after healing, the published history must reach the last input with the deterministic
// value of every event, on every surviving node too. A failing seed is kept below as a regression case.
public partial class ReplicationNodeCoordinatorTest
{
    const ulong LastEvent = 40;

    static byte EventValue(ulong id) => (byte)(id * 7 % 251 + 1);

    // BTDB_REPLICATION_EXPLORATION_SEEDS=first:count widens the sweep locally; CI runs the default range.
    public static IEnumerable<object[]> ExplorationSeeds()
    {
        var range = (Environment.GetEnvironmentVariable("BTDB_REPLICATION_EXPLORATION_SEEDS") ?? "1:200").Split(':');
        return Enumerable.Range(int.Parse(range[0]), int.Parse(range[1])).Select(seed => new object[] { (ulong)seed });
    }

    [Theory]
    [MemberData(nameof(ExplorationSeeds))]
    public async Task RandomSchedulesKeepOneAuthorityAndConvergeOnOneHistory(ulong seed)
    {
        var random = new SeededRandom(seed);
        var trace = new StringBuilder();
        await using var cluster = await Cluster.Create();
        // Per seed: TRL rotation and local compaction vary, so schedules also cross files and TRL retention. (The
        // in-process store has no delayed cleanup; checkpoints and remote cleanup races are covered on Azure.)
        var smallLogs = random.NextUInt64() % 2 == 0;
        var compaction = random.NextUInt64() % 2 == 0 ? 25L : (long?)null;
        var nodes = "abc".Select(name => cluster.Start(name.ToString(), smallLogs: smallLogs, compactionTicks: compaction))
            .ToArray();
        var names = new[] { "a", "b", "c" };
        var replacements = 0;
        string Explain(string failure) => $"seed={seed}: {failure}\n{trace}" +
            string.Join("\n", nodes.Select(n => $"{n.Coordinator.Role} ready {n.Status.Current.Ready} local {LocalEvent(n)} " +
                $"completed {n.Capture.Completed} acknowledged {n.Capture.Acknowledged} " +
                $"compared {n.Status.Current.Databases.FirstOrDefault()?.Compared} " +
                $"published {n.Status.Current.Databases.FirstOrDefault()?.Published} failed steps {n.Status.FailedSteps} " +
                $"polls {n.Peers.Polls} reads {n.Peers.Reads} restarts: {string.Join("; ", n.RestartReasons)}"));

        void Check(string step)
        {
            var authorities = nodes.Count(n => n.Leases.Current != null);
            if (authorities > 1) Assert.Fail(Explain($"{authorities} nodes hold lease authority after {step}"));
        }

        for (var step = 0; step < 150; step++)
        {
            var index = (int)(random.NextUInt64() % (ulong)nodes.Length);
            var node = nodes[index];
            var action = random.NextUInt64() % 11;
            string description;
            switch (action)
            {
                case < 4:
                    description = $"apply on {index}";
                    await Apply(node, 1 + (int)(random.NextUInt64() % 3));
                    break;
                case 4:
                    node.Storage.Unavailable = !node.Storage.Unavailable;
                    description = $"storage {index} {(node.Storage.Unavailable ? "down" : "up")}";
                    break;
                case 10 when replacements < 3:
                    // The node stops (fencing its lease) and a replacement restores from Blob under a new name.
                    await node.Cancellation.CancelAsync();
                    cluster.Isolated.Remove(names[index]);
                    names[index] = $"r{++replacements}";
                    nodes[index] = cluster.Start(names[index], smallLogs: smallLogs, compactionTicks: compaction);
                    description = $"replace {index} by {names[index]}";
                    break;
                case 5:
                    var name = names[index];
                    if (!cluster.Isolated.Remove(name)) cluster.Isolated.Add(name);
                    description = $"peers {index} {(cluster.Isolated.Contains(name) ? "cut" : "healed")}";
                    break;
                case 6 when random.NextUInt64() % 2 == 0:
                    // Slow Blob writes keep publication in flight across takeovers and partitions.
                    node.Storage.WriteDelayTicks = node.Storage.WriteDelayTicks == 0 ? 1 + (long)(random.NextUInt64() % 40) : 0;
                    description = $"write delay {index} {node.Storage.WriteDelayTicks}";
                    break;
                case 6:
                    var paused = random.NextUInt64() % 2 == 0;
                    node.Paused = paused;
                    description = $"{(paused ? "pause" : "resume")} {index}";
                    break;
                default:
                    var ticks = 1 + (long)(random.NextUInt64() % 60);
                    cluster.Advance(ticks);
                    description = $"advance {ticks}";
                    break;
            }
            trace.AppendLine(description);
            Check(description);
        }

        // Heal everything, finish every surviving node's input and let the cluster converge.
        cluster.Isolated.Clear();
        foreach (var node in nodes)
        {
            node.Storage.Unavailable = false;
            node.Storage.WriteDelayTicks = 0;
            node.Paused = false;
        }
        trace.AppendLine("heal");
        for (var round = 0; round < 400; round++)
        {
            foreach (var node in nodes) await Apply(node, (int)LastEvent);
            cluster.Advance(10);
            Check("healing");
            if (nodes.Where(n => !n.Run.IsCompleted).All(n => n.Status.Current.Ready && n.Db != null &&
                    n.Capture.Completed <= n.Capture.Acknowledged && LocalEvent(n) == LastEvent) &&
                await PublishedEvent(cluster) == LastEvent)
                break;
        }
        var survivors = nodes.Where(n => !n.Run.IsCompleted).ToArray();
        if (survivors.Length == 0) Assert.Fail(Explain("every node stopped"));
        if (await PublishedEvent(cluster) != LastEvent) Assert.Fail(Explain($"published history stopped at {await PublishedEvent(cluster)}"));
        await AssertRestoresEveryEvent(cluster);
        foreach (var node in survivors)
            if (LocalEvent(node) != LastEvent || node.Capture.Completed > node.Capture.Acknowledged)
                Assert.Fail(Explain($"{node.Coordinator.Role} node did not converge"));
    }

    // Found by seed 1051: a replacement restored from Blob became leader without any local commit, so capture reported
    // no complete prefix and every follower read failed until new input arrived.
    [Fact]
    public async Task LeaderRestoredFromBlobServesItsRestoredHistoryBeforeAnyLocalCommit()
    {
        await using var cluster = await Cluster.Create();
        var old = cluster.Start("old");
        var follower = cluster.Start("follower");
        cluster.Advance(10);
        cluster.Isolated.Add("follower"); // Its comparison, and thus its canonical base, stays behind.
        for (ulong id = 2; id <= 5; id++)
        {
            await old.Write(id, EventValue(id));
            await follower.Write(id, EventValue(id));
        }
        cluster.Advance(20);
        Assert.Equal(5ul, await cluster.RestoreEvent());
        var replacement = cluster.Start("replacement");
        cluster.Advance(10);
        follower.Storage.Unavailable = true; // Only the replacement can take over.
        await old.Cancellation.CancelAsync();
        cluster.Advance(150);
        Assert.Equal(ReplicationNodeRole.Leader, replacement.Coordinator.Role);
        Assert.Equal(0, replacement.Applied);
        cluster.Isolated.Clear();
        follower.Storage.Unavailable = false;
        cluster.Advance(30);
        Assert.Equal(5ul, Assert.Single(follower.Status.Current.Databases).Compared?.EventId);
        Assert.Equal(follower.Capture.Completed, follower.Capture.Acknowledged);
        Assert.Equal(0, follower.Restarts);
    }

    static ulong LocalEvent(Host node)
    {
        if (node.Db == null) return 0;
        using var read = node.Db.StartReadOnlyTransaction();
        return read.GetCommitUlong();
    }

    // Applies up to count consecutive inputs; a node that is restoring or stopped applies nothing.
    static async Task Apply(Host node, int count)
    {
        if (node.Db == null || node.Run.IsCompleted) return;
        for (var i = 0; i < count; i++)
        {
            var next = LocalEvent(node) + 1;
            if (next > LastEvent) return;
            await node.Write(next, EventValue(next));
        }
    }

    static async Task<ulong> PublishedEvent(Cluster cluster)
    {
        try { return await cluster.RestoreEvent(); }
        catch (System.IO.IOException) { return 0; } // A racing append requires a fresh discovery.
    }

    static async Task AssertRestoresEveryEvent(Cluster cluster)
    {
        using var files = new InMemoryReplicationFileStorage();
        var inventory = await CanonicalTrlInventory.DiscoverAsync(cluster.Trls, new(Cluster.Genesis(1), 1));
        await using var collection = new ReplicationFileSet(files, inventory);
        await collection.InitializeAsync();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = collection, Compression = new NoCompressionStrategy(), CompactorScheduler = null
        });
        using var read = db.StartReadOnlyTransaction();
        using var cursor = read.CreateCursor();
        var buffer = Span<byte>.Empty;
        for (ulong id = 2; id <= LastEvent; id++)
        {
            Assert.True(cursor.FindExactKey([(byte)id]), $"event {id} missing");
            Assert.Equal(EventValue(id), cursor.GetValueSpan(ref buffer)[0]);
        }
    }
}
