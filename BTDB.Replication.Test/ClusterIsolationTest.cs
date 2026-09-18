using System;
using System.Linq;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class ClusterIsolationTest
{
    [Fact]
    public void ThreeNativeNodesHaveIndependentDatabasesCursorsAndReaderVisibility()
    {
        using var cluster = new ClusterFixture(57);
        var a = cluster.AddNode("a", "one", "two");
        var b = cluster.AddNode("b", "one", "two");
        var c = cluster.AddNode("c", "one", "two");
        using var oldReader = b.Database("one").StartReadOnlyTransaction();
        Assert.Equal(3, new[] { a.SessionId, b.SessionId, c.SessionId }.Distinct().Count());
        Assert.NotSame(a.Database("one").FileCollection, b.Database("one").FileCollection);
        var first = cluster.AddInput(1, Enumerable.Repeat((byte)7, 1500).ToArray());
        var second = cluster.AddInput(2, [8]);
        var failed = cluster.AddInput(1, [99]);
        a.Scope.Schedule(TimeSpan.Zero, () => a.Apply("one", first), "event 1");
        b.Scope.Schedule(TimeSpan.Zero, () => b.Apply("one", first, inBatch: true), "batched event 1");
        b.Scope.Schedule(TimeSpan.FromTicks(1), () => b.Apply("one", failed, rollback: true, inBatch: true), "rollback");
        c.Scope.Schedule(TimeSpan.FromTicks(5), () => c.Apply("two", second), "event 2 other database");
        cluster.Scheduler.AdvanceBy(TimeSpan.Zero);
        Assert.Equal(0ul, oldReader.GetCommitUlong());
        cluster.Scheduler.RunNext(); // Rollback publishes the successful prefix, never the failed mutation.
        b.Scope.Schedule(TimeSpan.Zero, () => b.PublishBatch("one"), "publish memory batch");
        cluster.Scheduler.RunUntilIdle();
        using (var read = b.Database("one").StartReadOnlyTransaction()) Assert.Equal(1ul, read.GetCommitUlong());
        Assert.Equal(0ul, oldReader.GetCommitUlong());
        using (var read = c.Database("one").StartReadOnlyTransaction()) Assert.Equal(0ul, read.GetCommitUlong());
        using (var read = c.Database("two").StartReadOnlyTransaction()) Assert.Equal(2ul, read.GetCommitUlong());
        Assert.True(cluster.Checks >= 5);
        Assert.Empty(cluster.Storage.Journal);
    }

    [Fact]
    public void OracleDetectsRealDatabaseMutationOutsideTheApplicationHistory()
    {
        using var cluster = new ClusterFixture(58, _ => { });
        var node = cluster.AddNode("node", "one");
        node.Scope.Schedule(TimeSpan.Zero, () =>
        {
            using var transaction = node.Database("one").StartWritingTransaction(99ul).Result;
            transaction.Commit();
        }, "injected bad local cursor");
        Assert.Contains("local cursor", Assert.Throws<SimulationFailureException>(() => cluster.Scheduler.RunNext()).Message);
    }

    [Theory]
    [InlineData(1ul)]
    [InlineData(17ul)]
    [InlineData(987654321ul)]
    public void SeedReplaysTheSameMultiNodeApplicationSchedule(ulong seed)
    {
        static string Run(ulong seed)
        {
            using var cluster = new ClusterFixture(seed);
            var nodes = new[] { cluster.AddNode("a", "db"), cluster.AddNode("b", "db"), cluster.AddNode("c", "db") };
            var inputs = Enumerable.Range(1, 10).Select(i => cluster.AddInput((byte)i, [(byte)i])).ToArray();
            foreach (var node in nodes)
            {
                var delay = TimeSpan.Zero;
                foreach (var input in inputs)
                {
                    delay += node.Random.Backoff(TimeSpan.FromTicks(20));
                    node.Scope.Schedule(delay, () => node.Apply("db", input), $"apply {input.EventId}");
                }
            }
            cluster.Scheduler.RunUntilIdle();
            Assert.Equal(30, cluster.Checks);
            return cluster.Scheduler.Trace + "\n" + string.Join('\n', nodes.Select(n => n.NativeFileHash("db")));
        }
        Assert.Equal(Run(seed), Run(seed));
    }
}
