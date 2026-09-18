using System;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class HistoryOracleTest
{
    static readonly WriteFaults Immediate = new(TimeSpan.Zero, TimeSpan.Zero);
    const string Tail = HistoryOracle.HistoryPrefix + "db";

    static (ClusterFixture Cluster, NodeFixture Node, ModelHistory History) Prepare()
    {
        var cluster = new ClusterFixture(100, _ => { });
        var node = cluster.AddNode("leader", "db");
        cluster.Storage.Dispatch(node.Scope, node.SessionId, HistoryOracle.LeaderKey, MutationKind.Create, null,
            HistoryOracle.Encode(new ModelAuthority(1, node.SessionId)), Immediate, _ => { });
        cluster.Storage.Dispatch(node.Scope, node.SessionId, "model/file/1", MutationKind.Create, null,
            [1, 2, 3], Immediate, _ => { });
        cluster.Scheduler.RunUntilIdle();
        var range = new ModelRange("model/file/1", 0, 3, HistoryOracle.Hash([1, 2, 3]));
        return (cluster, node, new(0, 1, [new(1, 1, true, true, [range])]));
    }

    static void Publish(ClusterFixture cluster, NodeFixture node, ModelHistory history, WriteFaults? faults = null)
    {
        var version = cluster.Storage.Read(Tail)?.Version;
        cluster.Storage.Dispatch(node.Scope, node.SessionId, Tail,
            version.HasValue ? MutationKind.Replace : MutationKind.Create, version,
            HistoryOracle.Encode(history), faults ?? Immediate, _ => { });
    }

    [Fact]
    public void CompletePublishedHistoryIsCheckedEvenWhenTheSuccessResponseIsLost()
    {
        var (cluster, node, history) = Prepare();
        using (cluster)
        {
            Publish(cluster, node, history, new(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.FromTicks(5), true));
            cluster.Scheduler.RunUntilIdle();
            Assert.NotNull(cluster.Storage.Read(Tail));
            Assert.True(cluster.Checks > 0);
        }
    }

    [Theory]
    [InlineData("incomplete")]
    [InlineData("sequence gap")]
    [InlineData("cursor")]
    [InlineData("schema cursor")]
    [InlineData("missing file")]
    [InlineData("hash")]
    [InlineData("range overflow")]
    public void OracleRejectsInvalidRecoveryObservations(string corruption)
    {
        var (cluster, node, history) = Prepare();
        using (cluster)
        {
            var transaction = history.Transactions[0];
            var range = transaction.Ranges[0];
            history = corruption switch
            {
                "incomplete" => history with { Transactions = [transaction with { Complete = false }] },
                "sequence gap" => history with { Transactions = [transaction with { Sequence = 2 }] },
                "cursor" => history with { PublishedCursor = 2 },
                "schema cursor" => history with { Transactions = [transaction with { Application = false }] },
                "missing file" => history with { Transactions = [transaction with { Ranges = [range with { Key = "missing" }] }] },
                "hash" => history with { Transactions = [transaction with { Ranges = [range with { Sha256 = "bad" }] }] },
                "range overflow" => history with { Transactions = [transaction with { Ranges = [range with { Offset = int.MaxValue }] }] },
                _ => throw new ArgumentOutOfRangeException(nameof(corruption))
            };
            Publish(cluster, node, history);
            Assert.Throws<SimulationFailureException>(() => cluster.Scheduler.RunUntilIdle());
        }
    }

    [Fact]
    public void OracleRejectsRewritingPublishedHistory()
    {
        var (cluster, node, history) = Prepare();
        using (cluster)
        {
            Publish(cluster, node, history);
            cluster.Scheduler.RunUntilIdle();
            Publish(cluster, node, new(0, 2, [history.Transactions[0] with { EventId = 2 }]));
            Assert.Contains("prefix changed", Assert.Throws<SimulationFailureException>(() => cluster.Scheduler.RunNext()).Message);
        }
    }

    [Fact]
    public void OracleRejectsDeletingAReachableFileImmediatelyAfterTheEffect()
    {
        var (cluster, node, history) = Prepare();
        using (cluster)
        {
            Publish(cluster, node, history);
            cluster.Scheduler.RunUntilIdle();
            cluster.Storage.Dispatch(node.Scope, node.SessionId, "model/file/1", MutationKind.Delete,
                cluster.Storage.Read("model/file/1")!.Version, [], Immediate, _ => { });
            Assert.Contains("recovery range", Assert.Throws<SimulationFailureException>(() => cluster.Scheduler.RunNext()).Message);
        }
    }

    [Fact]
    public void OracleRejectsUnselectedPublisherWithoutTrustingNodeRoleFlags()
    {
        var (cluster, _, history) = Prepare();
        using (cluster)
        {
            var follower = cluster.AddNode("follower", "db");
            Publish(cluster, follower, history);
            Assert.Contains("selected authority", Assert.Throws<SimulationFailureException>(() => cluster.Scheduler.RunNext()).Message);
        }
    }

    [Fact]
    public void SelectionAloneDoesNotFenceAnAlreadyDispatchedPredecessorWrite()
    {
        var (cluster, node, history) = Prepare();
        using (cluster)
        {
            Publish(cluster, node, history, new(TimeSpan.FromTicks(10), TimeSpan.Zero));
            var next = cluster.AddNode("next", "db");
            cluster.Storage.Dispatch(next.Scope, next.SessionId, HistoryOracle.LeaderKey, MutationKind.Replace,
                cluster.Storage.Read(HistoryOracle.LeaderKey)!.Version,
                HistoryOracle.Encode(new ModelAuthority(2, next.SessionId)), Immediate, _ => { });
            node.Dispose();
            cluster.Scheduler.RunUntilIdle();
            Assert.NotNull(cluster.Storage.Read(Tail));
        }
    }
}
