using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;
using Storage = BTDB.Replication.Test.CanonicalTrlPublisherTest.Storage;

namespace BTDB.Replication.Test;

public class PublicationConflictTest
{
    sealed class Host(ActivationDatabase[] databases) : IReplicationNodeHost
    {
        public readonly TaskCompletionSource Restarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Restarts;
        public ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation) =>
            ValueTask.FromResult<IReadOnlyList<ActivationDatabase>>(databases);
        public LeaderCandidate CreateCandidate() => LeaderSelectionTest.Candidate() with { DatabaseNames = ["a", "b"] };
        public LeaderTrlProgress? GetProgress(string database)
        {
            var db = databases.Single(d => d.Name == database);
            var position = db.Capture.Completed;
            using var transaction = db.Database.StartReadOnlyTransaction();
            return new(transaction.GetCommitUlong(), position.FileId, position.Offset);
        }
        public void RequestRestart(string reason) { Restarts++; Restarted.TrySetResult(); }
        public void RequestFatalRestart(string reason) => Assert.Fail("Unexpected fatal restart.");
        public void ReportStatus(ReplicationNodeRole role) { }
        public void DatabaseRemoved(string database) => Assert.Fail("Unexpected database removal.");
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ConflictFencesAndRequestsRestartBeforeAnotherDatabaseFinishes(bool conflictFirst, bool ignoreCancellation)
    {
        using var a = await Node.Create();
        using var b = await Node.Create();
        var nodes = new[] { a, b };
        var stores = new[] { new Storage(), new Storage() };
        var clock = new DeterministicScheduler(913);
        var scope = clock.CreateScope("node");
        var seedAuthority = new LeaseAuthority(scope, 0, TimeSpan.Zero);
        Assert.True(seedAuthority.AcceptSuccess(seedAuthority.BeginRequest(), TimeSpan.FromSeconds(60)));
        var databases = new ActivationDatabase[2];
        for (var i = 0; i < 2; i++)
        {
            await nodes[i].Write(1, 1);
            using var publisher = new CanonicalTrlPublisher(nodes[i].Db, nodes[i].Capture, stores[i], seedAuthority, 1, id => $"{id}.trl");
            await publisher.PublishNextAsync();
            databases[i] = new(i == 0 ? "a" : "b", nodes[i].Db, nodes[i].Capture, stores[i],
                new("1.trl", 1), publisher.PublishedPosition, id => $"{id}.trl");
        }
        var records = new LeaderSelectionTest.Storage
        {
            Record = new("1", """{"format":1,"clusterId":"cluster","term":1,"applicationGeneration":1,"databaseNames":["a","b"]}""")
        };
        var leases = new LeaseSessionController(records, scope, 0, TimeSpan.Zero);
        var transport = new InProcessReplicationPeerTransport();
        var host = new Host(databases);
        var status = new ReplicationStatus();
        var options = new ReplicationNodeOptions("cluster", "in-process://node", TimeSpan.FromTicks(10),
            TimeSpan.FromTicks(20), TimeSpan.FromTicks(15), TimeSpan.FromTicks(10), 1);
        var coordinator = new ReplicationNodeCoordinator(options, host, records, leases, transport, scope, status);
        using var cancellation = new CancellationTokenSource();
        var run = coordinator.RunAsync(cancellation.Token);
        var held = new TaskCompletionSource<TrlWriteResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            Assert.Equal(ReplicationNodeRole.Leader, coordinator.Role);
            var authority = Assert.IsType<LeaseAuthority>(leases.Current);
            var identity = new ReplicationPeerIdentity("cluster", 2, "fresh-session", "in-process://node", "fresh-api-key");
            using var peer = await transport.ConnectAsync(identity, default);
            var conflicting = stores[conflictFirst ? 0 : 1];
            var blocked = stores[conflictFirst ? 1 : 0];
            CancellationToken heldToken = default;
            blocked.OverrideWrite = async (_, token) =>
            {
                heldToken = token;
                entered.TrySetResult();
                return ignoreCancellation ? await held.Task : await held.Task.WaitAsync(token);
            };
            conflicting.OverrideWrite = async (write, token) =>
            {
                await entered.Task;
                conflicting.OverrideWrite = null;
                return await conflicting.WriteAsync(write, token);
            };
            conflicting.BeforeEffect = write =>
            {
                if (write.ExpectedToken != null) return;
                var bytes = new byte[write.Length];
                write.Source.RandomRead(bytes, 0, false);
                bytes[^1] ^= 1;
                conflicting.Blobs[write.Key] = new(new("conflict", write.Length), bytes);
                conflicting.BeforeEffect = null;
            };
            for (ulong id = 2; id <= 6; id++)
                foreach (var node in nodes) await node.Write(id, (byte)id);
            clock.AdvanceBy(TimeSpan.FromTicks(10));
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await host.Restarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(held.Task.IsCompleted);
            Assert.False(authority.IsValid);
            Assert.Null(leases.Current);
            Assert.False(status.Current.Ready);
            Assert.Equal(1, host.Restarts);
            await Assert.ThrowsAsync<IOException>(() => peer.PollAsync([], 1, TimeSpan.FromTicks(1), 0, default).AsTask());
            // Cancellation is requested even if the provider ignores it. Cleanup may still need to drain that call.
            held.TrySetResult(new(TrlWriteOutcome.Ambiguous));
            await run.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(heldToken.IsCancellationRequested);
            Assert.Equal(ReplicationNodeRole.RestartRequired, coordinator.Role);
        }
        finally
        {
            cancellation.Cancel();
            held.TrySetResult(new(TrlWriteOutcome.Ambiguous));
            await run.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
        }
    }
}
