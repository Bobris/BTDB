using System;
using System.Collections.Generic;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class StorageFaultTest
{
    [Fact]
    public void TimeoutAndOldReadDoNotPreventLaterRemoteEffect()
    {
        var scheduler = new DeterministicScheduler(1);
        var caller = scheduler.CreateScope("caller");
        var storage = new SimulatedBlobStore(scheduler);
        var results = new List<WriteResult>();
        storage.Dispatch(caller, "a", "tail", MutationKind.Create, null, [1, 2],
            new(TimeSpan.FromTicks(10), TimeSpan.Zero, TimeSpan.FromTicks(2)), results.Add);
        scheduler.AdvanceBy(TimeSpan.FromTicks(2));
        Assert.Equal(WriteOutcome.Ambiguous, Assert.Single(results).Outcome);
        Assert.Null(storage.Read("tail"));
        caller.Dispose();
        scheduler.RunUntilIdle();
        Assert.Equal(new byte[] { 1, 2 }, storage.Read("tail")!.Content);
        Assert.Single(results);
    }

    [Fact]
    public void LostSuccessAndCompetingCasHaveDifferentOutcomes()
    {
        var scheduler = new DeterministicScheduler(2);
        var caller = scheduler.CreateScope("caller");
        var storage = new SimulatedBlobStore(scheduler);
        storage.Dispatch(caller, "a", "tail", MutationKind.Create, null, [1],
            new(TimeSpan.Zero, TimeSpan.Zero), _ => { });
        scheduler.RunUntilIdle();
        var version = storage.Read("tail")!.Version;
        var results = new List<WriteResult>();
        storage.Dispatch(caller, "a", "tail", MutationKind.Append, version, [2],
            new(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.FromTicks(3), true), results.Add);
        storage.Dispatch(caller, "b", "tail", MutationKind.Append, version, [9],
            new(TimeSpan.FromTicks(1), TimeSpan.Zero), results.Add);
        scheduler.RunUntilIdle();
        Assert.Equal(WriteOutcome.Rejected, results[0].Outcome);
        Assert.Equal(WriteOutcome.Ambiguous, results[1].Outcome);
        Assert.Equal(new byte[] { 1, 2 }, storage.Read("tail")!.Content);
    }

    [Fact]
    public void RequestAndReadBuffersAreIsolatedAndTokensNeverRepeatAfterDelete()
    {
        var scheduler = new DeterministicScheduler(3);
        var caller = scheduler.CreateScope("caller");
        var storage = new SimulatedBlobStore(scheduler);
        var bytes = new byte[] { 1 };
        var immediate = new WriteFaults(TimeSpan.Zero, TimeSpan.Zero);
        storage.Dispatch(caller, "a", "file", MutationKind.Create, null, bytes, immediate, _ => { });
        bytes[0] = 9;
        scheduler.RunUntilIdle();
        var old = storage.Read("file")!;
        old.Content[0] = 8;
        Assert.Equal(1, storage.Read("file")!.Content[0]);
        storage.Dispatch(caller, "a", "file", MutationKind.Delete, old.Version, [], immediate, _ => { });
        scheduler.RunUntilIdle();
        storage.Dispatch(caller, "a", "file", MutationKind.Create, null, [2], immediate, _ => { });
        scheduler.RunUntilIdle();
        Assert.NotEqual(old.Version, storage.Read("file")!.Version);
    }

    [Fact]
    public void PeerConnectionsHaveIndependentPartitionsBoundedQueuesAndStaleDeliveryRejection()
    {
        var scheduler = new DeterministicScheduler(4);
        var first = new SimulatedPeerLink(scheduler.CreateScope("first"), 1);
        var second = new SimulatedPeerLink(scheduler.CreateScope("second"), 1);
        var received = new List<byte>();
        Assert.True(first.TrySend([1], TimeSpan.FromTicks(5), bytes => received.Add(bytes.Span[0])));
        Assert.False(first.TrySend([2], TimeSpan.Zero, _ => { }));
        first.Disconnect();
        first.Reconnect();
        Assert.True(first.TrySend([4], TimeSpan.FromTicks(6), bytes => received.Add(bytes.Span[0])));
        Assert.True(second.TrySend([3], TimeSpan.Zero, bytes => received.Add(bytes.Span[0])));
        scheduler.AdvanceBy(TimeSpan.FromTicks(5));
        Assert.Equal(new byte[] { 3 }, received);
        Assert.False(first.TrySend([9], TimeSpan.Zero, _ => { }));
        scheduler.RunUntilIdle();
        Assert.Equal(new byte[] { 3, 4 }, received);
    }
}
