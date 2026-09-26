using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class ReplicationApplicationDataTest
{
    sealed class FaultStorage(LeaderSelectionTest.Storage inner) : ILeaderRecordStorage
    {
        public bool DelayWrite, LoseReply, FailRead;
        public Action? AfterWrite;
        public int Writes;
        public readonly Queue<Func<ValueTask<LeaderWriteOutcome>>> Delayed = new();
        public ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            if (FailRead) throw new IOException("Unavailable.");
            return inner.ReadAsync(cancellation);
        }
        public async ValueTask<LeaderWriteOutcome> WriteAsync(string handle, string token, string json, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            Writes++;
            if (DelayWrite)
            {
                Delayed.Enqueue(() => inner.WriteAsync(handle, token, json, default));
                return LeaderWriteOutcome.Ambiguous;
            }
            var result = await inner.WriteAsync(handle, token, json, cancellation);
            AfterWrite?.Invoke();
            cancellation.ThrowIfCancellationRequested();
            return LoseReply ? LeaderWriteOutcome.Ambiguous : result;
        }
    }

    sealed class Fixture
    {
        public readonly LeaderSelectionTest.Storage Storage = new();
        public FaultStorage Faults = null!;
        public LeaseSessionController Leases = null!;
        public SelectedLeadership? Selected;
        public ReplicationApplicationData Data = null!;
        public static async Task<Fixture> Create()
        {
            var result = new Fixture();
            var clock = new DeterministicScheduler(930);
            result.Leases = new(result.Storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
            result.Selected = await new LeaderSelection(result.Storage, result.Leases,
                (await result.Leases.MaintainAsync())!, LeaderSelectionTest.Candidate()).SelectAsync();
            result.Faults = new(result.Storage);
            result.Data = new(result.Faults, "cluster", result.Leases, () => result.Selected);
            return result;
        }
    }

    [Fact]
    public async Task ApplicationDataChangesOnlyItsOwnFieldAndRevisionAndNeverExposesPeerCredentials()
    {
        var fixture = await Fixture.Create();
        var before = JsonNode.Parse(fixture.Storage.Record.Json)!.AsObject();
        var snapshot = await fixture.Data.ReadAsync();
        var value = new JsonObject { ["term"] = 999, ["arbitrary"] = new JsonArray(1, 2, 3) };
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(snapshot, value));
        var after = JsonNode.Parse(fixture.Storage.Record.Json)!.AsObject();
        Assert.Equal(before["revision"]!.GetValue<ulong>() + 1, after["revision"]!.GetValue<ulong>());
        Assert.True(JsonNode.DeepEquals(value, after["applicationData"]));
        after.Remove("applicationData");
        before.Remove("applicationData");
        after.Remove("revision");
        before.Remove("revision");
        Assert.True(JsonNode.DeepEquals(before, after));
        var read = await fixture.Data.ReadAsync();
        Assert.DoesNotContain("fresh-api-key", JsonSerializer.Serialize(read));
        Assert.DoesNotContain("arbitrary", read.ToString());
        read.Value!["arbitrary"] = "edited";
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(read, read.Value));
        Assert.Equal("edited", (await fixture.Data.ReadAsync()).Value!["arbitrary"]!.GetValue<string>());
        Assert.True(JsonNode.DeepEquals(value["arbitrary"], new JsonArray(1, 2, 3)));
    }

    [Fact]
    public async Task SameSnapshotCannotOverwriteAnotherWriterAndNullClearsValue()
    {
        var fixture = await Fixture.Create();
        var first = await fixture.Data.ReadAsync();
        var second = await fixture.Data.ReadAsync();
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(first, JsonValue.Create("first")));
        Assert.Equal(LeaderWriteOutcome.Rejected, await fixture.Data.TryWriteAsync(second, JsonValue.Create("second")));
        Assert.Equal("first", (await fixture.Data.ReadAsync()).Value!.GetValue<string>());
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(await fixture.Data.ReadAsync(), null));
        Assert.Null((await fixture.Data.ReadAsync()).Value);
    }

    [Fact]
    public async Task LostReplyReconcilesAndRetryOfIdenticalIntentDoesNotIncrementRevisionAgain()
    {
        var fixture = await Fixture.Create();
        var snapshot = await fixture.Data.ReadAsync();
        fixture.Faults.LoseReply = true;
        var value = new JsonObject { ["custom"] = true };
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(snapshot, value));
        var version = (await fixture.Data.ReadAsync()).Version;
        fixture.Faults.LoseReply = false;
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(snapshot, value));
        Assert.Equal(version, (await fixture.Data.ReadAsync()).Version);
    }

    [Fact]
    public async Task UnchangedReadIsStillAmbiguousAndDelayedWriteCannotOverwriteANewerVersion()
    {
        var fixture = await Fixture.Create();
        var snapshot = await fixture.Data.ReadAsync();
        fixture.Faults.DelayWrite = true;
        Assert.Equal(LeaderWriteOutcome.Ambiguous, await fixture.Data.TryWriteAsync(snapshot, JsonValue.Create("delayed")));
        Assert.Equal(snapshot.Version, (await fixture.Data.ReadAsync()).Version);
        fixture.Faults.DelayWrite = false;
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(snapshot, JsonValue.Create("winner")));
        Assert.Equal(LeaderWriteOutcome.Rejected, await fixture.Faults.Delayed.Dequeue()());
        Assert.Equal("winner", (await fixture.Data.ReadAsync()).Value!.GetValue<string>());
    }

    [Fact]
    public async Task FailedReconciliationReadDoesNotTurnAnUnknownEffectIntoARejection()
    {
        var fixture = await Fixture.Create();
        var snapshot = await fixture.Data.ReadAsync();
        fixture.Faults.LoseReply = true;
        fixture.Faults.AfterWrite = () => fixture.Faults.FailRead = true;
        Assert.Equal(LeaderWriteOutcome.Ambiguous, await fixture.Data.TryWriteAsync(snapshot, JsonValue.Create(42)));
        fixture.Faults.FailRead = false;
        Assert.Equal(42, (await fixture.Data.ReadAsync()).Value!.GetValue<int>());
    }

    [Fact]
    public async Task CancellationAfterEffectDoesNotEraseTheValueAndSameIntentCanReconcile()
    {
        var fixture = await Fixture.Create();
        var snapshot = await fixture.Data.ReadAsync();
        using var cancellation = new CancellationTokenSource();
        fixture.Faults.AfterWrite = cancellation.Cancel;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            fixture.Data.TryWriteAsync(snapshot, JsonValue.Create(42), cancellation.Token).AsTask());
        fixture.Faults.AfterWrite = null;
        Assert.Equal(LeaderWriteOutcome.Applied, await fixture.Data.TryWriteAsync(snapshot, JsonValue.Create(42)));
    }

    [Fact]
    public async Task FollowerAndFencedLeaderCanReadButCannotDispatchWrites()
    {
        var fixture = await Fixture.Create();
        var leader = fixture.Selected;
        fixture.Selected = null;
        var snapshot = await fixture.Data.ReadAsync();
        Assert.Equal(LeaderWriteOutcome.Rejected, await fixture.Data.TryWriteAsync(snapshot, JsonValue.Create(1)));
        fixture.Selected = leader;
        fixture.Leases.Close();
        Assert.NotNull(await fixture.Data.ReadAsync());
        Assert.Equal(LeaderWriteOutcome.Rejected, await fixture.Data.TryWriteAsync(snapshot, JsonValue.Create(1)));
        Assert.Equal(0, fixture.Faults.Writes);
    }

    [Fact]
    public async Task NewSelectionPreservesDataAndRejectsSnapshotsFromThePredecessorTerm()
    {
        var fixture = await Fixture.Create();
        Assert.Equal(LeaderWriteOutcome.Applied,
            await fixture.Data.TryWriteAsync(await fixture.Data.ReadAsync(), JsonValue.Create("retained")));
        var previous = await fixture.Data.ReadAsync();
        fixture.Selected = await new LeaderSelection(fixture.Storage, fixture.Leases, fixture.Leases.Current!,
            LeaderSelectionTest.Candidate() with { SessionId = "replacement" }).SelectAsync();
        Assert.Equal("retained", (await fixture.Data.ReadAsync()).Value!.GetValue<string>());
        var count = fixture.Faults.Writes;
        Assert.Equal(LeaderWriteOutcome.Rejected, await fixture.Data.TryWriteAsync(previous, JsonValue.Create("stale")));
        Assert.Equal(count, fixture.Faults.Writes);
    }

    [Fact]
    public async Task SnapshotsCannotBeUsedWithADifferentStorageBinding()
    {
        var first = await Fixture.Create();
        var second = await Fixture.Create();
        var snapshot = await first.Data.ReadAsync();
        await Assert.ThrowsAsync<ArgumentException>(() => second.Data.TryWriteAsync(snapshot, null).AsTask());
        Assert.Equal(0, second.Faults.Writes);
    }
}
