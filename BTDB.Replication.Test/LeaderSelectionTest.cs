using System;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class LeaderSelectionTest
{
    internal sealed class Storage : IReplicationLeaseStorage, ILeaderRecordStorage
    {
        public LeaderRecord Record = new("1", """
            {"format":1,"clusterId":"cluster","term":7,"revision":9,"applicationGeneration":1,
             "databaseNames":["main"],"applicationData":{"custom":"retained"},"extension":42}
            """);
        public LeaderWriteOutcome Outcome = LeaderWriteOutcome.Applied;
        public bool Land = true;
        public int Writes;
        public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation) =>
            ValueTask.FromResult<LeaseGrant?>(new("lease", TimeSpan.FromSeconds(60)));
        public ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation) =>
            ValueTask.FromResult<TimeSpan?>(TimeSpan.FromSeconds(60));
        public ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation) => ValueTask.FromResult(Record);
        public ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string token, string json, CancellationToken cancellation)
        {
            Assert.Equal("lease", leaseHandle);
            Writes++;
            if (token != Record.Token) return ValueTask.FromResult(LeaderWriteOutcome.Rejected);
            if (Land) Record = new((int.Parse(token) + 1).ToString(), json);
            return ValueTask.FromResult(Outcome);
        }
    }
    internal static LeaderCandidate Candidate(ulong generation = 1) =>
        new("cluster", "node", "fresh-session", generation, ["main"], "in-process://node", "fresh-api-key");

    [Fact]
    public async Task LostSelectionReplyReconcilesExactJsonAndPreservesApplicationData()
    {
        var storage = new Storage { Outcome = LeaderWriteOutcome.Ambiguous };
        var clock = new DeterministicScheduler(710);
        var leases = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var selection = new LeaderSelection(storage, leases, (await leases.MaintainAsync())!, Candidate());
        var selected = await selection.SelectAsync();
        Assert.Equal(8ul, selected!.Term);
        var json = JsonNode.Parse(storage.Record.Json)!;
        Assert.Equal("retained", json["applicationData"]!["custom"]!.GetValue<string>());
        Assert.Equal(42, json["extension"]!.GetValue<int>());
        Assert.Equal(10ul, json["revision"]!.GetValue<ulong>());
        Assert.Equal(selected.Term, (await selection.SelectAsync())!.Term);
        Assert.Equal(1, storage.Writes);
    }

    [Fact]
    public async Task SameGenerationComparesDatabaseNamesAsASet()
    {
        var storage = new Storage();
        storage.Record = new("1", """{"format":1,"clusterId":"cluster","term":7,"applicationGeneration":1,"databaseNames":["b","a"]}""");
        var clock = new DeterministicScheduler(711);
        var leases = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var candidate = Candidate() with { DatabaseNames = ["a", "b"] };
        var selected = await new LeaderSelection(storage, leases, (await leases.MaintainAsync())!, candidate).SelectAsync();
        Assert.Equal(new[] { "a", "b" }, selected!.DatabaseNames);
        storage.Record = new("9", """{"format":1,"clusterId":"cluster","term":8,"applicationGeneration":1,"databaseNames":["a","c"]}""");
        var other = new LeaderSelection(storage, leases, leases.Current!, candidate with { SessionId = "another" });
        await Assert.ThrowsAsync<InvalidDataException>(() => other.SelectAsync().AsTask());
    }

    [Theory]
    [InlineData("""{"format":"1","clusterId":"cluster"}""")]
    [InlineData("""{"format":1,"clusterId":"cluster","term":"seven"}""")]
    [InlineData("""{"format":1,"clusterId":"cluster","applicationGeneration":-1}""")]
    [InlineData("""{"format":1,"clusterId":"cluster" """)]
    [InlineData("""[1]""")]
    public async Task MalformedLeaderRecordIsInvalidConfigurationAndIsNeverRewritten(string json)
    {
        var storage = new Storage { Record = new("1", json) };
        var clock = new DeterministicScheduler(711);
        var leases = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var selection = new LeaderSelection(storage, leases, (await leases.MaintainAsync())!, Candidate());
        await Assert.ThrowsAsync<InvalidDataException>(() => selection.SelectAsync().AsTask());
        Assert.Equal(0, storage.Writes);
    }

    [Fact]
    public async Task UnlandedAmbiguityRetriesSameTermAndGenerationFloorPreventsDowngrade()
    {
        var storage = new Storage { Outcome = LeaderWriteOutcome.Ambiguous, Land = false };
        var clock = new DeterministicScheduler(711);
        var leases = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var authority = (await leases.MaintainAsync())!;
        var selection = new LeaderSelection(storage, leases, authority, Candidate());
        Assert.Null(await selection.SelectAsync());
        storage.Land = true;
        Assert.Equal(8ul, (await selection.SelectAsync())!.Term);
        var stale = new LeaderSelection(storage, leases, authority, Candidate(0));
        Assert.Null(await stale.SelectAsync());
        Assert.True(authority.IsFenced);
    }
}
