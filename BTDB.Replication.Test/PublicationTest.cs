using System;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class PublicationTest
{
    static readonly WriteFaults Immediate = new(TimeSpan.Zero, TimeSpan.Zero);
    static TrlSnapshot Read(SimulatedBlobStore storage, string key = "tail")
    {
        var blob = storage.Read(key)!;
        return new(blob.Version.ToString(), blob.Content, TrlMetadata.Decode(blob.Metadata!));
    }

    static void Send(SimulatedBlobStore storage, DeterministicScheduler.Scope caller, TrlPublication intent,
        WriteFaults? faults = null, Action<WriteResult>? response = null, string key = "tail") =>
        storage.Dispatch(caller, "publisher", key, intent.ExpectedToken is null ? MutationKind.Create : MutationKind.Replace,
            intent.ExpectedToken is null ? null : ulong.Parse(intent.ExpectedToken), intent.Content,
            faults ?? Immediate, response ?? (_ => { }), intent.Metadata.Encode());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AdoptionAndOldAppendHaveExactlyOneCasWinner(bool appendFirst)
    {
        var scheduler = new DeterministicScheduler(200);
        var caller = scheduler.CreateScope("publisher");
        var storage = new SimulatedBlobStore(scheduler);
        Send(storage, caller, TrlPublication.Genesis([1], 1));
        scheduler.RunUntilIdle();
        var before = Read(storage);
        var append = TrlPublication.Append(before, 1, [2]);
        var adopt = TrlPublication.Adopt(before, 2);
        Send(storage, caller, append, new(TimeSpan.FromTicks(appendFirst ? 0 : 1), TimeSpan.Zero));
        Send(storage, caller, adopt, new(TimeSpan.FromTicks(appendFirst ? 1 : 0), TimeSpan.Zero));
        scheduler.RunUntilIdle();
        var actual = Read(storage);
        if (appendFirst)
        {
            Assert.Equal(PublicationResolution.Changed, adopt.Reconcile(actual));
            Assert.Equal(new byte[] { 1, 2 }, actual.Content.ToArray());
            Send(storage, caller, TrlPublication.Adopt(actual, 2));
            scheduler.RunUntilIdle();
            Assert.Equal(new byte[] { 1, 2 }, Read(storage).Content.ToArray());
        }
        else
        {
            Assert.Equal(PublicationResolution.Changed, append.Reconcile(actual));
            Assert.Equal(new byte[] { 1 }, actual.Content.ToArray());
        }
        Assert.Equal(2ul, Read(storage).Metadata.Term);
        Assert.Throws<InvalidOperationException>(() => TrlPublication.Append(Read(storage), 1, [9]));
    }

    [Fact]
    public void TimeoutAndOldReadRemainPendingUntilTheRequestLands()
    {
        var scheduler = new DeterministicScheduler(201);
        var caller = scheduler.CreateScope("publisher");
        var storage = new SimulatedBlobStore(scheduler);
        var intent = TrlPublication.Genesis([1], 1);
        Send(storage, caller, intent, new(TimeSpan.FromTicks(10), TimeSpan.Zero, TimeSpan.FromTicks(1)),
            result => Assert.Equal(WriteOutcome.Ambiguous, result.Outcome));
        scheduler.AdvanceBy(TimeSpan.FromTicks(1));
        Assert.Equal(PublicationResolution.Pending, intent.Reconcile(null));
        caller.Dispose();
        scheduler.RunUntilIdle();
        Assert.Equal(PublicationResolution.ExactResultObserved, intent.Reconcile(Read(storage)));
    }

    [Fact]
    public void ChangedAfterLostSuccessDoesNotMeanTheAppendWasRejected()
    {
        var scheduler = new DeterministicScheduler(202);
        var caller = scheduler.CreateScope("publisher");
        var storage = new SimulatedBlobStore(scheduler);
        Send(storage, caller, TrlPublication.Genesis([1], 1));
        scheduler.RunUntilIdle();
        var append = TrlPublication.Append(Read(storage), 1, [2]);
        Send(storage, caller, append, new(TimeSpan.Zero, TimeSpan.Zero, LoseResponse: true));
        scheduler.RunUntilIdle();
        Send(storage, caller, TrlPublication.Adopt(Read(storage), 2));
        scheduler.RunUntilIdle();
        Assert.Equal(PublicationResolution.Changed, append.Reconcile(Read(storage)));
        Assert.Equal(new byte[] { 1, 2 }, Read(storage).Content.ToArray());
    }

    [Fact]
    public void PreparedSuccessorDoesNotSelectItselfAndLinkSealsPredecessor()
    {
        var scheduler = new DeterministicScheduler(203);
        var caller = scheduler.CreateScope("publisher");
        var storage = new SimulatedBlobStore(scheduler);
        Send(storage, caller, TrlPublication.Genesis([1], 1));
        Send(storage, caller, TrlPublication.Genesis([2], 1), key: "unique-successor");
        scheduler.RunUntilIdle();
        Assert.Null(Read(storage).Metadata.Next);
        var selected = new TrlSuccessor("unique-successor", 3);
        Send(storage, caller, TrlPublication.Append(Read(storage), 1, [], selected));
        scheduler.RunUntilIdle();
        Assert.Equal(selected, Read(storage).Metadata.Next);
        Assert.Throws<InvalidOperationException>(() => TrlPublication.Append(Read(storage), 1, [9]));
        Assert.Throws<InvalidOperationException>(() => TrlPublication.Adopt(Read(storage), 2));
    }

    [Fact]
    public void CompetingGenesisSelectsOneContentAndMetadataTogether()
    {
        var scheduler = new DeterministicScheduler(204);
        var caller = scheduler.CreateScope("publisher");
        var storage = new SimulatedBlobStore(scheduler);
        Send(storage, caller, TrlPublication.Genesis([1], 1));
        var losing = TrlPublication.Genesis([2], 2);
        Send(storage, caller, losing);
        scheduler.RunUntilIdle();
        Assert.Equal(1ul, Read(storage).Metadata.Term);
        Assert.Equal(new byte[] { 1 }, Read(storage).Content.ToArray());
        Assert.Equal(PublicationResolution.Changed, losing.Reconcile(Read(storage)));
    }

    [Fact]
    public void MetadataRejectsMissingAuthorityAndIncompleteContinuation()
    {
        Assert.Throws<FormatException>(() => TrlMetadata.Decode(new System.Collections.Generic.Dictionary<string, string>
            { ["unrelated"] = "1" }));
        Assert.Throws<FormatException>(() => TrlMetadata.Decode(new System.Collections.Generic.Dictionary<string, string>
            { ["btdb_term"] = "1", ["btdb_next"] = "next" }));
        var metadata = new TrlMetadata(3, new("branch/next", 5));
        Assert.Equal(metadata, TrlMetadata.Decode(metadata.Encode()));
    }
}
