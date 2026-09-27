using System.IO;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class ReplicationPeerPollTest
{
    static readonly ReplicationPeerPollRequest[] Requested = [new("a"), new("b")];

    static ReplicationPeerPoll Poll(long challenge, params ReplicationPeerDatabaseProgress[] databases) =>
        new(challenge, true, databases);

    static ReplicationPeerDatabaseProgress Db(string name, LeaderTrlProgress? progress = null,
        TransactionLogPosition? published = null) => new(name, progress, published);

    static ReplicationPeerTrlChunk C(uint fileId, uint offset, int length) => new(fileId, offset, new byte[length]);

    [Fact]
    public void MatchingBatchAndEmptyHeartbeatAreAccepted()
    {
        Poll(5, Db("a", new(1, 1, 10), new(1, 10)), Db("b")).Validate(5, Requested, 0);
        Poll(6).Validate(6, [], 0);
    }

    [Fact]
    public void StaleIncompleteReorderedOrInvalidAnswersAreRejected()
    {
        Assert.Throws<IOException>(() => Poll(4, Db("a"), Db("b")).Validate(5, Requested, 0));
        Assert.Throws<IOException>(() => Poll(5, Db("a")).Validate(5, Requested, 0));
        Assert.Throws<IOException>(() => Poll(5, Db("b"), Db("a")).Validate(5, Requested, 0));
        Assert.Throws<IOException>(() => Poll(5, Db("a", new(1, 0, 10)), Db("b")).Validate(5, Requested, 0));
        Assert.Throws<IOException>(() => Poll(5, Db("a", null, new(0, 10)), Db("b")).Validate(5, Requested, 0));
        Assert.Throws<IOException>(() => new ReplicationPeerPoll(5, true, null!).Validate(5, Requested, 0));
    }

    [Fact]
    public void InlineBytesMustContinueTheRequestedPositionWithinBudgetAndProgress()
    {
        ReplicationPeerPollRequest[] requested = [new("a", new(3, 100))];
        var progress = new LeaderTrlProgress(1, 5, 50);
        ReplicationPeerPoll Chunks(params ReplicationPeerTrlChunk[] chunks) => Poll(1, Db("a", progress) with { Chunks = chunks });
        // Rest of file 3, a later file from its start, then the end file through the progress cut.
        Chunks(C(3, 100, 10), C(4, 0, 20), C(5, 0, 50)).Validate(1, requested, 80);
        Assert.Throws<IOException>(() => Chunks(C(3, 100, 10)).Validate(1, requested, 9));
        Assert.Throws<IOException>(() => Chunks(C(3, 99, 10)).Validate(1, requested, 100));
        Assert.Throws<IOException>(() => Chunks(C(3, 100, 10), C(3, 111, 1)).Validate(1, requested, 100));
        Assert.Throws<IOException>(() => Chunks(C(3, 100, 10), C(4, 5, 1)).Validate(1, requested, 100));
        Assert.Throws<IOException>(() => Chunks(C(5, 0, 51)).Validate(1, requested, 100));
        Assert.Throws<IOException>(() => Chunks(C(6, 0, 1)).Validate(1, requested, 100));
        Assert.Throws<IOException>(() => Chunks(C(3, 100, 0)).Validate(1, requested, 100));
        Assert.Throws<IOException>(() => Poll(1, Db("a") with { Chunks = [C(3, 100, 1)] }).Validate(1, requested, 100));
        Assert.Throws<IOException>(() => Poll(1, Db("a", progress) with { Chunks = [C(3, 100, 1)] })
            .Validate(1, [new("a")], 100));
    }
}
