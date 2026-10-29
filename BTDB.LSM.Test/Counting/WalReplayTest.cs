using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using Xunit;

namespace BTDB.LSM.Test.Counting;

public class WalReplayTest
{
    static byte[] Record(System.Action<WalRecordBuilder> log)
    {
        var builder = new WalRecordBuilder();
        log(builder);
        return builder.Finish(new(1, 0, []));
    }

    [Fact]
    public void ReplayAppliesInsertsAndUpdates()
    {
        var memtable = Memtable.CreateEmpty().CloneForWriting(1);
        WalRecordBuilder.Replay(memtable, Record(b =>
        {
            b.LogInsert([1], true, false, [10]);
            b.LogUpdate([1], true, [11]);
        }));
        Assert.True(memtable.TryGetNode([1], out var isSet, out _));
        Assert.True(isSet);
        Assert.Equal(1, memtable.EntryCount);
    }

    // Before the fix replaying an insert of a key the memtable held added a second node with it.
    [Fact]
    public void ReplayRejectsInsertOfPresentKey()
    {
        var memtable = Memtable.CreateEmpty().CloneForWriting(1);
        var e = Assert.Throws<BTDBException>(() => WalRecordBuilder.Replay(memtable, Record(b =>
        {
            b.LogInsert([1], true, false, [10]);
            b.LogInsert([1], true, false, [11]);
        })));
        Assert.Contains("already in its memtable", e.Message);
    }

    [Fact]
    public void ReplayRejectsUpdateOfMissingKey()
    {
        var memtable = Memtable.CreateEmpty().CloneForWriting(1);
        var e = Assert.Throws<BTDBException>(() =>
            WalRecordBuilder.Replay(memtable, Record(b => b.LogUpdate([1], true, [11]))));
        Assert.Contains("missing from its memtable", e.Message);
    }
}
