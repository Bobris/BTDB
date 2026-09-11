using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class InMemoryInMemoryKeyValueDBTest : KeyValueDBTestBase
{
    public InMemoryInMemoryKeyValueDBTest(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }

    protected override IKeyValueDB NewKeyValueDB()
    {
        return new InMemoryKeyValueDB();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BatchHintPreservesCommitRollbackAndSnapshots(bool inBatch)
    {
        using IKeyValueDB db = new InMemoryKeyValueDB();
        using var oldReader = db.StartReadOnlyTransaction();
        using (var committed = await db.StartWritingTransaction(inBatch))
        {
            using var cursor = committed.CreateCursor();
            cursor.CreateOrUpdateKeyValue([1], [1]);
            committed.Commit();
        }
        using var reader = db.StartReadOnlyTransaction();
        Assert.Equal(1, reader.GetKeyValueCount());
        Assert.Equal(0, oldReader.GetKeyValueCount());
        using (var rolledBack = await db.StartWritingTransaction(inBatch))
        {
            using var cursor = rolledBack.CreateCursor();
            cursor.CreateOrUpdateKeyValue([2], [2]);
        }
        db.FinishTransactionBatchAfterCurrentTransaction();
        using var finalReader = db.StartReadOnlyTransaction();
        Assert.Equal(1, finalReader.GetKeyValueCount());
        Assert.Equal(1, reader.GetKeyValueCount());
    }

}
