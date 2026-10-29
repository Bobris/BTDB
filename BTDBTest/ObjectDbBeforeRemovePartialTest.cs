using System.Linq;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Before the fix RemoveByIdPartial counted the items OnBeforeRemove kept towards maxCount, so it removed fewer items
// than it could, and calling it until it removed nothing left removable items after kept ones.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbBeforeRemovePartialTest : ObjectDbTestBase
{
    public ObjectDbBeforeRemovePartialTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Room
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }

        [OnBeforeRemove]
        bool KeepOdd() => Id % 2 == 1;
    }

    public interface IRoomTable : IRelation<Room>
    {
        int RemoveByIdPartial(ulong companyId, int maxCount);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ItemsKeptByOnBeforeRemoveDoNotCountTowardsMaxCount(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRoomTable>();
            for (ulong i = 1; i <= 8; i++) table.Upsert(new() { CompanyId = 1, Id = i });
            Assert.Equal(3, table.RemoveByIdPartial(1, 3));
            Assert.Equal(new ulong[] { 1, 3, 5, 7, 8 }, table.Select(r => r.Id));
            while (table.RemoveByIdPartial(1, 3) > 0)
            {
            }

            Assert.Equal(new ulong[] { 1, 3, 5, 7 }, table.Select(r => r.Id));
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }
}
