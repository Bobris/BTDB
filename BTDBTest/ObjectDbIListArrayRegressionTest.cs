using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbIListArrayRegressionTest : ObjectDbTestBase
{
    public ObjectDbIListArrayRegressionTest(ITestOutputHelper output) : base(output)
    {
    }

    public class JourneyMapLikeRow
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public IList<ulong> Phases { get; set; } = null!;
    }

    public interface IJourneyMapLikeTable : IRelation<JourneyMapLikeRow>
    {
    }

    public class StateItemIndexesWithIList
    {
        [PrimaryKey(1)]
        public ulong CompanyId { get; set; }

        [PrimaryKey(2)]
        public uint StateGroupId { get; set; }

        [PrimaryKey(3)]
        public ulong ItemId { get; set; }

        [SecondaryKey("Indexes", IncludePrimaryKeyOrder = 2)]
        public IList<ulong> Indexes { get; set; } = null!;
    }

    public interface IStateItemIndexesWithIListTable : IRelation<StateItemIndexesWithIList>
    {
        IEnumerable<StateItemIndexesWithIList> ListByIndexes(ulong companyId, uint stateGroupId, List<ulong> indexes);
    }

    public class StateItemIndexesWithList
    {
        [PrimaryKey(1)]
        public ulong CompanyId { get; set; }

        [PrimaryKey(2)]
        public uint StateGroupId { get; set; }

        [PrimaryKey(3)]
        public ulong ItemId { get; set; }

        [SecondaryKey("Indexes", IncludePrimaryKeyOrder = 2)]
        public List<ulong> Indexes { get; set; } = null!;
    }

    public interface IStateItemIndexesWithListTable : IRelation<StateItemIndexesWithList>
    {
        IEnumerable<StateItemIndexesWithList> ListByIndexes(ulong companyId, uint stateGroupId, List<ulong> indexes);
    }

    public class StateItemIndexesWithListAndDescription
    {
        [PrimaryKey(1)]
        public ulong CompanyId { get; set; }

        [PrimaryKey(2)]
        public uint StateGroupId { get; set; }

        [PrimaryKey(3)]
        public ulong ItemId { get; set; }

        [SecondaryKey("Indexes", IncludePrimaryKeyOrder = 2)]
        public List<ulong> Indexes { get; set; } = null!;

        public string? Description { get; set; }
    }

    public interface IStateItemIndexesWithListAndDescriptionTable : IRelation<StateItemIndexesWithListAndDescription>
    {
        bool RemoveById(ulong companyId, uint stateGroupId, ulong itemId);
    }

    [Fact]
    public void UpsertOfIListPropertyBackedByArrayShouldWork()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IJourneyMapLikeTable>();

        table.Upsert(new JourneyMapLikeRow
        {
            Id = 123,
            Phases = new ulong[] { 4 }
        });

        Assert.Equal(new ulong[] { 4 }, table.Single().Phases);
    }

    [Fact]
    public void SecondaryKeyCreatedFromIListCanBeOpenedAsList()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IStateItemIndexesWithIListTable>("StateItemIndexes")(tr);
            table.Upsert(new StateItemIndexesWithIList
            {
                CompanyId = 42,
                StateGroupId = 7,
                ItemId = 100,
                Indexes = new List<ulong> { 5, 9 }
            });
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IStateItemIndexesWithListTable>("StateItemIndexes")(tr);
            var item = table.ListByIndexes(42, 7, new List<ulong> { 5, 9 }).Single();

            Assert.Equal(42ul, item.CompanyId);
            Assert.Equal(7u, item.StateGroupId);
            Assert.Equal(100ul, item.ItemId);
            Assert.Equal(new ulong[] { 5, 9 }, item.Indexes);
            tr.Commit();
        }
    }

    [Fact]
    public void RemoveByIdMigratesSecondaryKeyFromIListToListWithEmit()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        try
        {
            using (var tr = _db.StartTransaction())
            {
                var table = tr.InitRelation<IStateItemIndexesWithIListTable>("StateItemIndexes")(tr);
                table.Upsert(new StateItemIndexesWithIList
                {
                    CompanyId = 42,
                    StateGroupId = 7,
                    ItemId = 100,
                    Indexes = new List<ulong> { 5, 9 }
                });
                tr.Commit();
            }

            ReopenDb();

            using var tr2 = _db.StartTransaction();
            var migratedTable = tr2.InitRelation<IStateItemIndexesWithListAndDescriptionTable>("StateItemIndexes")(tr2);
            Assert.True(migratedTable.RemoveById(42, 7, 100));
            tr2.Commit();
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
        }
    }
}
