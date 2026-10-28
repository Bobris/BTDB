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

    public class CompilerListRow
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public IList<int> Values { get; set; } = null!;

        [SecondaryKey("Key")]
        public IList<ulong> Key { get; set; } = null!;
    }

    public interface ICompilerListRowTable : IRelation<CompilerListRow>
    {
        CompilerListRow FindById(ulong id);
        IEnumerable<CompilerListRow> ListByKey(IList<ulong> key);
    }

    // Before the fix the list a one-item collection expression makes for IReadOnlyList<T> failed to save.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IListPropertyBackedBySingleItemCollectionExpressionShouldWork(bool noEmit)
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = noEmit;
        try
        {
            using (var tr = _db.StartTransaction())
            {
                tr.GetRelation<ICompilerListRowTable>().Upsert(new CompilerListRow
                {
                    Id = 1, Values = (IList<int>)(IReadOnlyList<int>)[5], Key = (IList<ulong>)(IReadOnlyList<ulong>)[9]
                });
                tr.Commit();
            }

            ReopenDb();

            using var tr2 = _db.StartReadOnlyTransaction();
            var table = tr2.GetRelation<ICompilerListRowTable>();
            Assert.Equal([5], table.FindById(1).Values);
            Assert.Equal(1ul, table.ListByKey(new List<ulong> { 9 }).Single().Id);
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
        }
    }

    public class EmittedListKeyRow
    {
        [PrimaryKey(1)]
        public ulong Id { get; set; }

        [PrimaryKey(2)]
        public IList<ulong> Path { get; set; } = null!;

        [SecondaryKey("Key")]
        public IList<ulong> Key { get; set; } = null!;
    }

    public interface IEmittedListKeyRowTable : IRelation<EmittedListKeyRow>
    {
        IEnumerable<EmittedListKeyRow> ListByKey(IList<ulong> key);
        int CountByKey(List<ulong> key);
        bool AnyByKey(IList<ulong> key);
        IEnumerable<EmittedListKeyRow> ListById(ulong id, IList<ulong> path);
    }

    // Before the fix a method listing by a key prefix ending with a list failed with InvalidProgramException when the
    // relation was emitted, as the list saver opened a try block with call arguments on the evaluation stack. The
    // interface is used by no other test, so its relation is emitted here.
    [Fact]
    public void EmittedListingByListKeyPrefixWorks()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        try
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IEmittedListKeyRowTable>();
            table.Upsert(new EmittedListKeyRow { Id = 1, Path = new List<ulong> { 2, 3 }, Key = new List<ulong> { 9 } });
            Assert.Equal(1ul, table.ListByKey(new List<ulong> { 9 }).Single().Id);
            Assert.Equal(1, table.CountByKey(new List<ulong> { 9 }));
            Assert.False(table.AnyByKey(new List<ulong> { 8 }));
            Assert.Equal(1ul, table.ListById(1, new List<ulong> { 2, 3 }).Single().Id);
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
        }
    }

    public class ReadOnlyViewRow
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public IList<int> Values { get; set; } = null!;

        [SecondaryKey("Key")]
        public IList<ulong> Key { get; set; } = null!;
    }

    public interface IReadOnlyViewRowTable : IRelation<ReadOnlyViewRow>
    {
        ReadOnlyViewRow FindById(ulong id);
        IEnumerable<ReadOnlyViewRow> ListByKey(IList<ulong> key);
    }

    // Before the fix an IList<T> holding a ReadOnlyCollection<T> or an ImmutableList<T> failed to save.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IListPropertyBackedByReadOnlyOrImmutableListShouldWork(bool noEmit)
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = noEmit;
        try
        {
            using (var tr = _db.StartTransaction())
            {
                tr.GetRelation<IReadOnlyViewRowTable>().Upsert(new ReadOnlyViewRow
                {
                    Id = 1, Values = new List<int> { 1, 2 }.AsReadOnly(),
                    Key = System.Collections.Immutable.ImmutableList.Create(5ul, 6ul)
                });
                tr.Commit();
            }

            ReopenDb();

            using var tr2 = _db.StartReadOnlyTransaction();
            var table = tr2.GetRelation<IReadOnlyViewRowTable>();
            Assert.Equal([1, 2], table.FindById(1).Values);
            Assert.Equal(1ul, table.ListByKey(new List<ulong> { 5, 6 }).Single().Id);
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
        }
    }
}
