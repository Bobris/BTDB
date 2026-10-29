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

    public class GuardedBase
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        public string Note { get; set; } = "";

        [OnBeforeRemove]
        protected bool KeepOdd() => Id % 2 == 1;

        [OnBeforeRemove]
        bool KeepThree() => Id == 4;

        [OnSerialize]
        void Normalize() => Note = Note.Trim();
    }

    public class Guarded : GuardedBase
    {
    }

    public interface IGuardedTable : IRelation<Guarded>
    {
        Guarded? FindByIdOrDefault(ulong companyId, ulong id);
        int RemoveById(ulong companyId);
    }

    // Before the fix generated relations called non-public OnBeforeRemove and OnSerialize methods of a base class
    // through an accessor of the derived class, which threw MissingMethodException.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void HookMethodsOfBaseClassAreCalled(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IGuardedTable>();
            for (ulong i = 1; i <= 6; i++) table.Upsert(new() { CompanyId = 1, Id = i, Note = " n " });
            Assert.Equal("n", table.FindByIdOrDefault(1, 2)!.Note);
            Assert.Equal(2, table.RemoveById(1));
            Assert.Equal(new ulong[] { 1, 3, 4, 5 }, table.Select(r => r.Id));
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    public class CountedBase
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [NotStored] public int Serialized { get; set; }

        [OnSerialize]
        protected virtual void Count() => Serialized += 100;
    }

    public class Counted : CountedBase
    {
        protected override void Count() => Serialized++;
    }

    public interface ICountedTable : IRelation<Counted>
    {
    }

    // An overridden hook method runs once, as its override.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OverriddenHookMethodRunsOnce(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            using var tr = _db.StartTransaction();
            var item = new Counted { Id = 1 };
            tr.GetRelation<ICountedTable>().Upsert(item);
            Assert.Equal(1, item.Serialized);
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    public class GenericGuardedBase<TNote>
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public TNote Note { get; set; } = default!;
        [NotStored] public int Serialized { get; set; }

        [OnBeforeRemove]
        bool KeepOne() => Id == 1;

        [OnSerialize]
        protected void Count() => Serialized++;
    }

    public class GenericGuarded : GenericGuardedBase<string>
    {
    }

    public interface IGenericGuardedTable : IRelation<GenericGuarded>
    {
        int RemoveById(ulong id);
    }

    // Before the fix generated relations could not call non-public hook methods of a generic base class.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void HookMethodsOfGenericBaseClassAreCalled(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IGenericGuardedTable>();
            var item = new GenericGuarded { Id = 1, Note = "a" };
            table.Upsert(item);
            table.Upsert(new() { Id = 2, Note = "b" });
            Assert.Equal(1, item.Serialized);
            Assert.Equal(0, table.RemoveById(1));
            Assert.Equal(1, table.RemoveById(2));
            Assert.Equal(new ulong[] { 1 }, table.Select(r => r.Id));
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }
}
