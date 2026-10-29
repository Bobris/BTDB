using System.Linq;
using System;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbMemberShapesTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [Generate]
    public class Modern
    {
        [PrimaryKey] public ulong Id { get; init; }
        public required string Name { get; init; }
        public int Count { get; set; }
        public string? Note { get; private set; }

        public void SetNote(string note) => Note = note;
    }

    public interface IModerns : IRelation<Modern>
    {
        void Insert(Modern value);
        Modern FindById(ulong id);
    }

    [Generate]
    public record RecordRow([property: PrimaryKey] ulong Id, string Title)
    {
        public int Extra { get; set; }
    }

    public interface IRecordRows : IRelation<RecordRow>
    {
        void Insert(RecordRow value);
        RecordRow FindById(ulong id);
    }

    [Fact]
    public void InitRequiredAndPrivateSettersRoundTrip()
    {
        using (var tr = _db.StartTransaction())
        {
            var m = new Modern { Id = 1, Name = "n", Count = 3 };
            m.SetNote("note");
            tr.GetRelation<IModerns>().Insert(m);
            tr.Commit();
        }

        ReopenDb();
        using var read = _db.StartReadOnlyTransaction();
        var loaded = read.GetRelation<IModerns>().FindById(1);
        Assert.Equal("n", loaded.Name);
        Assert.Equal(3, loaded.Count);
        Assert.Equal("note", loaded.Note);
    }

    [Fact]
    public void RecordWithPrimaryConstructorRoundTrips()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IRecordRows>().Insert(new RecordRow(1, "t") { Extra = 5 });
            tr.Commit();
        }

        ReopenDb();
        using var read = _db.StartReadOnlyTransaction();
        var loaded = read.GetRelation<IRecordRows>().FindById(1);
        Assert.Equal("t", loaded.Title);
        Assert.Equal(5, loaded.Extra);
    }

    public class HidingBase
    {
        [PrimaryKey] public ulong Id { get; set; }
        public int X { get; set; }
    }

    [Generate]
    public class Hiding : HidingBase
    {
        public new string? X { get; set; }
    }

    public interface IHidings : IRelation<Hiding>
    {
        void Insert(Hiding value);
        Hiding FindById(ulong id);
    }

    // Before the fix reflection listed a property hidden with `new` as a field of its own, beside the derived one: the
    // relation failed to build as its metadata differed, or in emit mode two fields of one name were loaded wrong.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void HiddenBasePropertyIsNotStored(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, StoreAndLoadHiding);
    }

    void StoreAndLoadHiding()
    {
        using (var tr = _db.StartTransaction())
        {
            var h = new Hiding { Id = 1, X = "derived" };
            ((HidingBase)h).X = 7;
            tr.GetRelation<IHidings>().Insert(h);
            tr.Commit();
        }

        ReopenDb();
        using var read = _db.StartReadOnlyTransaction();
        var loaded = read.GetRelation<IHidings>().FindById(1);
        Assert.Equal("derived", loaded.X);
        Assert.Equal(0, ((HidingBase)loaded).X);
    }

    [Generate]
    public class HidingObject : HidingBase
    {
        public new string? X { get; set; }
    }

    // Before the fix storing such an object failed, its table field taking the hidden property's type.
    [Fact]
    public void HiddenBasePropertyOfStoredObjectIsNotStored()
    {
        ulong oid;
        _db.RegisterType(typeof(HidingObject));
        using (var tr = _db.StartTransaction())
        {
            var h = new HidingObject { Id = 1, X = "derived" };
            ((HidingBase)h).X = 7;
            oid = tr.Store(h);
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(HidingObject));
        using var read = _db.StartReadOnlyTransaction();
        var loaded = (HidingObject)read.Get(oid)!;
        Assert.Equal("derived", loaded.X);
    }

    public class EventBase
    {
        public int X { get; set; }
    }

    [Generate]
    public class HidingEvent : EventBase
    {
        public new string? X { get; set; }
        public int Other { get; set; }
    }

    // Before the fix the event type descriptor had two fields X, and no saver could be built for the event.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void HiddenBasePropertyOfEventIsNotStored(EventLayer layer)
    {
        var e = new HidingEvent { X = "derived", Other = 5 };
        ((EventBase)e).X = 7;
        var loaded = Assert.IsType<HidingEvent>(EventLayerRoundTripHelper.RoundTrip(layer, e));
        Assert.Equal("derived", loaded.X);
        Assert.Equal(5, loaded.Other);
    }

    public class VirtualBase
    {
        [PrimaryKey] public virtual ulong Id { get; set; }
        [SecondaryKey("Name")] public virtual string Name { get; set; } = "";
        public virtual int Count { get; set; }
    }

    [Generate]
    public class Overriding : VirtualBase
    {
        public override ulong Id { get; set; }
        public override string Name { get; set; } = "";
        public override int Count { get; set; }
    }

    public interface IOverridings : IRelation<Overriding>
    {
        void Insert(Overriding value);
        Overriding FindById(ulong id);
        Overriding FindByName(string name);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OverriddenKeyPropertiesKeepTheirKeys(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            using (var tr = _db.StartTransaction())
            {
                var rows = tr.GetRelation<IOverridings>();
                rows.Insert(new Overriding { Id = 1, Name = "a", Count = 3 });
                rows.Insert(new Overriding { Id = 2, Name = "b", Count = 4 });
                tr.Commit();
            }

            ReopenDb();
            using var read = _db.StartReadOnlyTransaction();
            var table = read.GetRelation<IOverridings>();
            Assert.Equal(3, table.FindById(1).Count);
            Assert.Equal(2ul, table.FindByName("b").Id);
        });
    }

    public class SecretBase
    {
        [PrimaryKey] public ulong Id { get; set; }
        int Secret { get; set; }
        protected int Shared { get; set; }

        public int GetSecret() => Secret;
        public void SetSecret(int value) => Secret = value;
        public int GetShared() => Shared;
        public void SetShared(int value) => Shared = value;
    }

    [Generate]
    public class WithSecret : SecretBase
    {
        public string? Name { get; set; }
    }

    public interface IWithSecrets : IRelation<WithSecret>
    {
        void Insert(WithSecret value);
        WithSecret FindById(ulong id);
    }

    // Before the fix reflection left out private properties of base classes, which the source generator's metadata has:
    // the relation failed to build as its metadata differed, and in emit mode such a property was not stored.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PrivateBasePropertiesAreHandledAlike(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            using (var tr = _db.StartTransaction())
            {
                var row = new WithSecret { Id = 1, Name = "n" };
                row.SetSecret(5);
                row.SetShared(6);
                tr.GetRelation<IWithSecrets>().Insert(row);
                tr.Commit();
            }

            ReopenDb();
            using var read = _db.StartReadOnlyTransaction();
            var loaded = read.GetRelation<IWithSecrets>().FindById(1);
            Assert.Equal("n", loaded.Name);
            Assert.Equal(6, loaded.GetShared());
            Assert.Equal(5, loaded.GetSecret());
        });
    }

    [Generate]
    public class SecretObject : SecretBase
    {
        public string? Name { get; set; }
    }

    // Before the fix such an object lost the private property of its base class in both modes.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PrivateBasePropertiesOfStoredObjectsAreStored(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            ulong oid;
            _db.RegisterType(typeof(SecretObject));
            using (var tr = _db.StartTransaction())
            {
                var o = new SecretObject { Id = 1, Name = "n" };
                o.SetSecret(5);
                o.SetShared(6);
                oid = tr.Store(o);
                tr.Commit();
            }

            ReopenDb();
            _db.RegisterType(typeof(SecretObject));
            using var read = _db.StartReadOnlyTransaction();
            var loaded = (SecretObject)read.Get(oid)!;
            Assert.Equal("n", loaded.Name);
            Assert.Equal(6, loaded.GetShared());
            Assert.Equal(5, loaded.GetSecret());
        });
    }

    [Generate]
    public class GenericRow<T>
    {
        [PrimaryKey] public ulong Id { get; set; }
        public T? Value { get; set; }
        public int? MaybeCount { get; set; }
        public string? @class { get; set; }
        public int @event { get; set; }
    }

    public interface IGenericRows : IRelation<GenericRow<string>>
    {
        void Insert(GenericRow<string> value);
        GenericRow<string> FindById(ulong id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GenericItemWithKeywordNamedPropertiesRoundTrips(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            using (var tr = _db.StartTransaction())
            {
                tr.GetRelation<IGenericRows>().Insert(new GenericRow<string>
                    { Id = 1, Value = "v", MaybeCount = 4, @class = "c", @event = 9 });
                tr.Commit();
            }

            ReopenDb();
            using var read = _db.StartReadOnlyTransaction();
            var loaded = read.GetRelation<IGenericRows>().FindById(1);
            Assert.Equal("v", loaded.Value);
            Assert.Equal(4, loaded.MaybeCount);
            Assert.Equal("c", loaded.@class);
            Assert.Equal(9, loaded.@event);
        });
    }

    // Before the fix the object lost one of the two properties stored under one name. A type built with the source
    // generator gets BTDB0056 instead.
    [Fact]
    public void PersistedNameClashOfObjectIsRefused()
    {
        _db.RegisterType(typeof(Sample3rdPartyLib.PersistedNameClashObject));
        using var tr = _db.StartTransaction();
        var ex = Assert.Throws<BTDB.KVDBLayer.BTDBException>(() =>
            tr.Store(new Sample3rdPartyLib.PersistedNameClashObject { A = 5 }));
        Assert.Contains("under the name X", ex.Message);
    }

    public class RenamedRow
    {
        [PrimaryKey] public ulong Id { get; set; }
        [BTDB.FieldHandler.PersistedName("StoredName")] public string? Name { get; set; }
        [SecondaryKey("ByAge")] [BTDB.FieldHandler.PersistedName("StoredAge")] public int Age { get; set; }
    }

    public interface IRenamedRows : IRelation<RenamedRow>
    {
        void Insert(RenamedRow value);
        RenamedRow FindById(ulong id);
        bool UpdateById(ulong id, string? name);
        System.Collections.Generic.IEnumerable<RenamedRow> ListByByAge(AdvancedEnumeratorParam<int> age);
        System.Collections.Generic.IEnumerable<RenamedRow> ScanByByAge(Constraint<long> age);
    }

    // Before the fix in emit mode a partial update or constraint parameter had to name the field it is stored under,
    // not the property as without emit, so UpdateById(id, name) and ScanByByAge(age) of renamed properties were refused.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PersistedNamePartialUpdateAndSecondaryKey(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            using (var tr = _db.StartTransaction())
            {
                var rows = tr.GetRelation<IRenamedRows>();
                rows.Insert(new RenamedRow { Id = 1, Name = "a", Age = 30 });
                rows.Insert(new RenamedRow { Id = 2, Name = "b", Age = 20 });
                Assert.True(rows.UpdateById(1, "changed"));
                tr.Commit();
            }

            ReopenDb();
            using var read = _db.StartReadOnlyTransaction();
            var table = read.GetRelation<IRenamedRows>();
            Assert.Equal("changed", table.FindById(1).Name);
            Assert.Equal(30, table.FindById(1).Age);
            Assert.Equal([2ul, 1ul], table.ListByByAge(new()).Select(r => r.Id));
            Assert.Equal([1ul], table.ScanByByAge(Constraint.Signed.Exact(30)).Select(r => r.Id));
        });
    }

    public class RenamedKeyRow
    {
        [PrimaryKey] [BTDB.FieldHandler.PersistedName("Key")] public ulong Id { get; set; }
        [SecondaryKey("ByAge")] [BTDB.FieldHandler.PersistedName("StoredAge")] public int Age { get; set; }
    }

    public interface IRenamedKeyRows : IRelation<RenamedKeyRow>
    {
        void Insert(RenamedKeyRow value);
        RenamedKeyRow FindById(ulong id);
        RenamedKeyRow? FindByIdOrDefault(ulong id);
        bool RemoveById(ulong id);
        System.Collections.Generic.IEnumerable<RenamedKeyRow> FindByByAge(int age);
    }

    // Before the fix in emit mode key parameters had to name the fields keys are stored under, not the properties.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RenamedKeysAreFoundByPropertyNamedParameters(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var rows = tr.GetRelation<IRenamedKeyRows>();
            rows.Insert(new RenamedKeyRow { Id = 1, Age = 30 });
            rows.Insert(new RenamedKeyRow { Id = 2, Age = 30 });
            Assert.Equal(30, rows.FindById(1).Age);
            Assert.Equal([1ul, 2ul], rows.FindByByAge(30).Select(r => r.Id));
            Assert.True(rows.RemoveById(2));
            Assert.Null(rows.FindByIdOrDefault(2));
        });
    }

    // Queries map properties to the fields they are stored under, also when PersistedName renames them.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void QueriesOverRenamedPropertiesMatchLinq(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var rows = tr.GetRelation<IRenamedRows>();
            var all = new[]
            {
                new RenamedRow { Id = 1, Name = "a", Age = 30 }, new RenamedRow { Id = 2, Name = "b", Age = 20 },
                new RenamedRow { Id = 3, Name = "c", Age = 40 }
            };
            foreach (var row in all) rows.Insert(row);
            Assert.Equal(all.Where(r => r.Age > 25).Select(r => r.Id),
                rows.Query<RenamedRow>().Where(r => r.Age > 25).AsEnumerable().Select(r => r.Id));
            Assert.Equal(all.Where(r => r.Name == "b").Select(r => r.Id),
                rows.Query<RenamedRow>().Where(r => r.Name == "b").AsEnumerable().Select(r => r.Id));
            Assert.Equal(all.OrderBy(r => r.Age).Select(r => r.Id),
                rows.Query<RenamedRow>().OrderBy(r => r.Age).AsEnumerable().Select(r => r.Id));
        });
    }

    public class BeforeRename
    {
        [PrimaryKey] public ulong Id { get; set; }
        public string? Name { get; set; }
        public int Count { get; set; }
    }

    public interface IBeforeRenames : IRelation<BeforeRename>
    {
        void Insert(BeforeRename value);
    }

    public class AfterRename
    {
        [PrimaryKey] public ulong Id { get; set; }
        [BTDB.FieldHandler.PersistedName("Name")] public string? Title { get; set; }
        public int Count { get; set; }
    }

    public interface IAfterRenames : IRelation<AfterRename>
    {
        AfterRename FindById(ulong id);
        bool UpdateById(ulong id, string? title);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RenamingPropertyWithPersistedNameKeepsData(bool noEmit)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            using (var tr = _db.StartTransaction())
            {
                tr.InitRelation<IBeforeRenames>("Renamed")(tr).Insert(new BeforeRename { Id = 1, Name = "n", Count = 2 });
                tr.Commit();
            }

            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var after = tr.InitRelation<IAfterRenames>("Renamed")(tr);
                Assert.Equal("n", after.FindById(1).Title);
                Assert.Equal(2, after.FindById(1).Count);
                Assert.True(after.UpdateById(1, "t"));
                Assert.Equal("t", after.FindById(1).Title);
                tr.Commit();
            }
        });
    }
}
