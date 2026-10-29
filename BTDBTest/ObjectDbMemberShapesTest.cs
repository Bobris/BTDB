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
}
