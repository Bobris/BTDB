using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableTupleTest : ObjectDbTestBase
{
    public ObjectDbTableTupleTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Obj
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public Tuple<int, uint> Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IObjTable : IRelation<Obj>
    {
        void Insert(Obj obj);
    }

    [Fact]
    public void BasicTupleInValueWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IObjTable>().Insert(new() { Id = 1, Val = new(2, 3) });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(new Tuple<int, uint>(2, 3), tr.GetRelation<IObjTable>().First().Val);
        }
    }

    public class ObjVal
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public (int, uint) Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IObjValTable : IRelation<ObjVal>
    {
        void Insert(ObjVal obj);
    }

    [Fact]
    public void BasicValueTupleInValueWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IObjValTable>().Insert(new() { Id = 1, Val = new(2, 3) });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.Equal((2, 3u), tr.GetRelation<IObjValTable>().First().Val);
        }
    }

    public class KeyObj
    {
        [PrimaryKey(1)] public Tuple<int, uint> Id { get; set; }

        public int Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IKeyObjTable : IRelation<KeyObj>
    {
        void Insert(KeyObj obj);
        IEnumerable<KeyObj> ListById(AdvancedEnumeratorParam<Tuple<int, uint>> param);
    }

    [Fact]
    public void BasicTupleInKeyWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IKeyObjTable>().Insert(new() { Id = new(2, 3), Val = 1 });
            tr.GetRelation<IKeyObjTable>().Insert(new() { Id = new(3, 2), Val = 2 });
            tr.GetRelation<IKeyObjTable>().Insert(new() { Id = new(3, 3), Val = 3 });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(3,
                tr.GetRelation<IKeyObjTable>()
                    .ListById(new AdvancedEnumeratorParam<Tuple<int, uint>>(EnumerationOrder.Descending)).First()
                    .Val);
            Assert.Equal(2,
                tr.GetRelation<IKeyObjTable>()
                    .ListById(new AdvancedEnumeratorParam<Tuple<int, uint>>(EnumerationOrder.Ascending, new(3, 1),
                        KeyProposition.Included, null, KeyProposition.Ignored)).First().Val);
            Assert.Equal(2,
                tr.GetRelation<IKeyObjTable>()
                    .ListById(new AdvancedEnumeratorParam<Tuple<int, uint>>(EnumerationOrder.Ascending, new(2, 3),
                        KeyProposition.Excluded, null, KeyProposition.Ignored)).First().Val);
        }
    }

    public class KeyObjVal
    {
        [PrimaryKey(1)] public (int, uint) Id { get; set; }

        public int Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IKeyObjValTable : IRelation<KeyObjVal>
    {
        void Insert(KeyObjVal obj);
        IEnumerable<KeyObjVal> ListById(AdvancedEnumeratorParam<(int, uint)> param);
    }

    [Fact]
    public void BasicValueTupleInKeyWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IKeyObjValTable>().Insert(new() { Id = (2, 3), Val = 1 });
            tr.GetRelation<IKeyObjValTable>().Insert(new() { Id = (3, 2), Val = 2 });
            tr.GetRelation<IKeyObjValTable>().Insert(new() { Id = (3, 3), Val = 3 });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(3,
                tr.GetRelation<IKeyObjValTable>()
                    .ListById(new AdvancedEnumeratorParam<(int, uint)>(EnumerationOrder.Descending)).First()
                    .Val);
            Assert.Equal(2,
                tr.GetRelation<IKeyObjValTable>()
                    .ListById(new AdvancedEnumeratorParam<(int, uint)>(EnumerationOrder.Ascending, new(3, 1),
                        KeyProposition.Included, default, KeyProposition.Ignored)).First().Val);
            Assert.Equal(2,
                tr.GetRelation<IKeyObjValTable>()
                    .ListById(new AdvancedEnumeratorParam<(int, uint)>(EnumerationOrder.Ascending, new(2, 3),
                        KeyProposition.Excluded, default, KeyProposition.Ignored)).First().Val);
        }
    }

    public class Obj2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public Tuple<int, uint, bool> Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IObj2Table : IRelation<Obj2>
    {
        void Insert(Obj2 obj);
    }

    [Fact]
    public void UpgradeTupleInValueWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IObjTable>().Insert(new() { Id = 1, Val = new(2, 3) });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(new Tuple<int, uint, bool>(2, 3, false), tr.GetRelation<IObj2Table>().First().Val);
        }
    }

    public class Obj3
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public Tuple<int> Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IObj3Table : IRelation<Obj3>
    {
        void Insert(Obj3 obj);
    }

    [Fact]
    public void DowngradeTupleInValueWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IObjTable>().Insert(new() { Id = 1, Val = new(2, 3) });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(new Tuple<int>(2), tr.GetRelation<IObj3Table>().First().Val);
        }
    }

    public class ObjVal2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public (int, uint, bool) Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IObjVal2Table : IRelation<ObjVal2>
    {
        void Insert(ObjVal2 obj);
    }

    [Fact]
    public void UpgradeValueTupleInValueWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IObjValTable>().Insert(new() { Id = 1, Val = (2, 3) });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            Assert.Equal((2, 3u, false), tr.GetRelation<IObjVal2Table>().First().Val);
        }
    }

    public class ObjVal3
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public ValueTuple<int> Val { get; set; }
    }

    [PersistedName("Table")]
    public interface IObjVal3Table : IRelation<ObjVal3>
    {
        void Insert(ObjVal3 obj);
    }

    [Fact]
    public void DowngradeValueTupleInValueWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IObjValTable>().Insert(new() { Id = 1, Val = (2, 3) });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(new ValueTuple<int>(2), tr.GetRelation<IObjVal3Table>().First().Val);
        }
    }
}
