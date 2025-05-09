using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System;
using BTDB;
using Xunit;

namespace BTDBTest;

public class TypeRegistrationTest : IDisposable
{
    IKeyValueDB _lowDb;
    IObjectDB _db;

    [Generate]
    public class Parent
    {
        public IChild Child { get; set; }
    }

    [Generate]
    public class ParentOldVersion
    {
        public Child Child { get; set; }
    }

    [Generate]
    public class ParentNewVersion
    {
        public IChild Child { get; set; }
        public ulong Something { get; set; }
    }

    [Generate]
    public interface IChild
    {
        ulong Id { get; set; }
    }

    public class Child : IChild
    {
        public ulong Id { get; set; }
    }

    public class DerivedChild : Child
    {
    }

    public TypeRegistrationTest()
    {
        _lowDb = new InMemoryKeyValueDB();
        OpenDb();
    }

    void OpenDb()
    {
        _db = new ObjectDB();
        _db.Open(_lowDb, false);
    }

    void ReopenDb()
    {
        _db.Dispose();
        OpenDb();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }

    [Fact]
    public void MaterializesInlineObjectProperty()
    {
        ulong oid;
        using (var tr = _db.StartTransaction())
        {
            oid = tr.Store(new Parent { Child = new DerivedChild { Id = 1 } });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(ParentNewVersion), "Parent");
        _db.RegisterType(typeof(DerivedChild));
        _db.RegisterType(typeof(Child));
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var parent = (ParentNewVersion)tr.Get(oid);
            Assert.NotNull(parent.Child);
            Assert.Equal(1ul, parent.Child.Id);
        }
    }

    [Fact]
    public void UpgradesFromClassToInterface()
    {
        ulong oid;
        _db.RegisterType(typeof(ParentOldVersion), "Parent");
        using (var tr = _db.StartTransaction())
        {
            oid = tr.Store(new ParentOldVersion { Child = new DerivedChild { Id = 1 } });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(ParentNewVersion), "Parent");
        _db.RegisterType(typeof(DerivedChild));
        _db.RegisterType(typeof(Child));
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var parent = (ParentNewVersion)tr.Get(oid);
            Assert.NotNull(parent.Child);
            Assert.Equal(1ul, parent.Child.Id);
        }
    }
}
