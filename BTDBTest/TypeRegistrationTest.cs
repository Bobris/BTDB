using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System;
using Xunit;

namespace BTDBTest
{
    public class Parent
    {
        public IChild Child { get; set; }
    }

    public class ParentNewVersion
    {
        public IChild Child { get; set; }
        public ulong Something { get; set; }
    }

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

    public class TypeRegistrationTest : IDisposable
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;

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
            }
        }
    }
}