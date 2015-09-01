using System;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class ObjectDbTableTest
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;

        [SetUp]
        public void Setup()
        {
            _lowDb = new InMemoryKeyValueDB();
            OpenDb();
        }

        [TearDown]
        public void TearDown()
        {
            _db.Dispose();
            _lowDb.Dispose();
        }

        void ReopenDb()
        {
            _db.Dispose();
            OpenDb();
        }

        void OpenDb()
        {
            _db = new ObjectDB();
            _db.Open(_lowDb, false);
        }

        public class Person
        {
            [PrimaryKey(1)]
            public ulong TenantId { get; set; }
            [PrimaryKey(2)]
            public Guid Id { get; set; }
            public string Name { get; set; }
            public uint Age { get; set; }
        }

        public interface IPersonTableWithJustInsert
        {
            void Insert(Person person);
        }

        [Test]
        public void GeneratesCreator()
        {
            Func<IObjectDBTransaction, IPersonTableWithJustInsert> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IPersonTableWithJustInsert>("Person");
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                creator(tr).Insert(new Person { TenantId = 1, Id = Guid.NewGuid(), Name = "Boris", Age = 39 });
                tr.Commit();
            }
        }

    }
}
