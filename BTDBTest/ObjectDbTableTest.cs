using System;
using System.Collections.Generic;
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
            [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)]
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

        public interface IPersonTable
        {
            ulong TenantId { get; set; }
            // Should Insert with different TenantId throw? Or it should set tenantId before writing?
            // Insert will throw if already exists
            void Insert(Person person);
            // Upsert = Insert or Update - return true if inserted
            bool Upsert(Person person);
            // Update will throw if does not exist
            void Update(Person person);
            // It will throw if does not exists
            Person FindById(Guid id);
            // Will return null if not exists
            Person FindByIdOrDefault(Guid id);
            // Find by secondary key, it will throw if it find multiple Persons with that age
            Person FindByAgeOrDefault(uint age);
            IEnumerator<Person> FindByAge(uint age);  
            // Returns true if removed, if returning void it does throw if does not exists
            bool RemoveById(Guid id);
            // fills all your iterating needs
            IOrderedDictionaryEnumerator<Guid, Person> ListById(AdvancedEnumeratorParam<Guid> param);
            IEnumerator<Person> GetEnumerator();
            IOrderedDictionaryEnumerator<uint, Person> ListByAge(AdvancedEnumeratorParam<uint> param);
        }

    }
}
