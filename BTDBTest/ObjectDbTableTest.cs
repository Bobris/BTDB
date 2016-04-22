using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest
{
    public class ObjectDbTableTest : IDisposable
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;

        public ObjectDbTableTest()
        {
            _lowDb = new InMemoryKeyValueDB();
            OpenDb();
        }

        public void Dispose()
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

        public class PersonSimple : IEquatable<PersonSimple>
        {
            [PrimaryKey(1)]
            public ulong TenantId { get; set; }
            [PrimaryKey(2)]
            public string Email { get; set; }
            public string Name { get; set; }

            public bool Equals(PersonSimple other)
            {
                return TenantId == other.TenantId && string.Equals(Email, other.Email) && string.Equals(Name, other.Name);
            }
        }

        public interface IPersonSimpleTableWithJustInsert
        {
            void Insert(PersonSimple person);
        }

        [Fact]
        public void GeneratesCreator()
        {
            Func<IObjectDBTransaction, IPersonSimpleTableWithJustInsert> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IPersonSimpleTableWithJustInsert>("PersonSimple");
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                personSimpleTable.Insert(new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Boris" });
                tr.Commit();
            }
        }

        [Fact]
        public void RefuseUnshapedInterface()
        {
            using (var tr = _db.StartTransaction())
            {
                var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IDisposable>("PersonSimple"));
                Assert.True(ex.Message.Contains("Cannot deduce"));
            }
        }

        [Fact]
        public void CannotInsertSameKeyTwice()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPersonSimpleTableWithJustInsert>("PersonSimple");
                var personSimpleTable = creator(tr);
                personSimpleTable.Insert(new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Boris" });
                personSimpleTable.Insert(new PersonSimple { TenantId = 2, Email = "nospam@nospam.cz", Name = "Boris" });
                var ex = Assert.Throws<BTDBException>(() => personSimpleTable.Insert(new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Boris" }));
                Assert.True(ex.Message.Contains("duplicate"));
                tr.Commit();
            }
        }

        public interface ISimplePersonTable
        {
            void Insert(PersonSimple person);
            // Upsert = Insert or Update - return true if inserted
            bool Upsert(PersonSimple person);
            // Update will throw if does not exist
            void Update(PersonSimple person);
            IEnumerator<PersonSimple> GetEnumerator();
            // Returns true if removed
            bool RemoveById(ulong tenantId, string email);
            // It will throw if does not exists
            PersonSimple FindById(ulong tenantId, string email);
            // Will return null if not exists
            PersonSimple FindByIdOrDefault(ulong tenantId, string email);
        }

        [Fact]
        public void CanInsertAndEnumerate()
        {
            var personBoris = new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Boris" };
            var personLubos = new PersonSimple { TenantId = 2, Email = "nospam@nospam.cz", Name = "Lubos" };

            Func<IObjectDBTransaction, ISimplePersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTable>("Person");
                var personSimpleTable = creator(tr);
                personSimpleTable.Insert(personBoris);
                personSimpleTable.Insert(personLubos);
                tr.Commit();
            }
            using (var tr = _db.StartReadOnlyTransaction())
            {
                var personSimpleTable = creator(tr);
                var enumerator = personSimpleTable.GetEnumerator();
                Assert.True(enumerator.MoveNext());
                var person = enumerator.Current;
                Assert.Equal(personBoris, person);
                Assert.True(enumerator.MoveNext());
                person = enumerator.Current;
                Assert.Equal(personLubos, person);
                Assert.False(enumerator.MoveNext(), "Only one Person should be evaluated");
            }
        }

        T GetFirst<T>(IEnumerator<T> enumerator)
        {
            if (!enumerator.MoveNext())
                throw new Exception("Empty");
            return enumerator.Current;
        }

        [Fact]
        public void UpsertWorks()
        {
            var person = new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Boris" };
            Func<IObjectDBTransaction, ISimplePersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTable>("Person");
                var personSimpleTable = creator(tr);
                Assert.True(personSimpleTable.Upsert(person), "Is newly inserted");
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                person.Name = "Lubos";
                Assert.False(personSimpleTable.Upsert(person), "Was already there");
                var p = GetFirst(personSimpleTable.GetEnumerator());
                Assert.Equal("Lubos", p.Name);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var p = GetFirst(personSimpleTable.GetEnumerator());
                Assert.Equal("Lubos", p.Name);
            }
        }

        [Fact]
        public void UpdateWorks()
        {
            var person = new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Boris" };
            Func<IObjectDBTransaction, ISimplePersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTable>("Person");
                var personSimpleTable = creator(tr);
                Assert.Throws<BTDBException>(() => personSimpleTable.Update(person));
                personSimpleTable.Insert(person);
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                person.Name = "Lubos";
                personSimpleTable.Update(person);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var p = GetFirst(personSimpleTable.GetEnumerator());
                Assert.Equal("Lubos", p.Name);
            }
        }

        [Fact]
        public void RemoveByIdWorks()
        {
            var person = new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Lubos" };
            Func<IObjectDBTransaction, ISimplePersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTable>("PersonSimple");
                var personSimpleTable = creator(tr);
                personSimpleTable.Insert(person);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                Assert.False(personSimpleTable.RemoveById(0, "no@no.cz"));
                Assert.True(personSimpleTable.RemoveById(person.TenantId, person.Email));
                Assert.False(personSimpleTable.GetEnumerator().MoveNext());
            }
        }

        [Fact]
        public void FindByIdWorks()
        {
            var person = new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Lubos" };
            Func<IObjectDBTransaction, ISimplePersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTable>("PersonSimple");
                var personSimpleTable = creator(tr);
                personSimpleTable.Insert(person);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var foundPerson = personSimpleTable.FindById(person.TenantId, person.Email);
                Assert.True(person.Equals(foundPerson));
                Assert.Throws<BTDBException>(() => personSimpleTable.FindById(0, "no@no.cz"));
                foundPerson = personSimpleTable.FindByIdOrDefault(person.TenantId, person.Email);
                Assert.True(person.Equals(foundPerson));
                foundPerson = personSimpleTable.FindByIdOrDefault(0, "no@no.cz");
                Assert.Equal(null, foundPerson);
            }
        }


        public interface ISimplePersonTableWithVoidRemove
        {
            void Insert(PersonSimple person);
            // Throws if not removed
            void RemoveById(ulong tenantId, string email);
        }

        [Fact]
        public void RemoveByIdThrowsWhenNotFound()
        {
            var person = new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Lubos" };
            Func<IObjectDBTransaction, ISimplePersonTableWithVoidRemove> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTableWithVoidRemove>("PersonSimpleVoidRemove");
                var personSimpleTable = creator(tr);
                personSimpleTable.Insert(person);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                Assert.Throws<BTDBException>(() => personSimpleTable.RemoveById(0, "no@no.cz"));
                personSimpleTable.RemoveById(person.TenantId, person.Email);
            }
        }

        public class Person
        {
            [PrimaryKey(1)]
            public ulong TenantId { get; set; }
            [PrimaryKey(2)]
            public Guid Id { get; set; }
            [SecondaryKey("Age", Order = 2)]
            [SecondaryKey("Name", IncludePrimaryKeyOrder = 1)]
            public string Name { get; set; }
            [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)]
            public uint Age { get; set; }
        }

        public interface IPersonTableComplexFuture
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
