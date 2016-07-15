using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest
{
    public class ObjectDbTableTest : IDisposable
    {
        readonly IKeyValueDB _lowDb;
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
                return TenantId == other.TenantId && string.Equals(Email, other.Email) &&
                       string.Equals(Name, other.Name);
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
                var ex =
                    Assert.Throws<BTDBException>(
                        () =>
                            personSimpleTable.Insert(new PersonSimple
                            {
                                TenantId = 1,
                                Email = "nospam@nospam.cz",
                                Name = "Boris"
                            }));
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

        T GetNext<T>(IEnumerator<T> enumerator)
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
                var p = GetNext(personSimpleTable.GetEnumerator());
                Assert.Equal("Lubos", p.Name);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var p = GetNext(personSimpleTable.GetEnumerator());
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
                var p = GetNext(personSimpleTable.GetEnumerator());
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

        public interface InvalidPersonTable
        {
            ulong NotCorrespondingField { get; set; }
            void Insert(PersonSimple person);
        }

        [Fact]
        public void DoNotGenerateInterfaceWithPropertiesNotPresentInPrimaryKey()
        {
            using (var tr = _db.StartTransaction())
            {
                Assert.Throws<BTDBException>(() => tr.InitRelation<InvalidPersonTable>("InvalidTable"));
            }
        }

        public interface ISimplePersonTableWithTenantId
        {
            ulong TenantId { get; set; }
            void Insert(PersonSimple person);
            bool RemoveById(string email); //TenantId is used - no need to pass as parameter
        }

        [Fact]
        public void BasicRelationWithTenantApartWorks()
        {
            var person = new PersonSimple { Email = "nospam@nospam.cz", Name = "Lubos" };
            Func<IObjectDBTransaction, ISimplePersonTableWithTenantId> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTableWithTenantId>("PersonSimpleTenantId");
                var personSimpleTable = creator(tr);
                personSimpleTable.TenantId = 1;
                personSimpleTable.Insert(person);
                personSimpleTable.TenantId = 2;
                personSimpleTable.Insert(person);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                personSimpleTable.TenantId = 0;
                Assert.False(personSimpleTable.RemoveById(person.Email));
                personSimpleTable.TenantId = 1;
                Assert.True(personSimpleTable.RemoveById(person.Email));
                personSimpleTable.TenantId = 2;
                Assert.True(personSimpleTable.RemoveById(person.Email));
            }
        }

        public class Person
        {
            [PrimaryKey(1)]
            public ulong TenantId { get; set; }

            [PrimaryKey(2)]
            public ulong Id { get; set; }

            [SecondaryKey("Age", Order = 2)]
            [SecondaryKey("Name", IncludePrimaryKeyOrder = 1)]
            public string Name { get; set; }

            [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)]
            public uint Age { get; set; }
        }

        //SK content
        //"Age": TenantId, Age, Name, Id => void
        //"Name": TenantId, Name, Id => void

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
            Person FindById(ulong id);
            // Will return null if not exists
            Person FindByIdOrDefault(ulong id);
            // Find by secondary key, it will throw if it find multiple Persons with that age
            Person FindByAgeOrDefault(uint age);
            IEnumerator<Person> FindByAge(uint age);
            // Returns true if removed, if returning void it does throw if does not exists
            bool RemoveById(ulong id);

            // fills all your iterating needs
            IOrderedDictionaryEnumerator<string, Person> ListByName(AdvancedEnumeratorParam<string> param);
            IEnumerator<Person> GetEnumerator();
            IOrderedDictionaryEnumerator<uint, Person> ListByAge(AdvancedEnumeratorParam<uint> param);
        }

        [Fact]
        public void AdvancedIteratingWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPersonTableComplexFuture>("PersonComplex");
                var personTable = creator(tr);

                personTable.TenantId = 1;
                personTable.Insert(new Person { Id = 2, Name = "Lubos", Age = 28 });
                personTable.Insert(new Person { Id = 3, Name = "Boris", Age = 29 });

                personTable.TenantId = 2;
                personTable.Insert(new Person { Id = 2, Name = "Lubos", Age = 128 });
                personTable.Insert(new Person { Id = 3, Name = "Boris", Age = 129 });

                var orderedEnumerator = personTable.ListByAge(new AdvancedEnumeratorParam<uint>(EnumerationOrder.Ascending));
                Assert.Equal(2u, orderedEnumerator.Count);

                uint age;
                Assert.True(orderedEnumerator.NextKey(out age));
                Assert.Equal(128u, age);
                Assert.Equal("Lubos", orderedEnumerator.CurrentValue.Name);
                Assert.True(orderedEnumerator.NextKey(out age));
                Assert.Equal(129u, age);
                tr.Commit();
            }
        }

        public interface IPersonTable
        {
            void Insert(Person person);
            void Update(Person person);
            bool RemoveById(ulong tenantId, ulong id);
            Person FindByNameOrDefault(ulong tenantId, string name);
            Person FindByAgeOrDefault(ulong tenantId, uint age);
            IEnumerator<Person> FindByAge(ulong tenantId, uint age);
        }

        [Fact]
        public void SimpleFindBySecondaryKeyWorks()
        {
            Func<IObjectDBTransaction, IPersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IPersonTable>("Person");
                var personTable = creator(tr);
                personTable.Insert(new Person { TenantId = 1, Id = 2, Name = "Lubos", Age = 28 });
                personTable.Insert(new Person { TenantId = 1, Id = 3, Name = "Boris", Age = 28 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personTable = creator(tr);
                var ex = Assert.Throws<BTDBException>(() => personTable.FindByAgeOrDefault(1, 28));
                Assert.True(ex.Message.Contains("Ambiguous"));
                var p = personTable.FindByNameOrDefault(1, "Lubos");
                Assert.Equal(28u, p.Age);

                var enumerator = personTable.FindByAge(1, 28);
                Assert.Equal("Boris", GetNext(enumerator).Name);
                Assert.Equal("Lubos", GetNext(enumerator).Name);
                Assert.False(enumerator.MoveNext());

                Assert.True(personTable.RemoveById(1, 2));
                tr.Commit();
            }
        }

        public class Job
        {
            [PrimaryKey(1)]
            public ulong Id { get; set; }

            [SecondaryKey("Name")]
            [SecondaryKey("PrioritizedName", Order = 2)]
            public string Name { get; set; }

            [SecondaryKey("PrioritizedName")]
            public short Priority { get; set; }
        }

        public interface IJobTable
        {
            void Insert(Job person);
            void Update(Job person);
            bool RemoveById(ulong id);

            Job FindByIdOrDefault(ulong id);
            Job FindByNameOrDefault(string name);

            IEnumerator<Job> ListByName(AdvancedEnumeratorParam<string> param);
            IEnumerator<Job> ListByPrioritizedName(short priority, AdvancedEnumeratorParam<string> param);
        }

        [Fact]
        public void SecondaryKeyWorksWithoutIncludedPrimaryWorks()
        {
            Func<IObjectDBTransaction, IJobTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IJobTable>("Job");
                var jobTable = creator(tr);
                jobTable.Insert(new Job { Id = 11, Name = "Code", Priority = 1 });
                jobTable.Insert(new Job { Id = 22, Name = "Sleep", Priority = 2 });
                jobTable.Insert(new Job { Id = 33, Name = "Bicycle", Priority = 1 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var jobTable = creator(tr);
                var en = jobTable.ListByPrioritizedName(2, new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending));
                var j = GetNext(en);
                Assert.Equal("Sleep", j.Name);
                Assert.False(en.MoveNext());
            }
            using (var tr = _db.StartTransaction())
            {
                var jobTable = creator(tr);
                var job = jobTable.FindByNameOrDefault("Dude");
                Assert.Equal(null, job);
                job = jobTable.FindByNameOrDefault("Code");
                Assert.Equal(11u, job.Id);
                Assert.True(jobTable.RemoveById(11));
                tr.Commit();
            }
        }

        [Fact]
        public void CanAddButCannotGetDuplicateSecondaryKeys()
        {
            Func<IObjectDBTransaction, IJobTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IJobTable>("Job");
                var jobTable = creator(tr);
                jobTable.Insert(new Job { Id = 11, Name = "Code" });
                jobTable.Insert(new Job { Id = 12, Name = "Code" });
                var ex = Assert.Throws<BTDBException>(() => jobTable.FindByNameOrDefault("Code"));
                Assert.True(ex.Message.Contains("Ambiguous"));
            }
        }

        [Fact]
        public void UpdateSecondaryIndexWorks()
        {
            Func<IObjectDBTransaction, IJobTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IJobTable>("Job");
                var jobTable = creator(tr);
                var job = new Job { Id = 11, Name = "Code" };
                jobTable.Insert(job);
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IJobTable>("Job");
                var jobTable = creator(tr);
                var job = new Job { Id = 11, Name = "HardCore Code" };
                jobTable.Update(job);
                var j = jobTable.FindByNameOrDefault("HardCore Code");
                Assert.Equal(11u, j.Id);
            }
        }

        [Fact]
        public void CanIterateBySecondaryKey()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IJobTable>("Job");
                var jobTable = creator(tr);
                jobTable.Insert(new Job { Id = 11, Name = "Code" });
                jobTable.Insert(new Job { Id = 22, Name = "Sleep" });
                jobTable.Insert(new Job { Id = 33, Name = "Bicycle" });

                var en = jobTable.ListByName(new AdvancedEnumeratorParam<string>(EnumerationOrder.Descending));

                Assert.Equal("Sleep", GetNext(en).Name);
                Assert.Equal("Code", GetNext(en).Name);
                Assert.Equal("Bicycle", GetNext(en).Name);

                en = jobTable.ListByName(new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "B", KeyProposition.Included,
                    "C", KeyProposition.Included));
                Assert.Equal("Bicycle", GetNext(en).Name);
                Assert.False(en.MoveNext());
            }
        }

        public class Lic
        {
            [PrimaryKey(1)]
            [SecondaryKey("CompanyId")]
            [SecondaryKey("CompanyIdAndStatus")]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("UserId")]
            [SecondaryKey("UserIdAndStatus")]
            public ulong UserId { get; set; }

            [SecondaryKey("Status")]
            [SecondaryKey("CompanyIdAndStatus", Order = 2)]
            [SecondaryKey("UserIdAndStatus", Order = 2)]
            public string Status { get; set; }
        }

        public interface ILicTable
        {
            void Insert(Lic lic);
        }

        [Fact]
        public void CanHaveMoreSecondaryIndexesThanKeys()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.InitRelation<ILicTable>("Lic");
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<ILicTable>("Lic");
                creator(tr).Insert(new Lic { CompanyId = 1, UserId = 2, Status = "ok" });
            }
        }

        public class Room
        {
            [PrimaryKey(1)]
            [SecondaryKey("CompanyId")]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("Id")]
            public ulong Id { get; set; }

            public string Name { get; set; }
        }

        public interface IRoomTable
        {
            void Insert(Room room);
            IEnumerator<Room> ListByCompanyId(AdvancedEnumeratorParam<ulong> param);
            IOrderedDictionaryEnumerator<ulong, Room> ListById(AdvancedEnumeratorParam<ulong> param);
        }

        [Fact]
        public void SecondaryKeyCanBeDefinedOnSameFieldAsPrimaryKey()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IRoomTable>("Room");

                var rooms = creator(tr);
                rooms.Insert(new Room { CompanyId = 1, Id = 1, Name = "First 1" });
                rooms.Insert(new Room { CompanyId = 1, Id = 2, Name = "Second 1" });
                rooms.Insert(new Room { CompanyId = 2, Id = 1, Name = "First 2" });

                var en = rooms.ListByCompanyId(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                                               1, KeyProposition.Included,
                                               1 + 1, KeyProposition.Excluded));

                var m = GetNext(en);
                Assert.Equal("First 1", m.Name);
                m = GetNext(en);
                Assert.Equal("Second 1", m.Name);
                Assert.False(en.MoveNext());

                var oen = rooms.ListById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Descending));
                Assert.Equal(3u, oen.Count);
                ulong key;
                Assert.True(oen.NextKey(out key));
                Assert.Equal(2ul, key);
                Assert.True(oen.NextKey(out key));
                Assert.Equal(1ul, key);
                Assert.True(oen.NextKey(out key));
                Assert.Equal(1ul, key);
                Assert.False(oen.NextKey(out key));
            }
        }
    }
}
