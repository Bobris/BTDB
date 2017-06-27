using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest
{
    public class ObjectDbTableTest : IDisposable
    {
        readonly ITestOutputHelper _output;
        readonly IKeyValueDB _lowDb;
        IObjectDB _db;

        public ObjectDbTableTest(ITestOutputHelper output)
        {
            _output = output;
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
            _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration());
        }

        public class PersonSimple : IEquatable<PersonSimple>
        {
            [PrimaryKey(1)]
            public ulong TenantId { get; set; }

            [PrimaryKey(2)]
            public string Email { get; set; }

            public string Name { get; set; }

            public Dictionary<string, IList<byte>> Ratings { get; set; }

            public bool Equals(PersonSimple other)
            {
                if (TenantId != other.TenantId || !string.Equals(Email, other.Email) ||
                    !string.Equals(Name, other.Name))
                    return false;

                if (Ratings == other.Ratings)
                    return true;
                if (Ratings == null || other.Ratings == null)
                    return false;
                if (Ratings.Count != other.Ratings.Count)
                    return false;

                foreach (var r in Ratings)
                {
                    IList<byte> otherValue;
                    if (!other.Ratings.TryGetValue(r.Key, out otherValue))
                        return false;
                    if (r.Value == otherValue)
                        return true;
                    if (r.Value == null || otherValue == null)
                        return false;
                    if (!r.Value.SequenceEqual(otherValue))
                        return false;
                }

                return true;
            }
        }

        public interface IPersonSimpleTableWithJustInsert : IReadOnlyCollection<PersonSimple>
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
                Assert.Equal(1, personSimpleTable.Count);
                var en = personSimpleTable.GetEnumerator();
                en.MoveNext();
                Assert.Equal("Boris", en.Current.Name);
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

        public interface IPersonSimpleTableWithInsert : IReadOnlyCollection<PersonSimple>
        {
            bool Insert(PersonSimple person);
            PersonSimple FindById(ulong tenantId, string email);
        }

        [Fact]
        public void TryInsertWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPersonSimpleTableWithInsert>("TryInsertWorks");
                var personSimpleTable = creator(tr);
                Assert.True(personSimpleTable.Insert(new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "A" }));
                Assert.False(personSimpleTable.Insert(new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "B" }));
                Assert.Equal("A", personSimpleTable.FindById(1, "nospam@nospam.cz").Name);
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
            var person = new PersonSimple
            {
                TenantId = 1,
                Email = "nospam@nospam.cz",
                Name = "Boris",
                Ratings = new Dictionary<string, IList<byte>> { { "Czech", new List<byte> { 1, 2, 1 } } },
            };
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
                creator = tr.InitRelation<ISimplePersonTable>("Person");
                var personSimpleTable = creator(tr);
                person.Name = "Lubos";
                person.Ratings.Add("History", new List<byte> { 3 });
                personSimpleTable.Update(person);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var p = GetNext(personSimpleTable.GetEnumerator());
                Assert.Equal("Lubos", p.Name);
                Assert.Equal(new List<byte> { 3 }, p.Ratings["History"]);
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
                Assert.Null(foundPerson);
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

        [Fact]
        public void CannotUseUninitializedApartFields()
        {
            var person = new PersonSimple { Email = "nospam@nospam.cz", Name = "Lubos" };
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<ISimplePersonTableWithTenantId>("PersonSimpleTenantId");
                var personSimpleTable = creator(tr);
                ThrowsUninitialized(() => personSimpleTable.Insert(person));
                ThrowsUninitialized(() => personSimpleTable.RemoveById("nospam@nospam.cz"));
                tr.Commit();
            }
        }

        void ThrowsUninitialized(Action action)
        {
            var ex = Assert.Throws<BTDBException>(action);
            Assert.True(ex.Message.Contains("uninitialized"));
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

            //ListBy primary key (only for active tenant)
            IOrderedDictionaryEnumerator<ulong, Person> ListById(AdvancedEnumeratorParam<ulong> param);
            //enumerate all items - not using TenantId
            IEnumerator<Person> GetEnumerator();
            //ListBy{SecondaryKeyName}
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

                var en = personTable.GetEnumerator(); //enumerate for all tenants
                Assert.Equal(28u, GetNext(en).Age);
                Assert.Equal(29u, GetNext(en).Age);
                Assert.Equal(128u, GetNext(en).Age);
                Assert.Equal(129u, GetNext(en).Age);
                Assert.False(en.MoveNext());

                var orderedById = personTable.ListById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending));
                ulong id;
                Assert.True(orderedById.NextKey(out id));
                Assert.Equal(2ul, id);
                Assert.True(orderedById.NextKey(out id));
                Assert.Equal(3ul, id);
                Assert.False(orderedById.NextKey(out id));

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

            [SecondaryKey("Look")]
            public Dictionary<int, int> Lookup { get; set; }
            public IDictionary<int, int> UnusedDictionary { get; set; }  //test skiping with ctx

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
                jobTable.Insert(new Job { Id = 11, Name = "Code", Priority = 1,
                                          Lookup = new Dictionary<int, int> { { 1, 2 } } });
                jobTable.Insert(new Job { Id = 22, Name = "Sleep", Priority = 2 });
                jobTable.Insert(new Job { Id = 33, Name = "Bicycle", Priority = 1 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var jobTable = creator(tr);
                var en = jobTable.ListByPrioritizedName(2,
                    new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending));
                var j = GetNext(en);
                Assert.Equal("Sleep", j.Name);
                Assert.False(en.MoveNext());
            }
            using (var tr = _db.StartTransaction())
            {
                var jobTable = creator(tr);
                var job = jobTable.FindByNameOrDefault("Dude");
                Assert.Null(job);
                job = jobTable.FindByNameOrDefault("Code");
                Assert.Equal(11u, job.Id);
                Assert.True(jobTable.RemoveById(11));
                Assert.Null(jobTable.FindByNameOrDefault("Code"));
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

        public class WronglyDefined
        {
            [PrimaryKey(1)]
            [SecondaryKey("Id")]
            public ulong Id { get; set; }

            public string Name { get; set; }
        }

        public interface IWronglyDefined
        {
            void Insert(WronglyDefined room);
        }

        [Fact]
        public void NameIdIsReservedAndCannotBeUsedForSecondaryKeyName()
        {
            using (var tr = _db.StartTransaction())
            {
                Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefined>("No"));
            }
        }

        public class Room
        {
            [PrimaryKey(1)]
            [SecondaryKey("CompanyId")]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            public ulong Id { get; set; }

            public string Name { get; set; }
        }

        public interface IRoomTable
        {
            void Insert(Room room);
            IEnumerator<Room> ListByCompanyId(AdvancedEnumeratorParam<ulong> param);
            IOrderedDictionaryEnumerator<ulong, Room> ListById(AdvancedEnumeratorParam<ulong> param);
            IOrderedDictionaryEnumerator<ulong, Room> ListById(ulong companyId, AdvancedEnumeratorParam<ulong> param);
        }

        [Fact]
        public void SecondaryKeyCanBeDefinedOnSameFieldAsPrimaryKey()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IRoomTable>("Room");

                var rooms = creator(tr);
                rooms.Insert(new Room { CompanyId = 1, Id = 10, Name = "First 1" });
                rooms.Insert(new Room { CompanyId = 1, Id = 20, Name = "Second 1" });
                rooms.Insert(new Room { CompanyId = 2, Id = 30, Name = "First 2" });

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

                oen = rooms.ListById(2ul, new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending));
                Assert.True(oen.NextKey(out key));
                Assert.Equal(30ul, key);
                Assert.False(oen.NextKey(out key));
            }
        }

        public class Document
        {
            [PrimaryKey(1)]
            [SecondaryKey("CompanyId")]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            public ulong Id { get; set; }

            public string Name { get; set; }

            [SecondaryKey("DocumentType")]
            public uint DocumentType { get; set; }

            public DateTime CreatedDate { get; set; }
        }

        public interface IDocumentTable
        {
            void Insert(Document item);
            void Update(Document item);
            Document FindById(ulong companyId, ulong id);
            Document FindByIdOrDefault(ulong companyId, ulong id);
            IEnumerator<Document> ListByDocumentType(AdvancedEnumeratorParam<uint> param);
        }

        [Fact]
        public void SecondaryKeyOnBothPrimaryKeyAndNormalFieldWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.InitRelation<IDocumentTable>("Doc");
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IDocumentTable>("Doc");
                var docTable = creator(tr);
                docTable.Insert(new Document { CompanyId = 1, Id = 2, DocumentType = 3, CreatedDate = DateTime.UtcNow });
                var en = docTable.ListByDocumentType(new AdvancedEnumeratorParam<uint>(EnumerationOrder.Ascending));
                Assert.True(en.MoveNext());
                Assert.Equal(2u, en.Current.Id);
            }
        }

        public interface IWronglyDefinedUnknownMethod
        {
            void Insert(Person room);
            void Delete(Person room);
        }

        [Fact]
        public void ReportsProblemAboutUsageOfUnknownMethod()
        {
            using (var tr = _db.StartTransaction())
            {
                var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefinedUnknownMethod>("No"));
                Assert.True(ex.Message.Contains("Delete"));
                Assert.True(ex.Message.Contains("not supported"));
            }
        }

        public interface IWronglyDefinedWrongReturnType
        {
            void Insert(Person room);
            void Upsert(Person room);
        }

        [Fact]
        public void ReportsProblemAboutWrongReturnType()
        {
            using (var tr = _db.StartTransaction())
            {
                var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefinedWrongReturnType>("No"));
                Assert.True(ex.Message.Contains("Upsert"));
                Assert.True(ex.Message.Contains("return type"));
            }
        }

        public interface IWronglyDefinedWrongParamCount
        {
            void Insert(Person room);
            bool Upsert();
        }

        [Fact]
        public void ReportsProblemAboutWrongParamCount()
        {
            using (var tr = _db.StartTransaction())
            {
                var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefinedWrongParamCount>("No"));
                Assert.True(ex.Message.Contains("Upsert"));
                Assert.True(ex.Message.Contains("parameters count"));
            }
        }

        public interface IWronglyDefinedWrongParamType
        {
            void Insert(Person room);
            bool Upsert(PersonSimple room);
        }

        [Fact]
        public void ReportsProblemAboutWrongParamType()
        {
            using (var tr = _db.StartTransaction())
            {
                var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefinedWrongParamType>("No"));
                Assert.True(ex.Message.Contains("Upsert"));
                Assert.True(ex.Message.Contains("Person"));
            }
        }

        public class UserNotice
        {
            [PrimaryKey(1)]
            public ulong UserId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("NoticeId")]
            public ulong NoticeId { get; set; }
        }

        public interface IUserNoticeTable
        {
            void Insert(UserNotice un);
            IEnumerator<UserNotice> ListByNoticeId(AdvancedEnumeratorParam<ulong> noticeId);
        }

        [Fact]
        public void UserNoticeWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IUserNoticeTable>("UserNotice");
                var table = creator(tr);
                table.Insert(new UserNotice { UserId = 1, NoticeId = 2 });
                var en = table.ListByNoticeId(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                    1, KeyProposition.Excluded, 3, KeyProposition.Excluded));
                Assert.True(en.MoveNext());
                Assert.Equal(2u, en.Current.NoticeId);
                tr.Commit();
            }
            ReopenDb();
            var db = (ObjectDB)_db;
            using (var tr = _db.StartTransaction())
            {
                var relationInfo = db.RelationsInfo.CreateByName((IInternalObjectDBTransaction)tr, "UserNotice",
                    typeof(IUserNoticeTable));
                Assert.Equal(1u, relationInfo.ClientTypeVersion);
            }
        }

        public class File
        {
            [PrimaryKey]
            public ulong Id { get; set; }

            public IIndirect<RawData> Data { get; set; }
        }

        public class RawData
        {
            public byte[] Data { get; set; }
            public IDictionary<ulong, ulong> Edges { get; set; }
        }

        public interface IHddRelation
        {
            void Insert(File file);
            void RemoveById(ulong id);
            File FindById(ulong id);
        }

        [Fact]
        public void IIndirectIsProperlyLoadedAfterDbReopen()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IHddRelation>("HddRelation");
                var files = creator(tr);
                var file = new File
                {
                    Id = 1,
                    Data = new DBIndirect<RawData>(new RawData
                    {
                        Data = new byte[] { 1, 2, 3 },
                        Edges = new Dictionary<ulong, ulong> { [10] = 20 }
                    })
                };
                files.Insert(file);
                tr.Commit();
            }
            ReopenDb();

            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IHddRelation>("HddRelation");
                var files = creator(tr);
                var file = files.FindById(1);
                Assert.NotNull(file);
                Assert.NotNull(file.Data.Value);
                Assert.Equal(file.Data.Value.Data, new byte[] { 1, 2, 3 });
                tr.Commit();
            }
        }

        public interface IRoomTable2
        {
            ulong CompanyId { get; set; }
            void Insert(Room room);
            IEnumerator<Room> ListById(AdvancedEnumeratorParam<ulong> param);
            void RemoveById(ulong id);
            IEnumerator<Room> GetEnumerator();
        }

        [Fact]
        public void EnumerateAndDeleteTest()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IRoomTable2>("Room");

                var rooms = creator(tr);
                rooms.CompanyId = 1;
                rooms.Insert(new Room { Id = 10, Name = "First 1" });
                rooms.Insert(new Room { Id = 20, Name = "Second 1" });

                var en = rooms.GetEnumerator();
                var oen = rooms.ListById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending));
                Assert.True(oen.MoveNext());
                rooms.RemoveById(oen.Current.Id);
                var ex = Assert.Throws<InvalidOperationException>(() => oen.MoveNext());
                Assert.True(ex.Message.Contains("modified"));
                var ex2 = Assert.Throws<InvalidOperationException>(() => en.MoveNext());
                Assert.True(ex2.Message.Contains("modified"));
            }
        }

        public class PermutationOfKeys
        {
            [SecondaryKey("Sec", Order = 1)]
            public string A0 { get; set; }

            [PrimaryKey(1)]
            [SecondaryKey("Sec", Order = 2)]
            public string A { get; set; }

            [SecondaryKey("Sec", Order = 3)]
            public string A1 { get; set; }

            [SecondaryKey("Sec", Order = 7)]
            public string B0 { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("Sec", Order = 8)]
            public string B { get; set; }

            [SecondaryKey("Sec", Order = 9)]
            public string B1 { get; set; }

            [SecondaryKey("Sec", Order = 6)]
            public string C0 { get; set; }

            [PrimaryKey(3)]
            public string C { get; set; }

            [PrimaryKey(4)]
            [SecondaryKey("Sec", Order = 4)]
            public string D { get; set; }

            [SecondaryKey("Sec", Order = 5)]
            public string D1 { get; set; }

            [SecondaryKey("Sec", Order = 10)]
            public string E0 { get; set; }

            [PrimaryKey(5)]
            [SecondaryKey("Sec", Order = 11)]
            public string E { get; set; }

            [SecondaryKey("Sec", Order = 12)]
            public string E1 { get; set; }
        }
        //Sec: A0, A, A1, D, D1, C0, B0, B, B1, E0, E, E1

        public interface IPermutationOfKeysTable
        {
            void Insert(PermutationOfKeys per);
            IEnumerator<PermutationOfKeys> ListBySec(AdvancedEnumeratorParam<string> a0);
            IEnumerator<PermutationOfKeys> ListBySec(string a0, AdvancedEnumeratorParam<string> a);
            IEnumerator<PermutationOfKeys> ListBySec(string a0, string a, string a1, string d, string d1, AdvancedEnumeratorParam<string> c0);
        }

        [Fact]
        public void SecondaryKeyCanContainsPrimaryKeyFieldsInAnyOrder()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPermutationOfKeysTable>("Permutation");
                var table = creator(tr);
                table.Insert(new PermutationOfKeys
                {
                    A0 = "a",
                    A = "aa",
                    A1 = "aaa",
                    B0 = "b",
                    B = "bb",
                    B1 = "bbb",
                    C0 = "c",
                    C = "cc",
                    D = "dd",
                    D1 = "ddd",
                    E0 = "e",
                    E = "ee",
                    E1 = "eee"
                });
                var en = table.ListBySec(new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "a", KeyProposition.Excluded, "b", KeyProposition.Excluded));
                Assert.True(en.MoveNext());
                Assert.Equal("aa", en.Current.A);
                en = table.ListBySec("a", new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "a", KeyProposition.Excluded, "b", KeyProposition.Excluded));
                Assert.True(en.MoveNext());
                Assert.Equal("bb", en.Current.B);
                en = table.ListBySec("a", "aa", "aaa", "dd", "ddd", new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "c", KeyProposition.Excluded, "d", KeyProposition.Included));
                Assert.True(en.MoveNext());
                Assert.Equal("eee", en.Current.E1);
                tr.Commit();
            }
            ReopenDb();
            var db = (ObjectDB)_db;
            using (var tr = _db.StartTransaction())
            {
                var relationInfo = db.RelationsInfo.CreateByName((IInternalObjectDBTransaction)tr, "Permutation",
                    typeof(IPermutationOfKeysTable));
                Assert.Equal(1u, relationInfo.ClientTypeVersion);
            }
        }

        [Fact]
        public void ModificationCheckIsNotConfusedByOtherTransaction()
        {
            Func<IObjectDBTransaction, IRoomTable2> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IRoomTable2>("Room");

                var rooms = creator(tr);
                rooms.CompanyId = 1;
                rooms.Insert(new Room { Id = 10, Name = "First 1" });
                rooms.Insert(new Room { Id = 20, Name = "Second 1" });
                tr.Commit();
            }

            var roTr = _db.StartReadOnlyTransaction();
            var roTable = creator(roTr);
            var en = roTable.GetEnumerator();
            Assert.True(en.MoveNext());

            using (var tr = _db.StartTransaction())
            {
                var rooms = creator(tr);
                rooms.CompanyId = 1;
                rooms.Insert(new Room { Id = 30, Name = "First 1" });
                tr.Commit();
            }

            roTable.CompanyId = 1;
            roTable.ListById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending, 0, KeyProposition.Excluded,
                100, KeyProposition.Excluded));
            Assert.True(en.MoveNext());
            Assert.Equal(20ul, en.Current.Id);
            roTr.Dispose();
        }

        public interface IPersonTableNamePrefixSearch
        {
            ulong TenantId { get; set; }
            void Insert(Person person);
            //ListBy secondary key (only for active tenant)
            IOrderedDictionaryEnumerator<string, Person> ListByName(AdvancedEnumeratorParam<string> param);
        }

        [Fact]
        public void StringPrefixExample()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPersonTableNamePrefixSearch>("PersonStringPrefix");
                var personTable = creator(tr);

                personTable.TenantId = 1;
                personTable.Insert(new Person { Id = 2, Name = "Cecil" });
                personTable.Insert(new Person { Id = 3, Name = "Boris" });
                personTable.Insert(new Person { Id = 4, Name = "Alena" });
                personTable.Insert(new Person { Id = 5, Name = "Bob" });
                personTable.Insert(new Person { Id = 6, Name = "B" });
                personTable.Insert(new Person { Id = 7, Name = "C" });

                var orderedEnumerator = personTable.ListByName(new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "B", KeyProposition.Included, "C", KeyProposition.Excluded));
                Assert.Equal(3u, orderedEnumerator.Count);

                string name;
                Assert.True(orderedEnumerator.NextKey(out name));
                Assert.Equal("B", name);
                Assert.True(orderedEnumerator.NextKey(out name));
                Assert.Equal("Bob", name);
                Assert.True(orderedEnumerator.NextKey(out name));
                Assert.Equal("Boris", name);
                Assert.False(orderedEnumerator.NextKey(out name));
            }
        }

        public interface IPersonSimpleListTable : IReadOnlyCollection<PersonSimple>
        {
            ulong TenantId { get; set; }
            void Insert(PersonSimple person);
            IOrderedDictionaryEnumerator<string, PersonSimple> ListById(AdvancedEnumeratorParam<string> param);
            IEnumerator<PersonSimple> FindById();
            bool RemoveById(string email);
            int RemoveById();
        }

        [Fact]
        public void ListByIdWithApartField()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPersonSimpleListTable>("ListByIdWithApartField");
                var personTable = creator(tr);

                personTable.TenantId = 1;
                personTable.Insert(new PersonSimple { Email = "a@d.cz", Name = "A" });
                personTable.Insert(new PersonSimple { Email = "b@d.cz", Name = "B" });

                var enumerator = personTable.ListById(new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "a", KeyProposition.Included, "c", KeyProposition.Excluded));
                Assert.Equal(2u, enumerator.Count);
                string email;
                Assert.True(enumerator.NextKey(out email));
                Assert.Equal("a@d.cz", email);
                Assert.True(enumerator.NextKey(out email));
                Assert.Equal("b@d.cz", email);
                tr.Commit();
            }
        }

        [Fact]
        public void WorkWithPKPrefixWithApartField()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPersonSimpleListTable>("FindByPKPrefixWithApartField");
                var personTable = creator(tr);

                personTable.TenantId = 13;
                personTable.Insert(new PersonSimple { Email = "a@d.cz", Name = "A" });
                personTable.Insert(new PersonSimple { Email = "b@d.cz", Name = "B" });

                var enumerator = personTable.FindById();
                Assert.True(enumerator.MoveNext());
                Assert.Equal("a@d.cz", enumerator.Current.Email);
                Assert.True(enumerator.MoveNext());
                Assert.False(enumerator.MoveNext());

                personTable.TenantId = 2;
                enumerator = personTable.FindById();
                Assert.False(enumerator.MoveNext());

                personTable.TenantId = 13;
                Assert.True(personTable.RemoveById("a@d.cz"));

                var removedCount = personTable.RemoveById();
                Assert.Equal(1, removedCount);

                Assert.Equal(0, personTable.Count);

                tr.Commit();
            }
        }

        public interface IPersonSimpleFindTable : IReadOnlyCollection<PersonSimple>
        {
            void Insert(PersonSimple person);
            IEnumerator<PersonSimple> FindById(ulong tenantId);
            bool RemoveById(ulong tenantId, string email);
            int RemoveById(ulong tenantId);
        }

        [Fact]
        public void WorkWithPKPrefixWithoutApartField()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPersonSimpleFindTable>("FindByPKPrefix");
                var personTable = creator(tr);

                personTable.Insert(new PersonSimple { TenantId = 13, Email = "a@d.cz", Name = "A" });
                personTable.Insert(new PersonSimple { TenantId = 13, Email = "b@d.cz", Name = "B" });

                var enumerator = personTable.FindById(13);
                Assert.True(enumerator.MoveNext());
                Assert.Equal("a@d.cz", enumerator.Current.Email);
                Assert.True(enumerator.MoveNext());
                Assert.False(enumerator.MoveNext());

                enumerator = personTable.FindById(2);
                Assert.False(enumerator.MoveNext());

                Assert.True(personTable.RemoveById(13, "a@d.cz"));

                var removedCount = personTable.RemoveById(13);
                Assert.Equal(1, removedCount);

                Assert.Equal(0, personTable.Count);
            }
        }

        [Fact]
        public void RelationAssembliesCanBeGarbageCollected()
        {
            var createCount = 3;
            for (var i = 0; i < createCount; i++)
            {
                using (var tr = _db.StartTransaction())
                {
                    var tbl = tr.InitRelation<IPersonSimpleListTable>("TestGC")(tr);
                    tbl.TenantId = (ulong)i;
                    tbl.Insert(new PersonSimple());
                    tr.Commit();
                }
                Assert.True(AppDomain.CurrentDomain.GetAssemblies().Any(a => a.FullName.StartsWith("RelationTestGC")));
                ReopenDb();
            }
            GC.Collect(GC.MaxGeneration);
            GC.WaitForPendingFinalizers();
            int count = AppDomain.CurrentDomain.GetAssemblies().Count(a => a.FullName.StartsWith("RelationTestGC"));
            _output.WriteLine($"Reused {createCount - count} out of {createCount} relation assemblies");
            Assert.True(count < createCount);
        }

        public class ProductionTrackingDaily
        {
            [PrimaryKey(1)]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("ProductionDate")]
            [SecondaryKey("ProductionDateWithCompanyId", IncludePrimaryKeyOrder = 1)]
            public DateTime ProductionDate { get; set; }

            public uint ProductionsCount { get; set; }
        }

        public interface IProductionTrackingDailyTable
        {
            void Insert(ProductionTrackingDaily productionTrackingDaily);
            IEnumerator<ProductionTrackingDaily> FindByProductionDate(DateTime productionDate);
            IEnumerator<ProductionTrackingDaily> ListByProductionDateWithCompanyId(ulong companyId, AdvancedEnumeratorParam<DateTime> productionDate);
        }

        [Fact]
        public void AnotherCombinationOfListAndPkAndSkWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IProductionTrackingDailyTable>("AnotherCombinationOfListAndPkAndSkWorks");
                var table = creator(tr);

                var currentDay = new DateTime(2017, 2, 9, 1, 1, 1, DateTimeKind.Utc);

                table.Insert(new ProductionTrackingDaily { CompanyId = 5, ProductionDate = currentDay, ProductionsCount = 1 });

                var companyProduction = table.FindByProductionDate(currentDay);
                Assert.True(companyProduction.MoveNext());
                Assert.Equal(1u, companyProduction.Current.ProductionsCount);

                var nextDay = currentDay.AddDays(1);
                var dateParam = new AdvancedEnumeratorParam<DateTime>(EnumerationOrder.Ascending, currentDay, KeyProposition.Included,
                    nextDay, KeyProposition.Excluded);
                var en = table.ListByProductionDateWithCompanyId(5, dateParam);
                Assert.True(en.MoveNext());
                Assert.Equal(1u, en.Current.ProductionsCount);

                tr.Commit();
            }
        }

        public interface IProductionInvalidTable
        {
            void Insert(ProductionTrackingDaily productionTrackingDaily);
            IEnumerator<ProductionTrackingDaily> FindByProductionDateWithCompanyId(ulong companyId, AdvancedEnumeratorParam<DateTime> productionDate);
        }

        [Fact]
        public void FindByMetodsChecksParameterTypes()
        {
            using (var tr = _db.StartTransaction())
            {
                var ex = Assert.Throws<BTDBException>(() =>
                tr.InitRelation<IProductionInvalidTable>("FindByMetodsChecksParameterTypes"));
                Assert.Contains("expected 'System.DateTime'", ex.Message);
            }
        }

        public interface IProductionTableWithContains
        {
            void Insert(ProductionTrackingDaily productionTrackingDaily);
            bool Contains(ulong companyId, DateTime productionDate);
        }

        [Fact]
        public void ContainMethodWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IProductionTableWithContains>("ContainMethodWorks");
                var table = creator(tr);

                var currentDay = new DateTime(2017, 2, 9, 1, 1, 1, DateTimeKind.Utc);
                Assert.False(table.Contains(5, currentDay));
                table.Insert(new ProductionTrackingDaily { CompanyId = 5, ProductionDate = currentDay, ProductionsCount = 1 });
                Assert.True(table.Contains(5, currentDay));
            }
        }

        public class IdentityUser
        {
            [PrimaryKey(1)]
            public ulong CompanyId { get; set; }
            [PrimaryKey(2)]
            public ulong ApplicationId { get; set; }
            [PrimaryKey(3)]
            public string IdentityUserId { get; set; }

            [SecondaryKey("NormalizedUserName", IncludePrimaryKeyOrder = 2)]
            public string NormalizedUserName { get; set; }
        }

        public interface IIdentityUserTable
        {
            ulong CompanyId { get; set; }
            ulong ApplicationId { get; set; }
            void Insert(IdentityUser user);
            bool RemoveById(string identityUserId);
            IdentityUser FindByNormalizedUserNameOrDefault(string normalizedUserName);
            IEnumerator<IdentityUser> FindById();
            IOrderedDictionaryEnumerator<string, IdentityUser> ListByNormalizedUserName(AdvancedEnumeratorParam<string> param);
        }

        [Fact]
        public void SecondaryKeyWithApartFieldsWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IIdentityUserTable>("SpecificIdentityUserWorks");
                var table = creator(tr);
                var normalizedUserName = "n5";

                table.ApplicationId = 5;
                table.CompanyId = 7;

                table.Insert(new IdentityUser
                {
                    IdentityUserId = "i",
                    NormalizedUserName = normalizedUserName
                });

                var user = table.FindByNormalizedUserNameOrDefault(normalizedUserName);
                Assert.NotNull(user);
                Assert.Equal(normalizedUserName, user.NormalizedUserName);
                Assert.True(table.RemoveById("i"));
            }
        }

        [Fact]
        public void AccidentalAccessToUninitializedCurrentDoesNotMoveIterator()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IIdentityUserTable>("Enumerating");
                var table = creator(tr);

                table.ApplicationId = 5;
                table.CompanyId = 7;

                table.Insert(new IdentityUser { IdentityUserId = "i", NormalizedUserName = "a" });
                table.Insert(new IdentityUser { IdentityUserId = "ii", NormalizedUserName = "b" });

                var userRoleTableEnumerator = table.FindById();
                Assert.Throws<BTDBException>(() => userRoleTableEnumerator.Current);
                var counter = 0;
                while (userRoleTableEnumerator.MoveNext())
                    counter++;
                Assert.Equal(2, counter);

                var advancedEnumerator = table.ListByNormalizedUserName(new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending));
                Assert.Throws<BTDBException>(() => advancedEnumerator.CurrentValue);
                string key;
                Assert.True(advancedEnumerator.NextKey(out key));
                Assert.True(advancedEnumerator.NextKey(out key));
                Assert.Equal("b", key);
                Assert.False(advancedEnumerator.NextKey(out key));

                advancedEnumerator.Position = 1;
                Assert.Equal("ii", advancedEnumerator.CurrentValue.IdentityUserId);
                advancedEnumerator.Position = 0;
                Assert.Equal("i", advancedEnumerator.CurrentValue.IdentityUserId);
            }
        }

        public class EducatedPerson : PersonSimple
        {
            public string Degree { get; set; }
        }

        class TraceListenerCountingFails : TraceListener
        {
            public int FailCount { get; set; }

            public override void Fail(string message, string detailMessage)
            {
                FailCount++;
            }

            public override void Write(string message)
            {
            }

            public override void WriteLine(string message)
            {
            }
        }


        [RunnableInDebugOnly("Testing debug assertions only in debug")]
        public void ProgrammerIsWarnedWhenWorkingWithDerivedType()
        {
            var failCountingListener = new TraceListenerCountingFails();
            var listenersBackup = new TraceListener[Trace.Listeners.Count];
            for (int i = 0; i < Trace.Listeners.Count; i++)
                listenersBackup[i] = Trace.Listeners[i];
            Trace.Listeners.Clear();
            Trace.Listeners.Insert(0, failCountingListener);

            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<ISimplePersonTable>("PersonSimple");
                var table = creator(tr);
                table.Insert(new PersonSimple { Name = "Lubos", Email = "a@b.cz" });
                Assert.Equal(0, failCountingListener.FailCount);
                var educatedPerson = new EducatedPerson { Degree = "Dr.", Name = "Vostep", Email = "dr@les.cz" };
                table.Insert(educatedPerson);
                Assert.Equal(1, failCountingListener.FailCount);
                table.Upsert(educatedPerson);
                Assert.Equal(2, failCountingListener.FailCount);
                educatedPerson.Email = "a@b.cz";
                table.Update(educatedPerson);
                Assert.Equal(3, failCountingListener.FailCount);
                tr.Commit();
            }

            Trace.Listeners.Clear();
            Trace.Listeners.AddRange(listenersBackup);
        }


        public class Application
        {
            [PrimaryKey(1)]
            public ulong CompanyId { get; set; }
            [PrimaryKey(0)]
            public ulong ApplicationId { get; set; }
            public string Name { get; set; }
        }

        public interface IApplicationOutOfOrder
        {
            ulong CompanyId { get; set; }
            void Insert(Application user);
            IEnumerator<Application> FindById(); //ambiguous whether prefix contains apart field CompanyId
        }

        public interface IApplicationOutOfOrderWorking
        {
            ulong CompanyId { get; set; }
            void Insert(Application user);
            IEnumerator<Application> FindById(ulong applicationId);
        }

        [Fact]
        public void RefuseOutOfOrderPrefixSearch()
        {
            using (var tr = _db.StartTransaction())
            {
                var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IApplicationOutOfOrder>("OutOfOrderPk"));
                Assert.Contains("part of prefix", ex.Message);
                Assert.Contains("FindById", ex.Message);
            }

            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IApplicationOutOfOrderWorking>("OutOfOrderPkWorking");
                var table = creator(tr);
                table.CompanyId = 1;
                table.Insert(new Application { Name = "A1", ApplicationId = 10 });
                table.CompanyId = 2;
                table.Insert(new Application { Name = "A2", ApplicationId = 10 });

                var en = table.FindById(10);
                Assert.True(en.MoveNext());
                Assert.Equal("A2", en.Current.Name);
                Assert.False(en.MoveNext());

                table.CompanyId = 1;
                en = table.FindById(10);
                Assert.True(en.MoveNext());
                Assert.Equal("A1", en.Current.Name);
                Assert.False(en.MoveNext());
            }
        }
    }
}
