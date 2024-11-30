using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.EventStore2Layer;
using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDBTest;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest
{
    public class ObjectDbTableTest : ObjectDbTestBase
    {
        public ObjectDbTableTest(ITestOutputHelper output) : base(output)
        {
        }

        public class PersonSimple : IEquatable<PersonSimple>
        {
            [PrimaryKey(1)] public ulong TenantId { get; set; }

            [PrimaryKey(2)] public string Email { get; set; }

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

        public interface IPersonSimpleTableWithJustInsert : IRelation<PersonSimple>
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
                using var en = personSimpleTable.GetEnumerator();
                en.MoveNext();
                Assert.Equal("Boris", en.Current.Name);
                tr.Commit();
            }
        }

        [Fact]
        public void CannotInsertSameKeyTwice()
        {
            using var tr = _db.StartTransaction();
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
            Assert.Contains("duplicate", ex.Message);
            tr.Commit();
        }

        public interface IPersonSimpleTableWithInsert : IRelation<PersonSimple>
        {
            bool Insert(PersonSimple person);
            PersonSimple FindById(ulong tenantId, string email);
        }

        [Fact]
        public void TryInsertWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IPersonSimpleTableWithInsert>("TryInsertWorks");
            var personSimpleTable = creator(tr);
            Assert.True(personSimpleTable.Insert(new PersonSimple
                { TenantId = 1, Email = "nospam@nospam.cz", Name = "A" }));
            Assert.False(personSimpleTable.Insert(new PersonSimple
                { TenantId = 1, Email = "nospam@nospam.cz", Name = "B" }));
            Assert.Equal("A", personSimpleTable.FindById(1, "nospam@nospam.cz").Name);
            tr.Commit();
        }

        public interface ISimplePersonTable : IRelation<PersonSimple>
        {
            void Insert(PersonSimple person);

            // Upsert = Insert or Update - return true if inserted (already defined in IRelation)
            // bool Upsert(PersonSimple person);

            // Update will throw if does not exist
            void Update(PersonSimple person);
            bool ShallowUpsert(PersonSimple person);
            void ShallowUpdate(PersonSimple person);

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
                using var enumerator = personSimpleTable.GetEnumerator();
                Assert.True(enumerator.MoveNext());
                var person = enumerator.Current;
                Assert.Equal(personBoris, person);
                Assert.True(enumerator.MoveNext());
                person = enumerator.Current;
                Assert.Equal(personLubos, person);
                Assert.False(enumerator.MoveNext(), "Only one Person should be evaluated");
            }
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
                var p = personSimpleTable.First();
                Assert.Equal("Lubos", p.Name);
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var p = personSimpleTable.First();
                Assert.Equal("Lubos", p.Name);
            }
        }

        [Fact]
        public void ShallowUpsertWorks()
        {
            var person = new PersonSimple { TenantId = 1, Email = "nospam@nospam.cz", Name = "Boris" };
            Func<IObjectDBTransaction, ISimplePersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTable>("Person");
                var personSimpleTable = creator(tr);
                Assert.True(personSimpleTable.ShallowUpsert(person), "Is newly inserted");
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                person.Name = "Lubos";
                Assert.False(personSimpleTable.ShallowUpsert(person), "Was already there");
                var p = personSimpleTable.First();
                Assert.Equal("Lubos", p.Name);
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var p = personSimpleTable.First();
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
                Ratings = new() { { "Czech", new List<byte> { 1, 2, 1 } } },
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
                var p = personSimpleTable.First();
                Assert.Equal("Lubos", p.Name);
                Assert.Equal(new List<byte> { 3 }, p.Ratings["History"]);
            }
        }

        [Fact]
        public void ShallowUpdateWorks()
        {
            var person = new PersonSimple
            {
                TenantId = 1,
                Email = "nospam@nospam.cz",
                Name = "Boris",
                Ratings = new() { { "Czech", new List<byte> { 1, 2, 1 } } },
            };
            Func<IObjectDBTransaction, ISimplePersonTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ISimplePersonTable>("Person");
                var personSimpleTable = creator(tr);
                Assert.Throws<BTDBException>(() => personSimpleTable.ShallowUpdate(person));
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
                personSimpleTable.ShallowUpdate(person);
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var personSimpleTable = creator(tr);
                var p = personSimpleTable.First();
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
                Assert.Empty(personSimpleTable);
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

        [Fact]
        public void DeleteAllWorks()
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
                tr.DeleteAllData();
                var personSimpleTable = creator(tr);
                Assert.Throws<BTDBException>(() => personSimpleTable.FindById(person.TenantId, person.Email));
                tr.Commit();
            }
        }

        public interface ISimplePersonTableWithVoidRemove : IRelation<PersonSimple>
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
            [PrimaryKey(1)] public ulong TenantId { get; set; }

            [PrimaryKey(2)] public ulong Id { get; set; }

            [SecondaryKey("Age", Order = 2)]
            [SecondaryKey("Name", IncludePrimaryKeyOrder = 1)]
            public string Name { get; set; }

            [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)]
            public uint Age { get; set; }
        }

        public class PersonWithOnlyAge
        {
            public uint Age { get; set; }
        }

        //SK content
        //"Age": TenantId, Age, Name, Id => void
        //"Name": TenantId, Name, Id => void

        public interface IPersonTableComplexFuture : IRelation<Person>
        {
            // Insert will throw if already exists
            void Insert(Person person);

            // Upsert = Insert or Update - return true if inserted (already defined in IRelation<T>)
            // bool Upsert(Person person);

            // Update will throw if does not exist
            void Update(Person person);

            // It will throw if does not exists
            Person FindById(ulong tenantId, ulong id);

            // Will return null if not exists
            Person FindByIdOrDefault(ulong tenantId, ulong id);

            // Find by secondary key, it will throw if it find multiple Persons with that age
            Person FindByAgeOrDefault(ulong tenantId, uint age);

            IEnumerable<Person> FindByAge(ulong tenantId, uint age);

            // Returns true if removed, if returning void it does throw if does not exists
            bool RemoveById(ulong tenantId, ulong id);

            // fills all your iterating needs

            //ListBy primary key (only for active tenant)
            IOrderedDictionaryEnumerator<ulong, Person> ListById(ulong tenantId, AdvancedEnumeratorParam<ulong> param);

            //enumerate all items
            //IEnumerator<Person> GetEnumerator();

            //ListBy{SecondaryKeyName}
            IOrderedDictionaryEnumerator<uint, Person> ListByAge(ulong tenantId, AdvancedEnumeratorParam<uint> param);
            IEnumerable<Person> ListByAge(ulong tenantId, uint age);

            IOrderedDictionaryEnumerator<uint, PersonWithOnlyAge> ListByAgePartial(ulong tenantId,
                AdvancedEnumeratorParam<uint> param);

            IEnumerable<PersonWithOnlyAge> ListByAgePartial(ulong tenantId, uint age);

            // You can replace List by Count and it will return count of list faster if all you need is count
            int CountById(ulong tenantId, AdvancedEnumeratorParam<ulong> param);
            uint CountByAge(ulong tenantId, AdvancedEnumeratorParam<uint> param);
            long CountByAge(ulong tenantId, uint age);

            // You can replace Count by Any and it will return bool if there is any item matching range
            bool AnyById(ulong tenantId, AdvancedEnumeratorParam<ulong> param);
            bool AnyByAge(ulong tenantId, AdvancedEnumeratorParam<uint> param);
            bool AnyByAge(ulong tenantId, uint age);
        }

        public interface INestedEventMetaDataTable : IRelation<NestedEventMetaData>
        {
            void Insert(NestedEventMetaData data);
            IEnumerable<NestedEventMetaData> ListById(AdvancedEnumeratorParam<ulong> param);
        }

        public class NestedEventMetaData
        {
            [PrimaryKey(1)] public ulong SequenceNumber { get; set; }
            public ByteBuffer MetaData { get; set; }
        }

        [Fact]
        public void ListByIdOnEmptyDbWorks()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<INestedEventMetaDataTable>();
            var enumerable = table.ListById(new(
                EnumerationOrder.Ascending,
                0,
                KeyProposition.Included,
                ulong.MaxValue,
                KeyProposition.Ignored
            ));
            // ReSharper disable once PossibleMultipleEnumeration - it is supported
            Assert.Empty(enumerable);
            // ReSharper disable once PossibleMultipleEnumeration - it is supported
            using var enumerator = enumerable.GetEnumerator();
            Assert.False(enumerator.MoveNext());
        }

        [Fact]
        public void AdvancedIteratingWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IPersonTableComplexFuture>("PersonComplex");
            var personTable = creator(tr);

            using var ena2 = personTable.ListByAgePartial(1, new AdvancedEnumeratorParam<uint>(
                EnumerationOrder.Ascending, 29,
                KeyProposition.Included,
                0, KeyProposition.Ignored));
            Assert.False(ena2.NextKey(out var age));

            var firstPerson = new Person { TenantId = 1, Id = 2, Name = "Lubos", Age = 28 };
            personTable.Insert(firstPerson);
            personTable.Insert(new Person { TenantId = 1, Id = 4, Name = "Vladimir", Age = 28 });
            personTable.Insert(new Person { TenantId = 1, Id = 3, Name = "Boris", Age = 29 });

            personTable.Insert(new Person { TenantId = 2, Id = 2, Name = "Lubos", Age = 128 });
            personTable.Insert(new Person { TenantId = 2, Id = 3, Name = "Boris", Age = 129 });

            using var orderedEnumerator =
                personTable.ListByAge(2, new AdvancedEnumeratorParam<uint>(EnumerationOrder.Ascending));
            Assert.Equal(2u, orderedEnumerator.Count);

            Assert.Equal(2u, personTable.CountByAge(2, new AdvancedEnumeratorParam<uint>()));
            Assert.True(personTable.AnyByAge(2, new AdvancedEnumeratorParam<uint>()));

            Assert.True(orderedEnumerator.NextKey(out age));
            Assert.Equal(128u, age);
            Assert.Equal("Lubos", orderedEnumerator.CurrentValue.Name);
            Assert.True(orderedEnumerator.NextKey(out age));
            Assert.Equal(129u, age);

            using var orderedEnumeratorAgeOnly =
                personTable.ListByAgePartial(2, new AdvancedEnumeratorParam<uint>(EnumerationOrder.Ascending));
            Assert.Equal(2u, orderedEnumeratorAgeOnly.Count);

            Assert.True(orderedEnumeratorAgeOnly.NextKey(out age));
            Assert.Equal(128u, age);
            Assert.Equal(128u, orderedEnumeratorAgeOnly.CurrentValue.Age);
            Assert.True(orderedEnumeratorAgeOnly.NextKey(out age));
            Assert.Equal(129u, age);

            Assert.Equal((uint[]) [28u, 29u, 28u, 128u, 129u], personTable.Select(p => p.Age).ToList());

            using var orderedById =
                personTable.ListById(2, new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending));
            Assert.True(orderedById.NextKey(out var id));
            Assert.Equal(2ul, id);
            Assert.True(orderedById.NextKey(out id));
            Assert.Equal(3ul, id);
            Assert.False(orderedById.NextKey(out id));

            Assert.Equal(2, personTable.CountById(2, new AdvancedEnumeratorParam<ulong>()));
            Assert.True(personTable.AnyById(2, new AdvancedEnumeratorParam<ulong>()));
            Assert.False(personTable.AnyById(2, new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending, 10,
                KeyProposition.Included, 0, KeyProposition.Ignored)));

            using var ena = personTable.ListByAge(1, new AdvancedEnumeratorParam<uint>(EnumerationOrder.Ascending, 29,
                KeyProposition.Included,
                29, KeyProposition.Included));
            Assert.True(ena.NextKey(out age));
            Assert.Equal(29u, age);
            Assert.False(ena.NextKey(out _));

            using var ena3 = personTable.ListByAgePartial(1, new AdvancedEnumeratorParam<uint>(
                EnumerationOrder.Ascending, 29,
                KeyProposition.Included,
                29, KeyProposition.Included));
            Assert.True(ena3.NextKey(out age));
            Assert.Equal(29u, age);
            Assert.False(ena3.NextKey(out _));

            Assert.Equal(2, personTable.CountByAge(1, 28));
            Assert.Equal(1, personTable.CountByAge(1, 29));
            Assert.True(personTable.AnyByAge(1, 28));
            Assert.True(personTable.AnyByAge(1, 29));
            Assert.False(personTable.AnyByAge(1, 18));

            Assert.Equal(new[] { 2ul, 4ul }, personTable.ListByAge(1, 28).Select(p => p.Id));
            Assert.Equal(new[] { 28u, 28u }, personTable.ListByAgePartial(1, 28).Select(p => p.Age));
            tr.Commit();
        }

        public interface IPersonTable : IRelation<Person>
        {
            void Insert(Person person);
            void Update(Person person);
            bool RemoveById(ulong tenantId, ulong id);
            Person FindByNameOrDefault(ulong tenantId, string name);
            Person FindByAgeOrDefault(ulong tenantId, uint age);
            IEnumerable<Person> FindByAge(ulong tenantId, uint age);
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
                Assert.Contains("Ambiguous", ex.Message);
                var p = personTable.FindByNameOrDefault(1, "Lubos");
                Assert.Equal(28u, p.Age);

                var enumerator = personTable.FindByAge(1, 28);
                Assert.Equal((string[]) ["Boris", "Lubos"], enumerator.Select(p => p.Name).ToArray());

                Assert.True(personTable.RemoveById(1, 2));
                tr.Commit();
            }
        }

        public class Job
        {
            [PrimaryKey(1)] public ulong Id { get; set; }

            [SecondaryKey("Name")]
            [SecondaryKey("PrioritizedName", Order = 2)]
            public string Name { get; set; }

            [SecondaryKey("Look")] public Dictionary<int, int> Lookup { get; set; }
            public IDictionary<int, int> UnusedDictionary { get; set; } //test skipping with ctx

            [SecondaryKey("PrioritizedName")] public short Priority { get; set; }
        }

        public interface IJobTable : IRelation<Job>
        {
            void Insert(Job person);
            void Update(Job person);
            bool RemoveById(ulong id);

            Job FindByIdOrDefault(ulong id);
            Job FindByNameOrDefault(string name);

            IEnumerable<Job> ListByName(AdvancedEnumeratorParam<string> param);
            IEnumerable<Job> ListByPrioritizedName(short priority, AdvancedEnumeratorParam<string> param);
        }

        [Fact]
        public void SecondaryKeyWorksWithoutIncludedPrimaryWorks()
        {
            Func<IObjectDBTransaction, IJobTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IJobTable>("Job");
                var jobTable = creator(tr);
                jobTable.Insert(new Job
                {
                    Id = 11,
                    Name = "Code",
                    Priority = 1,
                    Lookup = new Dictionary<int, int> { { 1, 2 } }
                });
                jobTable.Insert(new Job { Id = 22, Name = "Sleep", Priority = 2 });
                jobTable.Insert(new Job { Id = 33, Name = "Bicycle", Priority = 1 });
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var jobTable = creator(tr);
                var en = jobTable.ListByPrioritizedName(2,
                    new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending));
                Assert.Equal((string[]) ["Sleep"], en.Select(j => j.Name).ToArray());
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
            using var tr = _db.StartTransaction();
            creator = tr.InitRelation<IJobTable>("Job");
            var jobTable = creator(tr);
            jobTable.Insert(new Job { Id = 11, Name = "Code" });
            jobTable.Insert(new Job { Id = 12, Name = "Code" });
            var ex = Assert.Throws<BTDBException>(() => jobTable.FindByNameOrDefault("Code"));
            Assert.Contains("Ambiguous", ex.Message);
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
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IJobTable>("Job");
            var jobTable = creator(tr);
            jobTable.Insert(new Job { Id = 11, Name = "Code" });
            jobTable.Insert(new Job { Id = 22, Name = "Sleep" });
            jobTable.Insert(new Job { Id = 33, Name = "Bicycle" });

            var en = jobTable.ListByName(new AdvancedEnumeratorParam<string>(EnumerationOrder.Descending));

            Assert.Equal((string[]) ["Sleep", "Code", "Bicycle"], en.Select(j => j.Name).ToArray());

            en = jobTable.ListByName(new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                "B", KeyProposition.Included,
                "C", KeyProposition.Included));
            Assert.Equal((string[]) ["Bicycle"], en.Select(j => j.Name).ToArray());
        }

        [Fact]
        public void NotifyForPossiblyWrongUsageOfExcludedListing()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IJobTable>("Job");
            var jobTable = creator(tr);
            jobTable.Insert(new Job { Id = 11, Name = "Code" });
            Assert.Empty(jobTable.ListByName(new(
                EnumerationOrder.Descending, "Code",
                KeyProposition.Excluded, "Z", KeyProposition.Included)));
            Assert.NotEmpty(jobTable.ListByName(new(EnumerationOrder.Descending, "Code",
                KeyProposition.Included, "Z", KeyProposition.Included)));
        }

        [Fact]
        public void ExcludedListingWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IJobTable>("Job");
            var jobTable = creator(tr);
            jobTable.Insert(new Job { Id = 11, Name = "Code" });
            var en = jobTable.ListByName(new(EnumerationOrder.Descending, "Code",
                KeyProposition.Excluded, "Z", KeyProposition.Included));
            Assert.Empty(en);
        }

        public class User
        {
            [PrimaryKey(1)]
            [SecondaryKey("Email", Order = 1)]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("Email", Order = 2)]
            public string Email { get; set; }
        }

        public interface IUserTable : IRelation<User>
        {
            IEnumerable<User> ListByEmail(ulong companyId, AdvancedEnumeratorParam<string> email);
            IEnumerable<User> ListById(ulong companyId, AdvancedEnumeratorParam<string> email);
            IEnumerable<User> ListById(AdvancedEnumeratorParam<ulong> companyId);
        }

        [Fact]
        public void WhenCompleteKeyNoExcludeListingWarning()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IUserTable>("User");
            var userTable = creator(tr);
            userTable.Upsert(new User { CompanyId = 1, Email = "a@c.cz" });
            userTable.Upsert(new User { CompanyId = 1, Email = "b@c.cz" });
            //secondary key
            var users = userTable.ListByEmail(1, new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                "a@c.cz",
                KeyProposition.Included, "", KeyProposition.Ignored));
            Assert.Equal(2, users.Count());
            users = userTable.ListByEmail(1, new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending, "a@c.cz",
                KeyProposition.Excluded, "", KeyProposition.Ignored));
            Assert.Single(users);
            //primary key
            users = userTable.ListById(1, new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending, "a@c.cz",
                KeyProposition.Included, "", KeyProposition.Ignored));
            Assert.Equal(2, users.Count());
            users = userTable.ListById(1, new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending, "a@c.cz",
                KeyProposition.Excluded, "", KeyProposition.Ignored));
            Assert.Single(users);
        }

        [Fact]
        public void ExcludedPrefixListingByIdWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IUserTable>("User");
            var usersTable = creator(tr);
            usersTable.Upsert(new User { CompanyId = 1, Email = "a@c.cz" });
            Assert.Empty(usersTable.ListById(new AdvancedEnumeratorParam<ulong>(
                EnumerationOrder.Ascending, 1, KeyProposition.Excluded, 0, KeyProposition.Ignored)));
            var users = usersTable.ListById(new AdvancedEnumeratorParam<ulong>(
                EnumerationOrder.Ascending, 1, KeyProposition.Included, 0, KeyProposition.Ignored));
            Assert.Single(users);
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

        public interface ILicTable : IRelation<Lic>
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
            [PrimaryKey(1)] [SecondaryKey("Id")] public ulong Id { get; set; }

            public string Name { get; set; }
        }

        public interface IWronglyDefined : IRelation<WronglyDefined>
        {
            void Insert(WronglyDefined room);
        }

        [Fact]
        public void NameIdIsReservedAndCannotBeUsedForSecondaryKeyName()
        {
            using var tr = _db.StartTransaction();
            Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefined>("No"));
        }

        public class Room
        {
            [PrimaryKey(1)]
            [SecondaryKey("CompanyId")]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)] public ulong Id { get; set; }
            public string Name { get; set; }

            [SecondaryKey("Beds")] public int Beds { get; set; }
        }

        public interface IRoomTable : IRelation<Room>
        {
            void Update(Room room);
            void Insert(Room room);
            bool RemoveById(ulong companyId, ulong id);
            IEnumerable<Room> ListByCompanyId(AdvancedEnumeratorParam<ulong> param);
            IOrderedDictionaryEnumerator<ulong, Room> ListById(AdvancedEnumeratorParam<ulong> param);
            IEnumerable<Room> ListById2(AdvancedEnumeratorParam<ulong> param);
            IOrderedDictionaryEnumerator<ulong, Room> ListById(ulong companyId, AdvancedEnumeratorParam<ulong> param);
            IEnumerable<Room> ListByBeds(AdvancedEnumeratorParam<int> param);
        }

        [Fact]
        public void SecondaryKeyCanBeDefinedOnSameFieldAsPrimaryKey()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IRoomTable>("Room");

            var rooms = creator(tr);
            rooms.Insert(new Room { CompanyId = 1, Id = 10, Name = "First 1" });
            rooms.Insert(new Room { CompanyId = 1, Id = 20, Name = "Second 1" });
            rooms.Insert(new Room { CompanyId = 2, Id = 30, Name = "First 2" });

            var en = rooms.ListByCompanyId(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                1, KeyProposition.Included,
                1 + 1, KeyProposition.Excluded));

            Assert.Equal((string[]) ["First 1", "Second 1"], en.Select(r => r.Name).ToArray());

            using var oen = rooms.ListById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Descending));
            Assert.Equal(3u, oen.Count);
            ulong key;
            Assert.True(oen.NextKey(out key));
            Assert.Equal(2ul, key);
            Assert.True(oen.NextKey(out key));
            Assert.Equal(1ul, key);
            Assert.True(oen.NextKey(out key));
            Assert.Equal(1ul, key);
            Assert.False(oen.NextKey(out key));

            using var oen2 = rooms.ListById(2ul, new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending));
            Assert.True(oen2.NextKey(out key));
            Assert.Equal(30ul, key);
            Assert.False(oen2.NextKey(out key));
        }

        public class Document
        {
            [PrimaryKey(1)]
            [SecondaryKey("CompanyId")]
            public ulong CompanyId { get; set; }

            [PrimaryKey(2)] public ulong Id { get; set; }

            public string Name { get; set; }

            [SecondaryKey("DocumentType")] public uint DocumentType { get; set; }

            public DateTime CreatedDate { get; set; }
        }

        public interface IDocumentTable : IRelation<Document>
        {
            void Insert(Document item);
            void Update(Document item);
            Document FindById(ulong companyId, ulong id);
            Document FindByIdOrDefault(ulong companyId, ulong id);
            IEnumerable<Document> ListByDocumentType(AdvancedEnumeratorParam<uint> param);
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
                docTable.Insert(new Document
                    { CompanyId = 1, Id = 2, DocumentType = 3, CreatedDate = DateTime.UtcNow });
                Assert.Equal(2u,
                    docTable.ListByDocumentType(new AdvancedEnumeratorParam<uint>(EnumerationOrder.Ascending)).First()
                        .Id);
            }
        }

        public interface IWronglyDefinedUnknownMethod : IRelation<Person>
        {
            void Insert(Person room);
            void Delete(Person room);
        }

        [Fact]
        public void ReportsProblemAboutUsageOfUnknownMethod()
        {
            using var tr = _db.StartTransaction();
            var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefinedUnknownMethod>("No"));
            Assert.Contains("Delete", ex.Message);
            Assert.Contains("not supported", ex.Message);
        }

        public interface IWronglyDefinedWrongParamCount : IRelation<Person>
        {
            void Insert();
        }

        [Fact]
        public void ReportsProblemAboutWrongParamCount()
        {
            using var tr = _db.StartTransaction();
            var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefinedWrongParamCount>("No"));
            Assert.Contains("Insert", ex.Message);
            Assert.Contains("parameters count", ex.Message);
        }

        public interface IWronglyDefinedWrongParamType : IRelation<Person>
        {
            void Insert(PersonSimple room);
        }

        [Fact]
        public void ReportsProblemAboutWrongParamType()
        {
            using var tr = _db.StartTransaction();
            var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IWronglyDefinedWrongParamType>("No"));
            Assert.Contains("Insert", ex.Message);
            Assert.Contains("Person", ex.Message);
        }

        public class UserNotice
        {
            [PrimaryKey(1)] public ulong UserId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("NoticeId")]
            public ulong NoticeId { get; set; }
        }

        [PersistedName("UserNotice")]
        public interface IUserNoticeTable : IRelation<UserNotice>
        {
            void Insert(UserNotice un);
            IEnumerable<UserNotice> ListByNoticeId(AdvancedEnumeratorParam<ulong> noticeId);
        }

        [Fact]
        public void UserNoticeWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var table = tr.GetRelation<IUserNoticeTable>();
                table.Insert(new UserNotice { UserId = 1, NoticeId = 2 });
                using var en = table.ListByNoticeId(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                    1, KeyProposition.Included, 3, KeyProposition.Included)).GetEnumerator();
                Assert.True(en.MoveNext());
                Assert.Equal(2u, en.Current!.NoticeId);
                foreach (var row in table)
                {
                    Assert.Equal(2u, row.NoticeId);
                }

                tr.Commit();
            }

            ReopenDb();
            var db = (ObjectDB)_db;
            using (var tr = _db.StartTransaction())
            {
                var relationInfo = ((IRelationDbManipulator)tr.GetRelation<IUserNoticeTable>()).RelationInfo;
                Assert.Equal(1u, relationInfo.ClientTypeVersion);
            }
        }

        public class File
        {
            [PrimaryKey] public ulong Id { get; set; }

            public IIndirect<RawData> Data { get; set; }
        }

        public class RawData
        {
            public byte[] Data { get; set; }
            public IDictionary<ulong, ulong> Edges { get; set; }
        }

        public interface IHddRelation : IRelation<File>
        {
            void Insert(File file);
            int RemoveByIdPartial(int maxCount);
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
            _db.RegisterType(typeof(RawData));
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

        [Fact]
        public void PartialRemove()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IHddRelation>("HddRelationCancell");
            var files = creator(tr);
            var file = new File();
            var itemCount = 100;
            for (var i = 0; i < itemCount; i++)
            {
                file.Id = (ulong)i;
                files.Insert(file);
            }

            var cnt = files.RemoveByIdPartial(50);
            Assert.Equal(50, files.Count);
            Assert.Equal(50, cnt);

            cnt = files.RemoveByIdPartial(100);
            Assert.Equal(0, files.Count);
            Assert.Equal(50, cnt);
        }

        [Fact]
        public void CheckModificationDuringEnumerate()
        {
            Func<IObjectDBTransaction, IRoomTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IRoomTable>("Room");

                var rooms = creator(tr);
                rooms.Insert(new Room { Id = 10, Name = "First 1" });
                rooms.Insert(new Room { Id = 20, Name = "Second 1" });

                tr.Commit();
            }

            IEnumerable<Room> Query(IRoomTable table) => table;
            ModifyDuringEnumerate(creator, Query, table => table.Insert(new Room { Id = 30, Name = "third" }), false);
            ModifyDuringEnumerate(creator, Query, table => table.RemoveById(0, 10), false);
            ModifyDuringEnumerate(creator, Query, table => table.Update(new Room { Id = 10, Name = "First" }), false);
            ModifyDuringEnumerate(creator, Query,
                table => table.Upsert(new Room { Id = 40, Name = "insert new value" }), false);
            ModifyDuringEnumerate(creator, Query, table => table.Upsert(new Room { Id = 10, Name = "update existing" }),
                false);
            ModifyDuringEnumerate(creator, Query,
                table => table.Upsert(new Room { Id = 10, Name = "update existing, change SK", Beds = 4 }), false);
        }

        void ModifyDuringEnumerate(Func<IObjectDBTransaction, IRoomTable> creator,
            Func<IRoomTable, IEnumerable<Room>> query, Action<IRoomTable> modifyAction,
            bool shouldThrow)
        {
            using var tr = _db.StartTransaction();
            var rooms = creator(tr);
            using var en = query(rooms).GetEnumerator();
            var oene = rooms.ListById2(new(EnumerationOrder.Ascending));
            using var oen = oene.GetEnumerator();
            Assert.True(oen.MoveNext());
            modifyAction(rooms);
            if (shouldThrow)
            {
                var ex = Assert.Throws<InvalidOperationException>(() => oen.MoveNext());
                Assert.Contains("modified", ex.Message);
                var ex2 = Assert.Throws<InvalidOperationException>(() => en.MoveNext());
                Assert.Contains("modified", ex2.Message);
            }
            else
            {
                Assert.True(en.MoveNext());
                Assert.True(oen.MoveNext());
            }
        }

        [Fact]
        public void CheckModificationDuringEnumerateBySecondaryKey()
        {
            Func<IObjectDBTransaction, IRoomTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IRoomTable>("Room");

                var rooms = creator(tr);
                rooms.Insert(new Room { Id = 10, Name = "First 1" });
                rooms.Insert(new Room { Id = 20, Name = "Second 1" });

                tr.Commit();
            }

            IEnumerable<Room> Query(IRoomTable table) => table.ListByBeds(AdvancedEnumeratorParam<int>.Instance);
            ModifyDuringEnumerate(creator, Query, table => table.Insert(new Room { Id = 30, Name = "third" }), false);
            ModifyDuringEnumerate(creator, Query, table => table.RemoveById(0, 10), false);
            ModifyDuringEnumerate(creator, Query, table => table.Update(new Room { Id = 10, Name = "First" }), false);
            ModifyDuringEnumerate(creator, Query, table => table.Update(new Room { Id = 10, Name = "First", Beds = 3 }),
                false);
            ModifyDuringEnumerate(creator, Query,
                table => table.Upsert(new Room { Id = 40, Name = "insert new value" }), false);
            ModifyDuringEnumerate(creator, Query,
                table => table.Upsert(new Room { Id = 10, Name = "update existing", Beds = 4 }), false);
        }

        public class PermutationOfKeys
        {
            [SecondaryKey("Sec", Order = 1)] public string A0 { get; set; }

            [PrimaryKey(1)]
            [SecondaryKey("Sec", Order = 2)]
            public string A { get; set; }

            [SecondaryKey("Sec", Order = 3)] public string A1 { get; set; }

            [SecondaryKey("Sec", Order = 7)] public string B0 { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("Sec", Order = 8)]
            public string B { get; set; }

            [SecondaryKey("Sec", Order = 9)] public string B1 { get; set; }

            [SecondaryKey("Sec", Order = 6)] public string C0 { get; set; }

            [PrimaryKey(3)] public string C { get; set; }

            [PrimaryKey(4)]
            [SecondaryKey("Sec", Order = 4)]
            public string D { get; set; }

            [SecondaryKey("Sec", Order = 5)] public string D1 { get; set; }

            [SecondaryKey("Sec", Order = 10)] public string E0 { get; set; }

            [PrimaryKey(5)]
            [SecondaryKey("Sec", Order = 11)]
            public string E { get; set; }

            [SecondaryKey("Sec", Order = 12)] public string E1 { get; set; }
        }
        //Sec: A0, A, A1, D, D1, C0, B0, B, B1, E0, E, E1

        public interface IPermutationOfKeysTable : IRelation<PermutationOfKeys>
        {
            void Insert(PermutationOfKeys per);
            IEnumerable<PermutationOfKeys> ListBySec(AdvancedEnumeratorParam<string> a0);
            IEnumerable<PermutationOfKeys> ListBySec(string a0, AdvancedEnumeratorParam<string> a);

            IEnumerable<PermutationOfKeys> ListBySec(string a0, string a, string a1, string d, string d1,
                AdvancedEnumeratorParam<string> c0);
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
                Assert.Equal("aa", table.ListBySec(new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "a", KeyProposition.Included, "b", KeyProposition.Excluded)).First().A);
                Assert.Equal("bb", table.ListBySec("a", new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending,
                    "a", KeyProposition.Included, "b", KeyProposition.Excluded)).First().B);
                Assert.Equal("eee", table.ListBySec("a", "aa", "aaa", "dd", "ddd", new AdvancedEnumeratorParam<string>(
                    EnumerationOrder.Ascending,
                    "c", KeyProposition.Included, "d", KeyProposition.Included)).First().E1);
                tr.Commit();
            }

            ReopenDb();
            var db = (ObjectDB)_db;
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IPermutationOfKeysTable>("Permutation");
                var table = creator(tr);
                var relationInfo = ((IRelationDbManipulator)table).RelationInfo;
                Assert.Equal(1u, relationInfo.ClientTypeVersion);
            }
        }

        [Fact]
        public void ModificationCheckIsNotConfusedByOtherTransaction()
        {
            Func<IObjectDBTransaction, IRoomTable> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IRoomTable>("Room");

                var rooms = creator(tr);
                rooms.Insert(new Room { Id = 10, Name = "First 1" });
                rooms.Insert(new Room { Id = 20, Name = "Second 1" });
                tr.Commit();
            }

            using var roTr = _db.StartReadOnlyTransaction();
            var roTable = creator(roTr);
            using var en = roTable.GetEnumerator();
            Assert.True(en.MoveNext());

            using (var tr = _db.StartTransaction())
            {
                var rooms = creator(tr);
                rooms.Insert(new Room { Id = 30, Name = "First 1" });
                tr.Commit();
            }

            roTable.ListById(new(EnumerationOrder.Ascending, 0, KeyProposition.Included,
                100, KeyProposition.Included)).Dispose();
            Assert.True(en.MoveNext());
            Assert.Equal(20ul, en.Current!.Id);
        }

        public interface IPersonTableNamePrefixSearch : IRelation<Person>
        {
            void Insert(Person person);

            //ListBy secondary key (only for active tenant)
            IOrderedDictionaryEnumerator<string, Person> ListByName(ulong tenantId,
                AdvancedEnumeratorParam<string> param);
        }

        [Fact]
        public void StringPrefixExample()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IPersonTableNamePrefixSearch>("PersonStringPrefix");
            var personTable = creator(tr);

            personTable.Insert(new Person { Id = 2, Name = "Cecil" });
            personTable.Insert(new Person { Id = 3, Name = "Boris" });
            personTable.Insert(new Person { Id = 4, Name = "Alena" });
            personTable.Insert(new Person { Id = 5, Name = "Bob" });
            personTable.Insert(new Person { Id = 6, Name = "B" });
            personTable.Insert(new Person { Id = 7, Name = "C" });

            using var orderedEnumerator = personTable.ListByName(0, new AdvancedEnumeratorParam<string>(
                EnumerationOrder.Ascending,
                "B", KeyProposition.Included, "C", KeyProposition.Excluded));
            Assert.Equal(3u, orderedEnumerator.Count);

            Assert.True(orderedEnumerator.NextKey(out var name));
            Assert.Equal("B", name);
            Assert.True(orderedEnumerator.NextKey(out name));
            Assert.Equal("Bob", name);
            Assert.True(orderedEnumerator.NextKey(out name));
            Assert.Equal("Boris", name);
            Assert.False(orderedEnumerator.NextKey(out name));
        }

        public interface IPersonSimpleFindTable : IRelation<PersonSimple>
        {
            void Insert(PersonSimple person);
            IEnumerable<PersonSimple> FindById(ulong tenantId);
            bool RemoveById(ulong tenantId, string email);
            int RemoveById(ulong tenantId);
        }

        [Fact]
        public void WorkWithPKPrefix()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IPersonSimpleFindTable>("FindByPKPrefix");
            var personTable = creator(tr);

            personTable.Insert(new PersonSimple { TenantId = 13, Email = "a@d.cz", Name = "A" });
            personTable.Insert(new PersonSimple { TenantId = 13, Email = "b@d.cz", Name = "B" });

            using var enumerator = personTable.FindById(13).GetEnumerator();
            Assert.True(enumerator.MoveNext());
            Assert.Equal("a@d.cz", enumerator.Current.Email);
            Assert.True(enumerator.MoveNext());
            Assert.False(enumerator.MoveNext());

            using var enumerator2 = personTable.FindById(2).GetEnumerator();
            Assert.False(enumerator2.MoveNext());

            Assert.True(personTable.RemoveById(13, "a@d.cz"));

            var removedCount = personTable.RemoveById(13);
            Assert.Equal(1, removedCount);

            Assert.Equal(0, personTable.Count);
        }

        public class ProductionTrackingDaily
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("ProductionDate")]
            [SecondaryKey("ProductionDateWithCompanyId", IncludePrimaryKeyOrder = 1)]
            public DateTime ProductionDate { get; set; }

            public uint ProductionsCount { get; set; }
        }

        public interface IProductionTrackingDailyTable : IRelation<ProductionTrackingDaily>
        {
            void Insert(ProductionTrackingDaily productionTrackingDaily);
            IEnumerable<ProductionTrackingDaily> FindByProductionDate(DateTime productionDate);

            IEnumerable<ProductionTrackingDaily> ListByProductionDateWithCompanyId(ulong companyId,
                AdvancedEnumeratorParam<DateTime> productionDate);

            IEnumerable<ProductionTrackingDaily> ListByProductionDate(AdvancedEnumeratorParam<DateTime> productionDate);
        }

        [Fact]
        public void AnotherCombinationOfListAndPkAndSkWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IProductionTrackingDailyTable>("AnotherCombinationOfListAndPkAndSkWorks");
            var table = creator(tr);

            var currentDay = new DateTime(2017, 2, 9, 1, 1, 1, DateTimeKind.Utc);

            table.Insert(new ProductionTrackingDaily
                { CompanyId = 5, ProductionDate = currentDay, ProductionsCount = 1 });

            using var companyProduction = table.FindByProductionDate(currentDay).GetEnumerator();
            Assert.True(companyProduction.MoveNext());
            Assert.Equal(1u, companyProduction.Current.ProductionsCount);

            var nextDay = currentDay.AddDays(1);
            var dateParam = new AdvancedEnumeratorParam<DateTime>(EnumerationOrder.Ascending, currentDay,
                KeyProposition.Included,
                nextDay, KeyProposition.Excluded);
            using var en = table.ListByProductionDateWithCompanyId(5, dateParam).GetEnumerator();
            Assert.True(en.MoveNext());
            Assert.Equal(1u, en.Current.ProductionsCount);

            using var en2 = table.ListByProductionDateWithCompanyId(5,
                new AdvancedEnumeratorParam<DateTime>(EnumerationOrder.Ascending, DateTime.MinValue,
                    KeyProposition.Included, DateTime.MaxValue, KeyProposition.Excluded)).GetEnumerator();
            Assert.True(en2.MoveNext());
            Assert.Equal(1u, en2.Current.ProductionsCount);

            tr.Commit();
        }

        [Fact]
        public void ListBySecondaryKey_ForDateTimeKey_StartKeyPropositionExcluded_ShouldNotContainThatItem()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IProductionTrackingDailyTable>(
                "ListBySecondaryKey_ForDateTimeKey_StartKeyPropositionExcluded_ShouldNotContainThatItem");
            var table = creator(tr);

            var dateTimeValue = DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);
            // var dateTimeValue = default(DateTime);

            table.Insert(new ProductionTrackingDaily
                { CompanyId = 123, ProductionDate = dateTimeValue, ProductionsCount = 12 });

            using var companyProduction = table.FindByProductionDate(dateTimeValue).GetEnumerator();
            Assert.True(companyProduction.MoveNext());
            Assert.Equal(12u, companyProduction.Current.ProductionsCount);

            var dateParam = new AdvancedEnumeratorParam<DateTime>(EnumerationOrder.Ascending,
                dateTimeValue, KeyProposition.Excluded,
                DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc), KeyProposition.Excluded);
            using var en = table.ListByProductionDate(dateParam).GetEnumerator();
            Assert.False(en.MoveNext());

            tr.Commit();
        }

        public interface IProductionInvalidTable : IRelation<ProductionTrackingDaily>
        {
            void Insert(ProductionTrackingDaily productionTrackingDaily);

            IEnumerator<ProductionTrackingDaily> FindByProductionDateWithCompanyId(ulong companyId,
                AdvancedEnumeratorParam<DateTime> productionDate);
        }

        [Fact]
        public void FindByMethodsChecksParameterTypes()
        {
            using var tr = _db.StartTransaction();
            var ex = Assert.Throws<BTDBException>(() =>
                tr.InitRelation<IProductionInvalidTable>("FindByMethodsChecksParameterTypes"));
            Assert.Contains("expected 'System.DateTime'", ex.Message);
        }

        public interface IProductionTableWithContains : IRelation<ProductionTrackingDaily>
        {
            void Insert(ProductionTrackingDaily productionTrackingDaily);
            bool Contains(ulong companyId, DateTime productionDate);
        }

        [Fact]
        public void ContainMethodWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IProductionTableWithContains>("ContainMethodWorks");
            var table = creator(tr);

            var currentDay = new DateTime(2017, 2, 9, 1, 1, 1, DateTimeKind.Utc);
            Assert.False(table.Contains(5, currentDay));
            table.Insert(new ProductionTrackingDaily
                { CompanyId = 5, ProductionDate = currentDay, ProductionsCount = 1 });
            Assert.True(table.Contains(5, currentDay));
        }

        public class IdentityUser
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }
            [PrimaryKey(2)] public ulong ApplicationId { get; set; }
            [PrimaryKey(3)] public string IdentityUserId { get; set; }

            [SecondaryKey("NormalizedUserName", IncludePrimaryKeyOrder = 2)]
            public string NormalizedUserName { get; set; }
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

        void ProgrammerIsWarnedWhenWorkingWithDerivedType()
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
            [PrimaryKey(1)] public ulong CompanyId { get; set; }
            [PrimaryKey(0)] public ulong ApplicationId { get; set; }
            public string Name { get; set; }
        }

        public interface IWithInsert<T>
        {
            void Insert(T user);
        }

        public interface IPersonInherited : IWithInsert<PersonSimple>, IRelation<PersonSimple>
        {
        }

        [Fact]
        public void SupportInheritedMethods()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IPersonInherited>("IPersonInherited");
            var table = creator(tr);
            table.Insert(new PersonSimple { Email = "anonymous" });
            Assert.Equal(1, table.Count);
        }

        public class SimpleObject
        {
            [PrimaryKey] public ulong Id { get; set; }

            [SecondaryKey("Name")] public string Name { get; set; }
        }

        public interface ISimpleRelation : IRelation<SimpleObject>
        {
            void Insert(SimpleObject obj);

            bool RemoveById(ulong id);

            IEnumerable<SimpleObject> ListByName(string name, AdvancedEnumeratorParam<ulong> param);
            SimpleObject FindByNameOrDefault(string name);
        }

        [Fact]
        public void RemoveFromRelationWhileEnumeratingWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<ISimpleRelation>("ISimpleRelation");
            var personSimpleTable = creator(tr);
            for (var i = 0; i < 100; i++)
            {
                var duty = new SimpleObject() { Id = (ulong)i, Name = "HardCore Code" + i % 5 };
                personSimpleTable.Insert(duty);
            }

            using var enumerator =
                personSimpleTable.ListByName("HardCore Code" + 0, new AdvancedEnumeratorParam<ulong>())
                    .GetEnumerator();
            while (enumerator.MoveNext())
            {
                personSimpleTable.RemoveById(enumerator.Current.Id);
            }

            tr.Commit();
        }

        [Fact]
        public void TransactionProtectionWorksForFindingBySecondaryKey()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<ISimpleRelation>("ISimpleRelation");
            var personSimpleTable = creator(tr);
            personSimpleTable.Insert(new SimpleObject { Id = 1, Name = "code1" });
            personSimpleTable.Insert(new SimpleObject { Id = 2, Name = "code2" });
            var cnt = 0;

            using (var en = personSimpleTable.GetEnumerator())
            {
                while (en.MoveNext())
                {
                    cnt++;
                    var so = en.Current;
                    Assert.Null(personSimpleTable.FindByNameOrDefault("x"));
                }
            }

            Assert.Equal(2, cnt);
            tr.Commit();
        }


        [Fact]
        public void NotCompleteSecondaryKeyIsRecalculatedDuringInit()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<ISimpleRelation>("ISimpleRelation");
                var personSimpleTable = creator(tr);
                for (int i = 0; i < 100; i++)
                {
                    var duty = new SimpleObject { Id = (ulong)i, Name = "HardCore Code" + i % 5 };
                    personSimpleTable.Insert(duty);
                }

                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                //be bad, delete secondary indexes
                var kvTr = ((IInternalObjectDBTransaction)tr).KeyValueDBTransaction;
                using var cursor = kvTr.CreateCursor();
                cursor.EraseAll(ObjectDB.AllRelationsSKPrefix);
                tr.Commit();
            }

            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<ISimpleRelation>("ISimpleRelation");
                var personSimpleTable = creator(tr);
                using var enumerator =
                    personSimpleTable.ListByName("HardCore Code" + 0, new AdvancedEnumeratorParam<ulong>())
                        .GetEnumerator();
                var cnt = 0;
                while (enumerator.MoveNext()) cnt++;
                Assert.Equal(20, cnt);
            }
        }

        public class WithNullableInKey
        {
            [PrimaryKey] public ulong? Key { get; set; }
            public ulong? Value { get; set; }
        }

        public interface IRelationWithNullableInKey : IRelation<WithNullableInKey>
        {
            void Insert(WithNullableInKey obj);
            WithNullableInKey FindById(ulong? key);
        }

        [Fact]
        public void NullableWorksInPrimaryKeys()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IRelationWithNullableInKey>("WithNullableInKey");
            var table = creator(tr);
            table.Insert(new WithNullableInKey { Value = 41u });
            table.Insert(new WithNullableInKey { Key = 1u, Value = 42u });
            var n = table.FindById(new ulong?());
            Assert.Equal(41u, n.Value.Value);
            n = table.FindById(1u);
            Assert.Equal(42u, n.Value.Value);
        }

        public class WithNullableInSecondaryKey
        {
            [PrimaryKey] public ulong Id { get; set; }
            [SecondaryKey("SK")] public ulong? Zip { get; set; }
            public ulong? Value { get; set; }
        }

        public interface IRelationWithNullableInSecondaryKey : IRelation<WithNullableInSecondaryKey>
        {
            void Insert(WithNullableInSecondaryKey obj);
            WithNullableInSecondaryKey FindBySK(ulong? zip);
        }

        [Fact]
        public void NullableWorksInSecondaryKey()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IRelationWithNullableInSecondaryKey>("WithNullableInSecondaryKey");
            var table = creator(tr);
            table.Insert(new WithNullableInSecondaryKey
            {
                Zip = 50346,
                Value = 42
            });
            var v = table.FindBySK(50346);
            Assert.NotNull(v);
            Assert.True(v.Value.HasValue);
            Assert.Equal(42u, v.Value.Value);
        }

        public enum TestEnum
        {
            Item1,
            Item2
        }

        public class WithNullables
        {
            [PrimaryKey] public ulong Id { get; set; }
            public sbyte? SByteField { get; set; }
            public byte? ByteField { get; set; }
            public short? ShortField { get; set; }
            public ushort? UShortField { get; set; }
            public int? IntField { get; set; }
            public uint? UIntField { get; set; }
            public long? LongField { get; set; }
            public ulong? ULongField { get; set; }
            public bool? BoolField { get; set; }
            public double? DoubleField { get; set; }
            public float? FloatField { get; set; }
            public decimal? DecimalField { get; set; }
            public Guid? GuidField { get; set; }
            public DateTime? DateTimeField { get; set; }
            public TimeSpan? TimeSpanField { get; set; }
            public TestEnum? EnumField { get; set; }
            public ByteBuffer? ByteBufferField { get; set; }
        }

        public interface IRelationWithNullables : IRelation<WithNullables>
        {
            void Insert(WithNullables obj);
            WithNullables FindById(ulong id);
        }

        [Fact]
        public void VariousNullableFieldsWorks()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IRelationWithNullables>("WithNullables");
            var table = creator(tr);
            table.Insert(new WithNullables
            {
                Id = 11,
                SByteField = -10,
                ByteField = 10,
                ShortField = -1000,
                UShortField = 1000,
                IntField = -100000,
                UIntField = 100000,
                LongField = -1000000000000,
                ULongField = 1000000000000,
                BoolField = true,
                DoubleField = 12.34,
                FloatField = -12.34f,
                DecimalField = 123456.789m,
                DateTimeField = new DateTime(2000, 1, 1, 12, 34, 56, DateTimeKind.Local),
                TimeSpanField = new TimeSpan(1, 2, 3, 4),
                GuidField = new Guid("39aabab2-9971-4113-9998-a30fc7d5606a"),
                EnumField = TestEnum.Item2,
                ByteBufferField = ByteBuffer.NewAsync(new byte[] { 0, 1, 2 }, 1, 1)
            });

            var o = table.FindById(11);

            Assert.Equal(-10, o.SByteField.Value);
            Assert.Equal(10, o.ByteField.Value);
            Assert.Equal(-1000, o.ShortField.Value);
            Assert.Equal(1000, o.UShortField.Value);
            Assert.Equal(-100000, o.IntField.Value);
            Assert.Equal(100000u, o.UIntField.Value);
            Assert.Equal(-1000000000000, o.LongField.Value);
            Assert.Equal(1000000000000u, o.ULongField.Value);
            Assert.True(o.BoolField.Value);
            Assert.InRange(12.34 - o.DoubleField.Value, -1e-10, 1e10);
            Assert.InRange(-12.34 - o.FloatField.Value, -1e-6, 1e6);
            Assert.Equal(123456.789m, o.DecimalField.Value);
            Assert.Equal(new DateTime(2000, 1, 1, 12, 34, 56, DateTimeKind.Local), o.DateTimeField.Value);
            Assert.Equal(new TimeSpan(1, 2, 3, 4), o.TimeSpanField.Value);
            Assert.Equal(new Guid("39aabab2-9971-4113-9998-a30fc7d5606a"), o.GuidField.Value);
            Assert.Equal(TestEnum.Item2, o.EnumField.Value);
            Assert.Equal(new byte[] { 1 }, o.ByteBufferField.Value.ToByteArray());
        }

        public class CompanyName
        {
            [PrimaryKey(1)]
            [PersistedName("BusinessId")]
            public uint CompanyId { get; set; }

            [PrimaryKey(2)] public string Code { get; set; }
            [PrimaryKey(3)] public ulong Id { get; set; }
            public string Name { get; set; }
        }


        public interface ICompanyName : IRelation<CompanyName>
        {
            [PersistedName("BusinessId")] uint CompanyId { get; set; }
            string Code { get; set; }

            void Insert(CompanyName room);

            CompanyName FindById(ulong Id);
        }

        [DebuggerDisplay("{DebuggerDisplay,nq}")]
        public class InheritedRelation_CompanyItem
        {
            [PrimaryKey] public ulong CompanyId { get; set; }
            [PrimaryKey(1)] public ulong UserId { get; set; }
            [PrimaryKey(3)] public int Value { get; set; }

            [NotStored] string DebuggerDisplay => $"CompanyId={CompanyId}, UserId={UserId}, Value={Value}";

            public override bool Equals(object obj) =>
                obj is InheritedRelation_CompanyItem item &&
                CompanyId == item.CompanyId &&
                UserId == item.UserId;

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = CompanyId.GetHashCode();
                    hashCode = (hashCode * 397) ^ UserId.GetHashCode();
                    hashCode = (hashCode * 397) ^ Value;
                    return hashCode;
                }
            }
        }

        public interface IPersonTableSuperfluousParameter : IRelation<Person>
        {
            void Insert(Person person);
            IEnumerator<Person> ListByName(ulong tenantId, string name, AdvancedEnumeratorParam<int> param);
        }

        [Fact]
        public void ReportErrorForSuperfluousMethodParameter()
        {
            using var tr = _db.StartTransaction();
            var ex = Assert.Throws<BTDBException>(() =>
                tr.InitRelation<IPersonTableSuperfluousParameter>("Superfluous"));
            Assert.Contains("mismatch", ex.Message);
        }

        public interface IPersonTableWrongTypeParameter : IRelation<Person>
        {
            void Insert(Person person);
            IEnumerator<Person> ListByName(ulong tenantId, AdvancedEnumeratorParam<int> param);
        }

        [Fact]
        public void ReportErrorForInvalidMethodParameter()
        {
            using var tr = _db.StartTransaction();
            var ex = Assert.Throws<BTDBException>(() => tr.InitRelation<IPersonTableWrongTypeParameter>("Invalid"));
            Assert.Contains("mismatch", ex.Message);
        }


        [Fact]
        public void PossibleToEnumerateRelations()
        {
            var expected = new List<Type>
            {
                typeof(IPersonTable),
                typeof(IJobTable),
                typeof(ILicTable)
            };

            void InitRelations(IObjectDBTransaction transaction)
            {
                transaction.InitRelation<IPersonTable>("PersonRelation")(transaction);
                transaction.InitRelation<IJobTable>("JobRelation")(transaction);
                transaction.InitRelation<ILicTable>("LicRelation")(transaction);
            }

            using (var tr = _db.StartTransaction())
            {
                InitRelations(tr);
                Assert.Equal(expected, tr.EnumerateRelationTypes());
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(expected, tr.EnumerateRelationTypes());
            }

            ReopenDb();

            using (var tr = _db.StartTransaction())
            {
                InitRelations(tr);
                Assert.Equal(expected, tr.EnumerateRelationTypes());
            }
        }

        public class DeliveryRuleV2
        {
            public DeliveryRuleV2()
            {
                Status = 100;
            }

            [PrimaryKey(1)] public ulong Id { get; set; }
            public int Status { get; set; }
        }

        public interface IDeliveryRuleV2Table : IRelation<DeliveryRuleV2>
        {
            void Insert(DeliveryRuleV2 job);
            void RemoveById(ulong id);
            DeliveryRuleV2 FindById(ulong id);
        }


        public class DeliveryRuleV1
        {
            public DeliveryRuleV1()
            {
                Status = 100;
            }

            public IList<Activity> Activities { get; set; }

            [PrimaryKey(1)] public ulong Id { get; set; }

            public int Status { get; set; }
        }

        public interface IDeliveryRuleTable : IRelation<DeliveryRuleV1>
        {
            void Insert(DeliveryRuleV1 job);
        }

        public class Activity
        {
            public ulong Id { get; set; }
        }

        [Fact]
        public void CanSkipNativeObjectField()
        {
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IDeliveryRuleTable>("DeliveryRule");
                var ruleTable = creator(tr);
                var rule1 = new DeliveryRuleV1 { Id = 11, Status = 300 };
                ruleTable.Insert(rule1);

                var rule2 = new DeliveryRuleV1 { Id = 12, Status = 200, Activities = new[] { new Activity() } };
                ruleTable.Insert(rule2);

                tr.Commit();
            }

            ReopenDb();
            _db.RegisterType(typeof(Activity));
            using (var tr = _db.StartTransaction())
            {
                var creator = tr.InitRelation<IDeliveryRuleV2Table>("DeliveryRule");
                var ruleV2Table = creator(tr);
                ruleV2Table.RemoveById(11);

                Assert.Equal(1, ruleV2Table.Count);

                var j = ruleV2Table.FindById(12);
                Assert.Equal(200, j.Status);

                tr.Commit();
            }
        }

        public class ApplicationV3
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }
            [PrimaryKey(2)] public ulong ApplicationId { get; set; }
            public string Description { get; set; }
            public ulong CreatedUserId { get; set; }
        }

        public interface IApplicationV3Table : IRelation<ApplicationV3>
        {
            IEnumerable<ApplicationV3> ListById(ulong companyId, AdvancedEnumeratorParam<ulong> param);
        }

        [Fact]
        public void DeserializeWellDuringListing()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IApplicationV3Table>("Company");
            var table = creator(tr);
            var app = new ApplicationV3
                { CompanyId = 1, ApplicationId = 100, CreatedUserId = 100, Description = "info" };
            table.Upsert(app);

            var app2 = table.ListById(1, new AdvancedEnumeratorParam<ulong>()).First();
            Assert.Equal(app.Description, app2.Description);
            Assert.Equal(app.ApplicationId, app2.ApplicationId);
            Assert.Equal(app.CreatedUserId, app2.CreatedUserId);
            tr.Commit();
        }

        public class ItemTask
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }

            [PrimaryKey(2)] public DateTime Expiration { get; set; }

            public string Name { get; set; }
        }

        public interface IItemTaskTable : IRelation<ItemTask>
        {
            void Insert(ItemTask room);
            int RemoveById(AdvancedEnumeratorParam<ulong> param);
            int RemoveById(ulong companyId, AdvancedEnumeratorParam<DateTime> param);
        }

        [Fact]
        public void RemoveByIdWithAdvancedEnumerator()
        {
            Func<IObjectDBTransaction, IItemTaskTable> creator;
            var date = new DateTime(2019, 1, 24, 1, 0, 0, DateTimeKind.Utc);

            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<IItemTaskTable>("ItemTask");
                var items = creator(tr);
                items.Insert(new ItemTask { CompanyId = 1, Expiration = date, Name = "1" });
                items.Insert(new ItemTask { CompanyId = 1, Expiration = date + TimeSpan.FromDays(1), Name = "2" });
                items.Insert(new ItemTask { CompanyId = 2, Expiration = date, Name = "1" });
                items.Insert(new ItemTask { CompanyId = 2, Expiration = date + TimeSpan.FromDays(1), Name = "3" });
                items.Insert(new ItemTask { CompanyId = 3, Expiration = date, Name = "1" });
                items.Insert(new ItemTask { CompanyId = 3, Expiration = date + TimeSpan.FromDays(1), Name = "4" });
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var items = creator(tr);
                var cnt = items.RemoveById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                    1, KeyProposition.Included,
                    2, KeyProposition.Excluded));

                Assert.Equal(2, cnt);
                Assert.Equal(4, items.Count);
            }

            using (var tr = _db.StartTransaction())
            {
                var items = creator(tr);
                var cnt = items.RemoveById(new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending,
                    1, KeyProposition.Included,
                    2, KeyProposition.Included));

                Assert.Equal(4, cnt);
                Assert.Equal(2, items.Count);
            }

            using (var tr = _db.StartTransaction())
            {
                var items = creator(tr);
                var cnt = items.RemoveById(1, new AdvancedEnumeratorParam<DateTime>(EnumerationOrder.Ascending,
                    date, KeyProposition.Included,
                    date + TimeSpan.FromDays(1), KeyProposition.Excluded));

                Assert.Equal(1, cnt);
            }
        }

        public class SimpleJob
        {
            [PrimaryKey] public ulong Id { get; set; }
            public IDictionary<int, string> Properties { get; set; }
        }

        public interface ISimpleJobTable : IRelation<SimpleJob>
        {
            void Insert(SimpleJob link);
            SimpleJob FindById(ulong id);
            void ShallowRemoveById(ulong id);
        }


        [Fact]
        public void CanEasilyCopyComplexObjectBetweenRelationsOfSameType()
        {
            Func<IObjectDBTransaction, ISimpleJobTable> creatorToProcess;
            Func<IObjectDBTransaction, ISimpleJobTable> creatorDone;

            using (var tr = _db.StartTransaction())
            {
                creatorToProcess = tr.InitRelation<ISimpleJobTable>("JobsToProcess");
                creatorDone = tr.InitRelation<ISimpleJobTable>("JobsDone");
                var todo = creatorToProcess(tr);

                todo.Insert(new SimpleJob
                    { Id = 1, Properties = new Dictionary<int, string> { [1] = "one", [2] = "two" } });
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var todo = creatorToProcess(tr);
                var done = creatorDone(tr);

                var job = todo.FindById(1);
                todo.ShallowRemoveById(1);

                done.Insert(job);
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var todo = creatorToProcess(tr);
                var done = creatorDone(tr);

                Assert.Equal(0, todo.Count);
                var job = done.FindById(1);
                Assert.Equal(2, job.Properties.Count);
                Assert.Equal("two", job.Properties[2]);
            }
        }

        public class ThreeInts
        {
            [PrimaryKey(1)] public int A { get; set; }
            [PrimaryKey(2)] public int B { get; set; }
            [PrimaryKey(3)] public int C { get; set; }
        }

        public interface IThreeIntsTable : IRelation<ThreeInts>
        {
            void Insert(ThreeInts value);
            int CountById();
            int CountById(int a);
            int CountById(int a, int b);
            int CountById(int a, int b, int c);
            int CountById(int a, AdvancedEnumeratorParam<int> b);
            IEnumerable<ThreeInts> ListById(int a, int b);
        }

        [Fact]
        public void CanUseCountByIdAndSimpleListById()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IThreeIntsTable>("ThreeInts");
            var table = creator(tr);
            table.Insert(new ThreeInts { A = 1, B = 1, C = 1 });
            table.Insert(new ThreeInts { A = 1, B = 1, C = 2 });
            table.Insert(new ThreeInts { A = 1, B = 2, C = 1 });
            table.Insert(new ThreeInts { A = 1, B = 2, C = 2 });
            table.Insert(new ThreeInts { A = 1, B = 2, C = 3 });
            table.Insert(new ThreeInts { A = 1, B = 3, C = 1 });
            Assert.Equal(6, table.CountById());
            Assert.Equal(6, table.CountById(1));
            Assert.Equal(4,
                table.CountById(1,
                    new AdvancedEnumeratorParam<int>(EnumerationOrder.Ascending, 2, KeyProposition.Included, 3,
                        KeyProposition.Included)));
            Assert.Equal(3, table.CountById(1, 2));
            Assert.Equal(1, table.CountById(1, 2, 3));
            Assert.Equal([1, 2, 3], table.ListById(1, 2).Select(o => o.C));
        }

        public class EncryptedStringSecondaryKey
        {
            [PrimaryKey(1)] public ulong A { get; set; }
            [SecondaryKey("B")] public EncryptedString B { get; set; }
        }

        public interface IEncryptedStringSecondaryKey : IRelation<EncryptedStringSecondaryKey>
        {
            void Insert(EncryptedStringSecondaryKey value);
            IEnumerable<EncryptedStringSecondaryKey> FindByB(EncryptedString b);
        }

        [Fact]
        public void CanUseUseEncryptedStringForSecondaryKeyForFindBySecondaryKey()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IEncryptedStringSecondaryKey>("EncryptedStringSecondaryKey");
            var table = creator(tr);

            EncryptedString secondaryKey = "string";
            table.Insert(new EncryptedStringSecondaryKey { A = 1, B = secondaryKey });

            var item = table.FindByB(secondaryKey).First();
            Assert.Equal(1ul, item.A);
            Assert.Equal(secondaryKey, item.B);
        }

        public class ItemWithList
        {
            [PrimaryKey(1)] public ulong Id { get; set; }

            public IList<string> Parts { get; set; }
        }

        public class PartWithSet
        {
            public ISet<string> Parts { get; set; }
        }

        public interface ITableWithListAsSet : IRelation<ItemWithList>
        {
            PartWithSet FindById(ulong id);
        }

        [Fact]
        public void SetDeserializationDeduplicatesList()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<ITableWithListAsSet>("TableWithListAsSet");
            var table = creator(tr);

            table.Upsert(new ItemWithList { Id = 1, Parts = new List<string> { "A", "B", "C", "A", "C" } });

            var set = table.FindById(1);
            Assert.Equal(new[] { "A", "B", "C" }, set.Parts.OrderBy(a => a));
        }


        public class ItemWithOrderedSet
        {
            [PrimaryKey(1)] public long Id { get; set; }
            public IOrderedSet<string> Parts { get; set; }
        }

        public interface ITableWithOrderedSet : IRelation<ItemWithOrderedSet>
        {
            bool RemoveById(long id);

            ItemWithOrderedSet FindById(long id);
        }

        [Fact]
        public void BasicOrderedSet()
        {
            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<ITableWithOrderedSet>("TableWithOrderedSet");
            var table = creator(tr);

            var i = new ItemWithOrderedSet { Id = 1, Parts = new OrderedSet<string> { "A", "B", "C" } };
            table.Upsert(i);
            i = table.FindById(1);
            Assert.Equal(3, i.Parts.Count);
            Assert.True(i.Parts.Add("D"));
            Assert.False(i.Parts.Add("D"));
            Assert.Equal(2,
                i.Parts.RemoveRange(new(EnumerationOrder.Ascending, "B",
                    KeyProposition.Included, "D", KeyProposition.Excluded)));
            Assert.Equal(2, i.Parts.Count);
        }

        [Fact]
        public void AutoRegisterOfRelationWorks()
        {
            {
                using var tr = _db.StartTransaction();
                tr.GetRelation<IPersonTable>();
            }
            {
                using var tr = _db.StartTransaction();
                var table = tr.GetRelation<IPersonTable>();
                table.Upsert(new Person());
            }
        }

        [Fact]
        public void AutoRegistrationCouldBeForbidden()
        {
            _db.AllowAutoRegistrationOfRelations = false;
            using var tr = _db.StartTransaction();
            Assert.Throws<BTDBException>(() => tr.GetRelation<IPersonTable>());
        }

        public interface ICustomRelation : IRelation
        {
            void Hello(int call);
        }

        public class CustomRelation : ICustomRelation
        {
            readonly IObjectDBTransaction _tr;

            public CustomRelation(IObjectDBTransaction tr)
            {
                _tr = tr;
            }

            public Type BtdbInternalGetRelationInterfaceType() => typeof(ICustomRelation);

            public IRelation? BtdbInternalNextInChain { get; set; }

            int _index;

            public void Hello(int call)
            {
                _index++;
                Assert.Equal(call, (int)_tr.GetCommitUlong() + _index);
            }
        }

        [Fact]
        public void AllowsRegisteringOfCustomRelation()
        {
            _db.RegisterCustomRelation(typeof(ICustomRelation), tr => new CustomRelation(tr));
            using var tr = _db.StartTransaction();
            tr.GetRelation<ICustomRelation>().Hello(1);
            tr.GetRelation<ICustomRelation>().Hello(2);
        }

        public interface IAncestor
        {
            [PrimaryKey] int A { get; set; }
        }

        public class AncestorItem1 : IAncestor
        {
            public int A { get; set; }
            public int B { get; set; }
        }

        public interface IAncestorTable1 : ICovariantRelation<AncestorItem1>
        {
            void Insert(AncestorItem1 item);
        }

        [Fact]
        void SupportCovariantRelations()
        {
            {
                using var tr = _db.StartTransaction();
                var t = tr.GetRelation<IAncestorTable1>();
                t.Insert(new AncestorItem1 { A = 1, B = 2 });
                tr.Commit();
            }
            {
                using var tr = _db.StartTransaction();
                // ReSharper disable once SuspiciousTypeConversion.Global - All relations have Upsert even if you don't request it
                var t = (IRelation<AncestorItem1>)tr.GetRelation<IAncestorTable1>();
                Assert.Equal(2, t.First().B);
                t.Upsert(new AncestorItem1 { A = 1, B = 3 });
                Assert.Equal(3, t.First().B);
                tr.Commit();
            }
        }

        public class WithPublicField
        {
            [PrimaryKey] public int A { get; set; }
            public int B;
        }

        public interface IWithPublicFieldTable : IRelation<WithPublicField>
        {
        }

        [Fact]
        void ThrowsOnPublicField()
        {
            using var tr = _db.StartTransaction();
            Assert.Throws<BTDBException>(() => tr.GetRelation<IWithPublicFieldTable>());
        }

        public class WithPublicNotStoredField
        {
            [PrimaryKey] public int A { get; set; }
            [NotStored] public int B;
        }

        public interface IWithPublicNotStoredFieldTable : IRelation<WithPublicNotStoredField>
        {
        }

        [Fact]
        void WorksWithPublicNotStoredField()
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IWithPublicNotStoredFieldTable>();
            t.Upsert(new WithPublicNotStoredField { A = 1, B = 2 });
            Assert.Equal(0, t.First().B);
        }

        public class ContactGroupRelationDb
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }
            [PrimaryKey(2)] public ulong GroupId { get; set; }

            [PrimaryKey(3)]
            [SecondaryKey("ContactId", IncludePrimaryKeyOrder = 1)]
            public ulong ContactId { get; set; }
        }

        public interface IContactGroupRelationTable : IRelation<ContactGroupRelationDb>
        {
            IEnumerable<ContactGroupRelationDb> FindById(ulong companyId, ulong groupId);
            IEnumerable<ContactGroupRelationDb> FindByContactId(ulong companyId, ulong contactId);
            int CountById(ulong companyId, ulong groupId);
            int RemoveById(ulong companyId, ulong groupId);
            int RemoveByContactId(ulong companyId, ulong contactId);
            bool RemoveById(ulong companyId, ulong groupId, ulong contactId);
        }

        [Fact]
        public void RemoveBySecondaryKeyThrowsUnsupported()
        {
            using var tr = _db.StartTransaction();
            Assert.Contains("unsupported",
                Assert.Throws<BTDBException>(() => tr.GetRelation<IContactGroupRelationTable>()).Message);
        }

        public class MyContactGroupRelationDb : IEquatable<MyContactGroupRelationDb>
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }
            [PrimaryKey(2)] public ulong GroupId { get; set; }

            [PrimaryKey(3)] public ulong ContactId { get; set; }

            public bool Equals(MyContactGroupRelationDb? other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return CompanyId == other.CompanyId && GroupId == other.GroupId && ContactId == other.ContactId;
            }

            public override bool Equals(object? obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((MyContactGroupRelationDb)obj);
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(CompanyId, GroupId, ContactId);
            }
        }

        public interface IMyContactGroupRelationTable : IRelation<MyContactGroupRelationDb>
        {
            IEnumerable<MyContactGroupRelationDb> FindById(ulong companyId, ulong groupId);
            bool RemoveById(ulong companyId, ulong groupId, ulong contactId);

            bool RemoveByIdGlobal(ulong groupId, ulong contactId) => RemoveById(0UL, groupId, contactId);

            MyContactGroupRelationDb FindByIdGlobal(ulong groupId) => FindById(1UL, groupId).First();

            string Hello(string name) => $"Hello {name}";
        }

        [Fact]
        public void SupportsDefaultInterfaceMethods()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IMyContactGroupRelationTable>();
            Assert.Equal("Hello Boris", table.Hello("Boris"));

            const ulong groupId = 2UL;
            const ulong contactId = 3UL;

            table.Upsert(new MyContactGroupRelationDb { CompanyId = 1UL, GroupId = groupId, ContactId = contactId });
            Assert.Equal(new MyContactGroupRelationDb { CompanyId = 1UL, ContactId = contactId, GroupId = groupId },
                table.FindByIdGlobal(groupId));
        }

        [Flags]
        public enum BatchType
        {
            Undefined = 0,
            Email = 1,
            Sms = 1 << 1,
            Notification = 1 << 2,
            DocumentUpdate = 1 << 3,
            AppTemplateUpdate = 1 << 4,
            WhatsApp = 1 << 5
        }

        public class BatchOnlyPk
        {
            public ulong CompanyId { get; set; }
            public ulong BatchId { get; set; }
        }

        public class Batch
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }

            [PrimaryKey(2)]
            [SecondaryKey("BatchId")]
            public ulong BatchId { get; set; }

            [SecondaryKey("UploadTime", IncludePrimaryKeyOrder = 1)]
            public BatchType Type { get; set; }

            [SecondaryKey("UploadTime", Order = 2)]
            public DateTime LastSplitUploadTime { get; set; }
        }

        public interface IBatchTable : IRelation<Batch>
        {
            IEnumerable<BatchOnlyPk> FindByIdOnlyPk(ulong companyId);
            uint CountByUploadTime(ulong companyId, BatchType type, AdvancedEnumeratorParam<DateTime> param);
        }

        [Fact]
        public void CountByIdWorks()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IBatchTable>();

            table.Upsert(new Batch()
            {
                CompanyId = 1,
                BatchId = 1,
                Type = BatchType.Email,
                LastSplitUploadTime = DateTime.SpecifyKind(new DateTime(2018, 12, 15), DateTimeKind.Utc)
            });

            table.Upsert(new Batch()
            {
                CompanyId = 1,
                BatchId = 2,
                Type = BatchType.Email,
                LastSplitUploadTime = DateTime.SpecifyKind(new DateTime(2019, 1, 15), DateTimeKind.Utc)
            });

            table.Upsert(new Batch()
            {
                CompanyId = 1,
                BatchId = 3,
                Type = BatchType.Email,
                LastSplitUploadTime = DateTime.SpecifyKind(new DateTime(2019, 1, 25), DateTimeKind.Utc)
            });

            var from = DateTime.SpecifyKind(new DateTime(2019, 1, 1), DateTimeKind.Utc);
            var to = DateTime.SpecifyKind(new DateTime(2019, 2, 1), DateTimeKind.Utc);
            var param = new AdvancedEnumeratorParam<DateTime>(EnumerationOrder.Descending, from,
                KeyProposition.Included, to, KeyProposition.Included);

            Assert.Equal((uint)0, table.CountByUploadTime(1, BatchType.Notification, param));
            Assert.Equal(new[] { 1ul, 2ul, 3ul }, table.FindByIdOnlyPk(1).Select(v => v.BatchId).ToList());
            table.RemoveAll();
            Assert.Equal(0, table.Count);
        }

        [Fact]
        public void ClassesWithSameNameInAnotherNamespaceThrowsBecauseTheyHaveSameMappedName()
        {
            _db.RegisterType(typeof(Imp1.InnerImplementation));
            _db.RegisterType(typeof(Imp2.InnerImplementation));

            using var tr = _db.StartTransaction();
            var creator = tr.InitRelation<IInnerInterfaceTable>("InnerInterface");
            var innerInterfaceTable = creator(tr);
            innerInterfaceTable.Insert(new BaseClass { Id = 1, Inner = new Imp1.InnerImplementation() });
            Assert.Throws<BTDBException>(() =>
            {
                innerInterfaceTable.Insert(new BaseClass { Id = 2, Inner = new Imp2.InnerImplementation() });
            });
            tr.Commit();
        }

        public interface IInnerInterfaceTable : IRelation<BaseClass>
        {
            void Insert(BaseClass person);
        }

        public class BaseClass
        {
            [PrimaryKey(1)] public ulong Id { get; set; }

            public IInnerInterface Inner { get; set; }
        }

        public interface IInnerInterface;

        public class PersonPrivateConstructor
        {
            private PersonPrivateConstructor()
            {
            }

            public PersonPrivateConstructor(ulong tenantId)
            {
                TenantId = tenantId;
                Name = "Test";
            }

            [PrimaryKey(1)] public ulong TenantId { get; set; }

            public string Name { get; set; }
        }

        public interface IPersonPrivateConstructorTable : IRelation<PersonPrivateConstructor>
        {
        }

        [Fact]
        public void ItemsWithPrivateConstructorWorks()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IPersonPrivateConstructorTable>();
            table.Upsert(new(1) { Name = "Boris" });
            Assert.Equal("Boris", table.First().Name);
        }

        public class PersonWoConstructor
        {
            public PersonWoConstructor(ulong tenantId)
            {
                TenantId = tenantId;
                Name = "Test";
            }

            [PrimaryKey(1)] public ulong TenantId { get; set; }

            public string Name { get; set; }
        }

        public interface IPersonWoConstructorTable : IRelation<PersonWoConstructor>
        {
        }

        [Fact]
        public void ItemsWithoutDefaultConstructorWorks()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IPersonWoConstructorTable>();
            table.Upsert(new(1) { Name = "Boris" });
            Assert.Equal("Boris", table.First().Name);
        }

        [Fact]
        public void DisposingTransactionDuringEnumerationThrowsNiceException()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IPersonWoConstructorTable>();
            table.Upsert(new(1) { Name = "Boris" });
            using var enumerator = table.GetEnumerator();
            tr.Commit();
            Assert.Throws<BTDBException>(() => enumerator.MoveNext());
        }

        public class RowWithOrderedSet
        {
            [PrimaryKey(1)] public ulong TenantId { get; set; }

            // DON'T DO THIS IN YOUR CODE !!! Either use IList<T>, List<T> for inline storage or IOrderedSet<T> for externaly stored T
            public OrderedSet<int> Ordered { get; set; }
        }

        public interface IRowWithOrderedSetTable : IRelation<RowWithOrderedSet>
        {
        }

        [Fact]
        public void RowWithOrderedSetWorksButYouShouldUseIOrderedSetInstead()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRowWithOrderedSetTable>();
            table.Upsert(new() { TenantId = 1, Ordered = new() { 3, 5, 4 } });
            Assert.Equal(new[] { 3, 5, 4 }, table.First().Ordered);
        }

        public class UrlWithStatus
        {
            [PrimaryKey(1)] public string Url { get; set; }
            public HttpStatusCode StatusCode { get; set; }
        }

        public interface IUrlWithStatusTable : IRelation<UrlWithStatus>
        {
        }

        [Fact]
        public void StatusCodeAmbiguousEnumCanBeStored()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IUrlWithStatusTable>();
            table.Upsert(new() { Url = "home.com", StatusCode = HttpStatusCode.MultipleChoices });
            Assert.Equal(HttpStatusCode.MultipleChoices, table.First().StatusCode);
            Assert.Equal(HttpStatusCode.Ambiguous, table.First().StatusCode); //also 300
        }

        public class RowWithObject
        {
            [PrimaryKey(1)] public ulong Id { get; set; }
            public object Anything { get; set; }
        }

        public interface IRowWithObjectTable : IRelation<RowWithObject>
        {
        }

        [Fact]
        public void StoringClassIntoObjectWorks()
        {
            _db.RegisterType(typeof(RowWithObject));
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRowWithObjectTable>();
            table.Upsert(new() { Id = 1, Anything = new RowWithObject { Id = 2 } });
            Assert.True(table.First().Anything is RowWithObject);
            Assert.Equal(2ul, ((RowWithObject)table.First().Anything).Id);
        }

        [Fact]
        public void StoringBoxedIntIntoObjectThrows()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRowWithObjectTable>();
            Assert.Throws<InvalidOperationException>(() => table.Upsert(new() { Id = 1, Anything = 666 }));
        }

        [Fact]
        public void StoringStringIntoObjectThrows()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRowWithObjectTable>();
            Assert.Throws<InvalidOperationException>(() => table.Upsert(new() { Id = 1, Anything = "bad" }));
        }

        [Fact]
        public void StoringDelegateIntoObjectThrows()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRowWithObjectTable>();
            Assert.Throws<InvalidOperationException>(() =>
                table.Upsert(new() { Id = 1, Anything = (Func<int>)(() => 1) }));
        }

        public class ItemWithOnSerialize
        {
            [PrimaryKey(1)] public ulong Id { get; set; }
            public string Text { get; set; }

            [OnSerialize]
            void MakeTextUpperCase()
            {
                Text = Text.ToUpperInvariant();
            }

            [OnSerialize]
            public void VerifyIdToBeHigherThan100()
            {
                if (Id < 100) throw new ArgumentOutOfRangeException(nameof(Id));
            }
        }

        public interface IItemWithOnSerializeTable : IRelation<ItemWithOnSerialize>
        {
        }

        [Fact]
        public void UpsertRunningAllOnSerializeMethods()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IItemWithOnSerializeTable>();
            Assert.Throws<ArgumentOutOfRangeException>(() => table.Upsert(new() { Id = 1, Text = "a" }));
            table.Upsert(new() { Id = 1000, Text = "ahoj" });
            Assert.Equal("AHOJ", table.First().Text);
        }

        public class ItemWithOnBeforeRemove
        {
            [PrimaryKey(1)] public ulong Id { get; set; }
            public int Number { get; set; }

            public static ulong CallCounter;

            [OnBeforeRemove]
            void VoidReturningBeforeRemove(IObjectDBTransaction tr, uint add)
            {
                CallCounter += tr.KeyValueDBTransaction.GetCommitUlong() + add;
            }

            // return true to prevent remove
            [OnBeforeRemove]
            public bool PreventRemoveForOddNumbers()
            {
                return Number % 2 == 1;
            }
        }

        public interface IItemWithOnBeforeRemoveTable : IRelation<ItemWithOnBeforeRemove>
        {
            int RemoveById(ulong id);
        }

        [Fact]
        public void SimpleOnBeforeRemoveWorks()
        {
            var builder = new ContainerBuilder();
            builder.RegisterInstance<uint>(0u).Named<uint>("add");
            _container = builder.Build();
            ReopenDb();
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IItemWithOnBeforeRemoveTable>();
            table.Upsert(new() { Id = 1, Number = 1 });
            table.Upsert(new() { Id = 2, Number = 2 });
            ItemWithOnBeforeRemove.CallCounter = 0;
            tr.KeyValueDBTransaction.SetCommitUlong(1);
            Assert.Equal(0, table.RemoveById(1));
            Assert.Equal(1, table.RemoveById(2));
            Assert.Equal(2ul, ItemWithOnBeforeRemove.CallCounter);
            _container = null;
        }

        [Fact]
        public void RemoveAllOnBeforeRemoveWorks()
        {
            var builder = new ContainerBuilder();
            builder.RegisterInstance<uint>(2u).Named<uint>("add");
            _container = builder.Build();
            ReopenDb();
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IItemWithOnBeforeRemoveTable>();
            table.Upsert(new() { Id = 1, Number = 1 });
            table.Upsert(new() { Id = 2, Number = 2 });
            ItemWithOnBeforeRemove.CallCounter = 0;
            tr.KeyValueDBTransaction.SetCommitUlong(1);
            table.RemoveAll();
            Assert.Equal(1, table.Count);
            Assert.Equal(6ul, ItemWithOnBeforeRemove.CallCounter);
            _container = null;
        }

        public class ProcessingPipelineV2
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }
            [PrimaryKey(2)] public ulong Id { get; set; }

            [PrimaryKey(3)]
            [SecondaryKey("StateVersion", Order = 4)]
            public ulong VersionNumber { get; set; } = 1;

            [SecondaryKey("StateVersion", Order = 3, IncludePrimaryKeyOrder = 2)]
            public bool VersionState { get; set; }

            public string? VersionComment { get; set; }

            [OnBeforeRemove]
            void TryRemoveProcessingPipelineData()
            {
                Assert.Equal(1ul, VersionNumber);
            }
        }

        public interface IProcessingPipelineV2Table : IRelation<ProcessingPipelineV2>
        {
            int RemoveById(ulong companyId, ulong id);
        }

        [Fact]
        void PrefixRemoveByIdIsCorrectlyCalledWithOnBeforeRemove()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IProcessingPipelineV2Table>();
            table.Upsert(new ProcessingPipelineV2 { CompanyId = 1, Id = 1, VersionNumber = 1 });
            Assert.Equal(1, table.RemoveById(1, 1));
        }

        public class Category
        {
            [PrimaryKey(1)] public ulong TenantId { get; set; }

            [PrimaryKey(2)] public ulong Id { get; set; }

            public required string Name { get; set; }

            [SecondaryKey("LowerCaseName", IncludePrimaryKeyOrder = 1)]
            public string LowerCaseName => Name.ToLowerInvariant();
        }

        public interface ICategoryTable : IRelation<Category>
        {
            IEnumerable<Category> FindByLowerCaseName(ulong tenantId, string lowerCaseName);
            bool RemoveById(ulong tenantId, ulong id);
        }

        [Fact]
        void SecondaryIndexWithLowercasedStringWorks()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<ICategoryTable>();
            table.Upsert(new Category { TenantId = 1, Id = 1, Name = "Hello" });
            table.Upsert(new Category { TenantId = 1, Id = 2, Name = "World" });
            Assert.Single(table.FindByLowerCaseName(1, "hello"));
            Assert.Single(table.FindByLowerCaseName(1, "world"));
            Assert.Empty(table.FindByLowerCaseName(1, "bad"));
            Assert.True(table.RemoveById(1, 1));
            Assert.Empty(table.FindByLowerCaseName(1, "hello"));
        }

        public class ItemWithDictWithReadOnlyMemory
        {
            [PrimaryKey(1)] public ulong Id { get; set; }

            public Dictionary<string, ReadOnlyMemory<byte>> Dict { get; set; }
        }

        public interface IItemWithDictWithReadOnlyMemoryTable : IRelation<ItemWithDictWithReadOnlyMemory>
        {
        }

        [Fact]
        void DictionaryWithReadOnlyMemoryCanBeStored()
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IItemWithDictWithReadOnlyMemoryTable>();
            table.Upsert(new()
            {
                Id = 1,
                Dict = new()
                {
                    { "a", new([1, 2, 3]) },
                    { "b", new([4, 5, 6]) }
                }
            });
            var item = table.First();
            Assert.Equal(2, item.Dict.Count);
            Assert.Equal(new byte[] { 1, 2, 3 }, item.Dict["a"].ToArray());
            Assert.Equal(new byte[] { 4, 5, 6 }, item.Dict["b"].ToArray());
        }

        public record PlannedMigrationStartInfo(ulong ActionId, DateTime PlannedStartDateTime);

        public class ActionItem
        {
            public PlannedMigrationStartInfo PlannedMigrationStart { get; set; }
        }

        public interface IActionTable : IRelation<ActionItem>
        {
        }

        [Fact]
        public void GivenRecordToStore_ItSilentlySkipsEqualityContractProperty()
        {
            using var tr = _db.StartTransaction();
            var rel = tr.GetRelation<IActionTable>();
            rel.Upsert(new() { PlannedMigrationStart = new(1, DateTime.UtcNow) });
            tr.Commit();
        }

        public class Invoice
        {
            [PrimaryKey(1)] public ulong CompanyId { get; set; }

            [SecondaryKey("ArchiveIdForMigration", IncludePrimaryKeyOrder = 1)]
            public string? OldArchiveIdForMigration => ArchiveId;

            public string ArchiveId { get; set; }
        }

        public interface IInvoiceTable : IRelation<Invoice>
        {
            IEnumerable<Invoice> FindByArchiveIdForMigration(ulong companyId, string oldArchiveIdForMigration);
        }

        [Fact]
        public void CanSerializeObjectWithComputedPropertyToEventLayer()
        {
            using var tr = _db.StartTransaction();
            var rel = tr.GetRelation<IInvoiceTable>();
            rel.Upsert(new Invoice() { CompanyId = 1, ArchiveId = "1" });
            Assert.Single(rel.FindByArchiveIdForMigration(1, "1"));
            new EventSerializer().Serialize(out var _, rel.FindByArchiveIdForMigration(1, "1").First());
        }
    }
}

namespace Imp1
{
    public class InnerImplementation : ObjectDbTableTest.IInnerInterface
    {
    }
}

namespace Imp2
{
    public class InnerImplementation : ObjectDbTableTest.IInnerInterface
    {
    }
}
