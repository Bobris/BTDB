using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbRelationQueryTest : ObjectDbTestBase
{
    public ObjectDbRelationQueryTest(ITestOutputHelper output) : base(output)
    {
    }

    [Generate]
    [GenerateFor(typeof(IPersonProjection))]
    public class Person
    {
        [PrimaryKey(1)]
        public ulong TenantId { get; set; }

        [PrimaryKey(2)]
        [SecondaryKey("Email")]
        public string Email { get; set; } = "";

        public string? Name { get; set; }
        public int Age { get; set; }

        DateTime _uploaded;
        public static int UploadedSetterCalls;
        public DateTime Uploaded
        {
            get { return _uploaded; }
            set
            {
                UploadedSetterCalls++;
                _uploaded = value;
            }
        }
    }

    [Generate]
    public class PersonProjection
    {
        [PrimaryKey(1)]
        public ulong TenantId { get; set; }

        [PrimaryKey(2)]
        public string Email { get; set; } = "";

        public string? Name { get; set; }
    }

    public class MissingMetadataPerson
    {
        public ulong TenantId { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        bool RemoveById(ulong tenantId, string email);
    }

    public interface IPersonProjection : IRelation<Person>
    {
        PersonProjection FindByIdOrDefault(ulong tenantId, string email);
    }

    [Generate]
    public class QueryVersion1
    {
        [PrimaryKey(1)] public uint Id { get; set; }
        public string? Name { get; set; }
        public int Score { get; set; }
        public string? Ignored { get; set; }
        public float Ratio { get; set; }
    }

    [Generate]
    public class QueryVersion2
    {
        [PrimaryKey(1)] public uint Id { get; set; }
        public long Score { get; set; }
        public string? Added { get; set; }
        public string? Name { get; set; }
        public double Ratio { get; set; }
    }

    public enum QueryLevel
    {
        Low,
        High
    }

    [Generate]
    public class QueryTypesRow
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public bool Active { get; set; }
        public int? Bonus { get; set; }
        public QueryLevel Level { get; set; }
        public Guid Token { get; set; }
        public decimal Price { get; set; }
        public byte Small { get; set; }
        public float Weight { get; set; }
    }

    public interface IQueryTypesTable : IRelation<QueryTypesRow>
    {
    }

    [PersistedName("RelationQueryVersions")]
    public interface IQueryVersion1Table : IRelation<QueryVersion1>
    {
    }

    [PersistedName("RelationQueryVersions")]
    public interface IQueryVersion2Table : IRelation<QueryVersion2>
    {
    }

    [Fact]
    public void QueryEnumeratesAllItems()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Equal(["Ada", "Bob", null, "Dana"], table.Query<Person>().AsEnumerable().Select(p => p.Name).ToArray());
    }

    [Fact]
    public void QueryWhereFiltersByPrimarySecondaryAndValueFields()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        var uploaded = new DateTime(2025, 2, 4, 0, 0, 0, DateTimeKind.Utc);

        Assert.Equal(["Ada"], table.Query<Person>()
            .Where(p => p.TenantId == 1 && p.Email == "ada@example.com" && p.Age == 41)
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
        Assert.Equal(["Dana"], table.Query<Person>()
            .Where(p => p.Uploaded == uploaded)
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
    }

    [Fact]
    public void QueryWhereSupportsOrCapturedConstantsAndNull()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        var firstEmail = "ada@example.com";
        string? missingName = null;

        Assert.Equal(["Ada", null], table.Query<Person>()
            .Where(p => (p.Email == firstEmail || p.Name == missingName) && true)
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
        Assert.Equal(["Bob"], table.Query<Person>()
            .Where(p => "bob@example.com" == p.Email || false)
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
    }

    [Fact]
    public void QuerySupportsMultipleWhereCalls()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Equal(["Bob"], table.Query<Person>()
            .Where(p => p.TenantId == 1)
            .Where(p => p.Age == 35)
            .Where(p => p.Name == "Bob")
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
    }

    [Fact]
    public void QueryDoesNotMaterializeRowsWhileTestingPredicate()
    {
        FillPersonData();
        Person.UploadedSetterCalls = 0;

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        using var enumerator = table.Query<Person>().Where(p => p.Age == 35).GetEnumerator();

        Assert.True(enumerator.MoveNext());
        Assert.Equal(0, Person.UploadedSetterCalls);
        Assert.Equal("Bob", enumerator.Current.Name);
        Assert.Equal(1, Person.UploadedSetterCalls);
    }

    [Fact]
    public async Task QueryDoesNotReadValueForRowsRejectedByKeyPredicate()
    {
        FillPersonData();
        byte[] prefix;
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IPersonTable>();
            prefix = ((IRelationDbManipulator)table).RelationInfo.Prefix.ToArray();
        }

        using (var lowTr = await _lowDb.StartWritingTransaction())
        {
            using var cursor = lowTr.CreateCursor();
            Assert.True(cursor.FindFirstKey(prefix));
            cursor.SetValue([]);
            lowTr.Commit();
        }

        using var readTr = _db.StartTransaction();
        var readTable = readTr.GetRelation<IPersonTable>();

        Assert.Equal(["Dana"], readTable.Query<Person>()
            .Where(p => p.TenantId == 2)
            .Where(p => p.Age == 41)
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
    }

    [Fact]
    public void QueryProgramHandlesAllPersistedValueVersions()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IQueryVersion1Table>().Upsert(new()
            {
                Id = 1, Name = "old", Score = 11, Ignored = "old ignored"
            });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IQueryVersion2Table>().Upsert(new()
            {
                Id = 2, Name = "new", Score = 22, Added = "new added"
            });
            tr.Commit();
        }

        ReopenDb();
        using var readTr = _db.StartTransaction();
        var table = readTr.GetRelation<IQueryVersion2Table>();

        Assert.Equal(["old"], table.Query<QueryVersion2>()
            .Where(p => p.Score == 11)
            .Where(p => p.Name == "old")
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
        Assert.Equal(["new"], table.Query<QueryVersion2>()
            .Where(p => p.Score == 22)
            .Where(p => p.Name == "new")
            .AsEnumerable()
            .Select(p => p.Name)
            .ToArray());
    }

    [Fact]
    public void QueryProgramConvertsAndDefaultsFieldsOfOlderValueVersions()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IQueryVersion1Table>().Upsert(new() { Id = 1, Name = "old", Score = 11, Ratio = 1.5f });
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<IQueryVersion2Table>().Upsert(new()
            {
                Id = 2, Name = "new", Score = 22, Added = "new added", Ratio = 2.5
            });
            tr.Commit();
        }

        ReopenDb();
        using var readTr = _db.StartTransaction();
        var table = readTr.GetRelation<IQueryVersion2Table>();

        // The old version stores Ratio as float, so the program converts it before comparing.
        Assert.Equal(["old"], table.Query<QueryVersion2>().Where(p => p.Ratio == 1.5).AsEnumerable()
            .Select(p => p.Name).ToArray());
        Assert.Equal(["new"], table.Query<QueryVersion2>().Where(p => p.Ratio == 2.5).AsEnumerable()
            .Select(p => p.Name).ToArray());
        // Added does not exist in the old version, so it reads as its default value there.
        Assert.Equal(["old"], table.Query<QueryVersion2>().Where(p => p.Added == null).AsEnumerable()
            .Select(p => p.Name).ToArray());
        Assert.Equal(["new"], table.Query<QueryVersion2>().Where(p => p.Added == "new added").AsEnumerable()
            .Select(p => p.Name).ToArray());
        Assert.Equal(["old", "new"], table.Query<QueryVersion2>()
            .Where(p => p.Added == null || p.Score == 22).AsEnumerable().Select(p => p.Name).ToArray());
    }

    [Fact]
    public void QueryContinuesWhenMatchedRowsAreRemovedDuringEnumeration()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        var names = new List<string?>();
        foreach (var person in table.Query<Person>().Where(p => p.Age == 41))
        {
            names.Add(person.Name);
            Assert.True(table.RemoveById(person.TenantId, person.Email));
        }

        Assert.Equal(["Ada", "Dana"], names);
        Assert.Equal(["Bob", null], table.Query<Person>().AsEnumerable().Select(p => p.Name).ToArray());
    }

    [Fact]
    public void QueryContinuesWhenMatchedRowsAreUpdatedDuringEnumeration()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        var names = new List<string?>();
        foreach (var person in table.Query<Person>().Where(p => p.Age == 41))
        {
            names.Add(person.Name);
            person.Age = 42;
            Assert.False(table.Upsert(person));
        }

        Assert.Equal(["Ada", "Dana"], names);
        Assert.Equal(["Ada", "Dana"], table.Query<Person>().Where(p => p.Age == 42).AsEnumerable()
            .Select(p => p.Name).ToArray());
    }

    [Fact]
    public void QueryWithAlwaysFalsePredicateReturnsNothing()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Empty(table.Query<Person>().Where(p => p.Age == 41 && false));
        Assert.Equal(4, table.Query<Person>().Where(p => p.Age == 41 || true).AsEnumerable().Count());
    }

    [Fact]
    public void QueryWhereSupportsComparisonsAndNot()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Equal(["Ada", "Dana"], Names(table.Query<Person>().Where(p => p.Age > 35 && p.Age <= 41)));
        Assert.Equal(["Bob", null], Names(table.Query<Person>().Where(p => !(p.Age == 41))));
        Assert.Equal(["Ada", "Bob"], Names(table.Query<Person>().Where(p => p.Uploaded < new DateTime(2025, 2, 3,
            0, 0, 0, DateTimeKind.Utc))));
        Assert.Equal(["Bob", null], Names(table.Query<Person>().Where(p => (long)p.Age * 2 < 80L)));
    }

    [Fact]
    public void QueryWhereSupportsContainsOnConstantCollections()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        var ages = new[] { 28, 35 };
        var names = new List<string?> { "Dana", null };
        var tenants = new HashSet<ulong> { 2 };

        // C# 14 binds array.Contains inside expression trees to MemoryExtensions.Contains.
        Assert.Equal(["Bob", null], Names(table.Query<Person>().Where(p => ages.Contains(p.Age))));
        Assert.Equal([null, "Dana"], Names(table.Query<Person>().Where(p => names.Contains(p.Name))));
        Assert.Equal([null, "Dana"], Names(table.Query<Person>().Where(p => tenants.Contains(p.TenantId))));
        Assert.Equal(["Ada"], Names(table.Query<Person>().Where(p => new[] { "ada@example.com" }.Contains(p.Email))));
    }

    [Fact]
    public void QueryWhereSupportsStringMethods()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Equal(["Bob"], Names(table.Query<Person>().Where(p => p.Email.StartsWith("bo", StringComparison.Ordinal))));
        Assert.Equal(["Ada", "Dana"], Names(table.Query<Person>().Where(p => p.Name != null && p.Name.EndsWith("a"))));
        Assert.Equal(["Dana"], Names(table.Query<Person>().Where(p => p.Email.Contains("an"))));
        Assert.Equal(["Ada"], Names(table.Query<Person>()
            .Where(p => string.Equals(p.Name, "ADA", StringComparison.OrdinalIgnoreCase))));
        Assert.Equal([null], Names(table.Query<Person>().Where(p => string.IsNullOrEmpty(p.Name))));
        Assert.Equal(["Dana"], Names(table.Query<Person>().Where(p => p.Name != null && p.Name.Length == 4)));
        Assert.Equal(["Ada", "Bob"], Names(table.Query<Person>()
            .Where(p => string.CompareOrdinal(p.Email, "c") < 0)));
    }

    [Fact]
    public void QueryKeepsExceptionsOfConjunctsWrittenBeforeOthers()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        // Tenant 2 contains a person without a name; the tenant filter written first avoids it.
        Assert.Equal(["Ada", "Bob"], Names(table.Query<Person>()
            .Where(p => p.TenantId == 1 && p.Name!.Length > 2)));
        // Written first, the throwing conjunct must see that row like LINQ does, despite the key prefix.
        Assert.Throws<NullReferenceException>(() => Names(table.Query<Person>()
            .Where(p => p.Name!.Length > 2 && p.TenantId == 1)));
    }

    [Fact]
    public void QueryWhereSupportsBoolsNullablesEnumsAndOtherTypes()
    {
        var token = Guid.NewGuid();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IQueryTypesTable>();
            table.Upsert(new() { Id = 1, Active = true, Bonus = null, Level = QueryLevel.High, Token = token, Price = 1.5m, Small = 200 });
            table.Upsert(new() { Id = 2, Active = false, Bonus = 3, Level = QueryLevel.Low, Price = 2m, Small = 5 });
            table.Upsert(new() { Id = 3, Active = true, Bonus = -1, Level = QueryLevel.Low, Price = -1m, Small = 5 });
            tr.Commit();
        }

        using var readTr = _db.StartTransaction();
        var rows = readTr.GetRelation<IQueryTypesTable>().Query<QueryTypesRow>();

        Assert.Equal([1ul, 3], Ids(rows.Where(p => p.Active)));
        Assert.Equal([2ul], Ids(rows.Where(p => !p.Active)));
        Assert.Equal([1ul], Ids(rows.Where(p => p.Bonus == null)));
        Assert.Equal([2ul], Ids(rows.Where(p => p.Bonus > 0)));
        Assert.Equal([1ul, 3], Ids(rows.Where(p => !(p.Bonus > 0))));
        Assert.Equal([2ul, 3], Ids(rows.Where(p => p.Bonus.HasValue)));
        Assert.Equal([1ul], Ids(rows.Where(p => p.Level == QueryLevel.High)));
        Assert.Equal([2ul, 3], Ids(rows.Where(p => new[] { QueryLevel.Low }.Contains(p.Level))));
        Assert.Equal([1ul], Ids(rows.Where(p => p.Token == token)));
        Assert.Equal([1ul, 2], Ids(rows.Where(p => p.Price * 2 >= 3m)));
        Assert.Equal([1ul], Ids(rows.Where(p => p.Small > 100)));
        Assert.Throws<InvalidOperationException>(() => Ids(rows.Where(p => p.Bonus!.Value > 0)));
    }

    [Fact]
    public void QueryCountsWithoutMaterializingRows()
    {
        FillPersonData();
        Person.UploadedSetterCalls = 0;

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Equal(4, table.Query<Person>().Count());
        Assert.Equal(2, table.Query<Person>().Count(p => p.TenantId == 1));
        Assert.Equal(2L, table.Query<Person>().Where(p => p.TenantId == 2).LongCount());
        Assert.Equal(1, table.Query<Person>().Count(p => p.Age == 41 && p.TenantId == 2));
        Assert.Equal(0, table.Query<Person>().Count(p => p.TenantId == 3));
        Assert.True(table.Query<Person>().Any());
        Assert.False(table.Query<Person>().Any(p => p.Age > 100));
        Assert.True(table.Query<Person>().All(p => p.Age > 20));
        Assert.False(table.Query<Person>().Where(p => p.TenantId == 1).All(p => p.Age == 41));
        Assert.Equal(0, Person.UploadedSetterCalls);

        Assert.True(table.RemoveById(1, "ada@example.com"));
        Assert.Equal(3, table.Query<Person>().Count());
        Assert.Equal(1, table.Query<Person>().Count(p => p.TenantId == 1));
    }

    [Fact]
    public void QueryElementOperatorsBehaveLikeLinq()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        var query = table.Query<Person>();
        var fallback = new Person { Name = "fallback" };

        Assert.Equal("Ada", query.First().Name);
        Assert.Equal("Bob", query.First(p => p.Age == 35).Name);
        Assert.Equal("Dana", query.Last().Name);
        Assert.Equal("Dana", query.Where(p => p.Age > 30).Last(p => p.TenantId == 2).Name);
        Assert.Equal("Bob", query.Single(p => p.Age == 35).Name);
        Assert.Null(query.SingleOrDefault(p => p.Age == 99));
        Assert.Null(query.FirstOrDefault(p => p.Age == 99));
        Assert.Same(fallback, query.FirstOrDefault(p => p.Age == 99, fallback));
        Assert.Same(fallback, query.Where(p => p.Age == 99).LastOrDefault(fallback));
        Assert.Equal("Sequence contains no elements",
            Assert.Throws<InvalidOperationException>(() => query.Where(p => p.Age == 99).First()).Message);
        Assert.Equal("Sequence contains no matching element",
            Assert.Throws<InvalidOperationException>(() => query.Last(p => p.Age == 99)).Message);
        Assert.Equal("Sequence contains more than one element",
            Assert.Throws<InvalidOperationException>(() => query.Where(p => p.Age == 41).Single()).Message);
        Assert.Equal("Sequence contains more than one matching element",
            Assert.Throws<InvalidOperationException>(() => query.SingleOrDefault(p => p.TenantId == 1)).Message);
    }

    [Fact]
    public void QueryAggregatesBehaveLikeLinq()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IQueryTypesTable>();
            table.Upsert(new() { Id = 1, Bonus = null, Level = QueryLevel.Low, Price = 1.5m, Weight = 16777216f });
            table.Upsert(new() { Id = 2, Bonus = 3, Level = QueryLevel.High, Price = 2m, Weight = 1f });
            table.Upsert(new() { Id = 3, Bonus = -1, Level = QueryLevel.Low, Price = -1m, Weight = 1f });
            tr.Commit();
        }

        using var readTr = _db.StartTransaction();
        var rows = readTr.GetRelation<IQueryTypesTable>().Query<QueryTypesRow>();
        var all = rows.AsEnumerable().ToList();

        Assert.Equal(2.5m, rows.Sum(p => p.Price));
        Assert.Equal(2, rows.Sum(p => p.Bonus));
        // Floats are summed as double like Enumerable.Sum, which keeps the two small values.
        Assert.Equal(all.Sum(p => p.Weight), rows.Sum(p => p.Weight));
        Assert.Equal(1.0, rows.Average(p => p.Bonus));
        Assert.Equal(2.0, rows.Average(p => (int)p.Id));
        Assert.Equal(-1, rows.Min(p => p.Bonus));
        Assert.Equal(QueryLevel.High, rows.Max(p => p.Level));
        Assert.Equal(-1m, rows.Min(p => p.Price));
        Assert.Equal(3ul, rows.Max(p => p.Id));
        Assert.Null(rows.Where(p => p.Id == 1).Average(p => p.Bonus));
        Assert.Null(rows.Where(p => p.Id == 1).Min(p => p.Bonus));
        Assert.Equal(0, rows.Where(p => p.Id > 5).Sum(p => p.Bonus));
        Assert.Equal("Sequence contains no elements",
            Assert.Throws<InvalidOperationException>(() => rows.Where(p => p.Id > 5).Max(p => p.Price)).Message);
        Assert.Equal("Sequence contains no elements",
            Assert.Throws<InvalidOperationException>(() => rows.Where(p => p.Id > 5).Average(p => p.Price)).Message);
    }

    [Fact]
    public void QueryOrdersAndPagesLikeLinq()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var query = tr.GetRelation<IPersonTable>().Query<Person>();

        Assert.Equal(["Dana", null, "Bob", "Ada"], Names(query.OrderByDescending(p => p.TenantId).ThenByDescending(p => p.Email)));
        // Ties keep primary key order in both directions.
        Assert.Equal([null, "Bob", "Ada", "Dana"], Names(query.OrderBy(p => p.Age)));
        Assert.Equal(["Ada", "Dana", "Bob", null], Names(query.OrderByDescending(p => p.Age)));
        Assert.Equal(["Dana", "Ada", "Bob", null], Names(query.OrderByDescending(p => p.Age).ThenByDescending(p => p.TenantId)));
        // Strings are ordered by the current culture like Comparer<string>.Default; null comes first.
        Assert.Equal([null, "Ada", "Bob", "Dana"], Names(query.OrderBy(p => p.Name)));
        Assert.Equal(["Bob", null], Names(query.Skip(1).Take(2)));
        Assert.Equal(["Bob", "Ada"], Names(query.Where(p => p.TenantId == 1).OrderByDescending(p => p.Email).Take(5)));
        Assert.Equal(["Dana"], Names(query.OrderBy(p => p.Age).Skip(3)));
        Assert.Empty(Names(query.OrderBy(p => p.Age).Take(0)));

        Assert.Equal("Dana", query.OrderByDescending(p => p.Uploaded).First().Name);
        Assert.Equal("Bob", query.OrderBy(p => p.Age).First(p => p.TenantId == 1).Name);
        Assert.Equal("Dana", query.OrderBy(p => p.Age).Last().Name);
        Assert.Equal(2, query.Skip(1).Take(2).Count());
        Assert.Equal(1, query.OrderBy(p => p.Age).Skip(3).Count());
        Assert.Equal(82, query.OrderByDescending(p => p.Age).Take(2).Sum(p => p.Age));
        Assert.Equal(28, query.OrderBy(p => p.Age).Min(p => p.Age));

        Assert.Throws<NotSupportedException>(() => Names(query.OrderBy(p => p.Age).Where(p => p.Age > 30)));
        Assert.Throws<NotSupportedException>(() => Names(query.Take(2).Where(p => p.Age > 30)));
        Assert.Throws<NotSupportedException>(() => query.OrderBy(p => p.Age).First(p => p.Name!.Length > 2));
    }

    static string?[] Names(IQueryable<Person> query) => query.AsEnumerable().Select(p => p.Name).ToArray();
    static ulong[] Ids(IQueryable<QueryTypesRow> query) => query.AsEnumerable().Select(p => p.Id).ToArray();

    [Fact]
    public void QueryCanLoadRegisteredProjection()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        var result = table.Query<PersonProjection>().Where(p => p.Name == "Bob").AsEnumerable().Single();
        Assert.Equal((ulong)1, result.TenantId);
        Assert.Equal("bob@example.com", result.Email);
    }

    [Fact]
    public void QueryRequiresRegisteredMetadata()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Contains("does not have registered metadata",
            Assert.Throws<BTDBException>(() => table.Query<MissingMetadataPerson>()).Message);
    }

    [Fact]
    public void QueryRejectsUnsupportedExpressions()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();

        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Where(p => p.Name == p.Email).ToArray());
        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Where(p => p.Name + "x" == "Bobx").ToArray());
        Assert.Throws<NotSupportedException>(() =>
            table.Query<Person>().Where(p => string.CompareOrdinal(p.Name, "B") == 1).ToArray());
        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Where(p => (object)p.Age == (object)35).ToArray());
        Assert.Throws<NotSupportedException>(() => table.Query<Person>().ElementAt(1));
        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Select(p => p.Name));
    }

    [Fact]
    public void ReflectionEmitRelationForwardsQueryToManipulator()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        ObjectDB.ResetAllMetadataCaches();
        try
        {
            ReopenEmptyDb();

            using (var tr = _db.StartTransaction())
            {
                var table = tr.GetRelation<IPersonTable>();
                table.Upsert(new() { TenantId = 1, Email = "ada@example.com", Name = "Ada", Age = 41 });
                table.Upsert(new() { TenantId = 1, Email = "bob@example.com", Name = "Bob", Age = 35 });
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var table = tr.GetRelation<IPersonTable>();
                Assert.Equal(["Bob"], table.Query<Person>()
                    .Where(p => p.Age == 35)
                    .AsEnumerable()
                    .Select(p => p.Name)
                    .ToArray());
            }
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    void FillPersonData()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IPersonTable>();
        table.Upsert(new()
        {
            TenantId = 1, Email = "ada@example.com", Name = "Ada", Age = 41,
            Uploaded = new DateTime(2025, 2, 1, 0, 0, 0, DateTimeKind.Utc)
        });
        table.Upsert(new()
        {
            TenantId = 1, Email = "bob@example.com", Name = "Bob", Age = 35,
            Uploaded = new DateTime(2025, 2, 2, 0, 0, 0, DateTimeKind.Utc)
        });
        table.Upsert(new()
        {
            TenantId = 2, Email = "c@example.com", Name = null, Age = 28,
            Uploaded = new DateTime(2025, 2, 3, 0, 0, 0, DateTimeKind.Utc)
        });
        table.Upsert(new()
        {
            TenantId = 2, Email = "dana@example.com", Name = "Dana", Age = 41,
            Uploaded = new DateTime(2025, 2, 4, 0, 0, 0, DateTimeKind.Utc)
        });
        tr.Commit();
    }
}
