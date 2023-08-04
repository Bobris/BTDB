using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableInKeyValueTest : ObjectDbTestBase
{
    public ObjectDbTableInKeyValueTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Person
    {
        [PrimaryKey(1)] public uint TenantId { get; set; }

        [PrimaryKey(2)]
        public string? Email { get; set; }

        [InKeyValue(3)]
        public DateTime LastLogin { get; set; }

        public string? Name { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<string> email, Constraint<DateTime> lastLogin);
        bool UpdateById(uint tenantId, string email, DateTime lastLogin);
        bool UpdateById(uint tenantId, string email, DateTime lastLogin, string name);
        void Insert(Person person);
        void Update(Person person);
    }

    [Fact]
    public void SimpleCaseUpsertWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        Assert.Equal("AB",
            string.Join("",
                t.ScanById(Constraint<ulong>.Any, Constraint<string>.Any,
                        Constraint.DateTime.UpTo(new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc)))
                    .Select(p => p.Name)));
    }

    [Fact]
    public void JustKeyCaseUpdateByIdWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        Assert.False(t.UpdateById(1, "c@b.cd", new DateTime(2023, 7, 30, 0, 0, 0, DateTimeKind.Utc)));
        Assert.True(t.UpdateById(1, "b@b.cd", new DateTime(2023, 7, 30, 0, 0, 0, DateTimeKind.Utc)));
        Assert.Equal("A",
            string.Join("",
                t.ScanById(Constraint<ulong>.Any, Constraint<string>.Any,
                        Constraint.DateTime.UpTo(new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc)))
                    .Select(p => p.Name)));
    }

    [Fact]
    public void FullCaseUpdateByIdWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        Assert.False(t.UpdateById(1, "c@b.cd", new DateTime(2023, 7, 30, 0, 0, 0, DateTimeKind.Utc), "does not matter"));
        Assert.True(t.UpdateById(1, "b@b.cd", new DateTime(2023, 7, 30, 0, 0, 0, DateTimeKind.Utc), "OK"));
        Assert.Equal("A",
            string.Join("",
                t.ScanById(Constraint<ulong>.Any, Constraint<string>.Any,
                        Constraint.DateTime.UpTo(new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc)))
                    .Select(p => p.Name)));
        Assert.Equal("OK",
            t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.Exact("b@b.cd"), Constraint<DateTime>.Any)
                .Single().Name);
    }

    [Fact]
    public void InsertAndUpdateWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        t.Insert(new() { TenantId = 1, Email = "c@b.cd", Name = "BB", LastLogin = new(2023, 7, 30, 0, 0, 0, DateTimeKind.Utc) });
        Assert.Equal("BB",
            t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.Exact("c@b.cd"), Constraint<DateTime>.Any)
                .Single().Name);
        t.Update(new() { TenantId = 1, Email = "c@b.cd", Name = "CC", LastLogin = new(2024, 1,1,0,0,0,DateTimeKind.Utc) });
        Assert.Equal("CC",
            t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.Exact("c@b.cd"), Constraint.DateTime.Predicate(d=>d.Year==2024))
                .Single().Name);
    }

    void FillPersonData()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        t.Upsert(new()
            { TenantId = 1, Email = "a@b.cd", Name = "A", LastLogin = new(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        t.Upsert(new()
            { TenantId = 1, Email = "b@b.cd", Name = "B", LastLogin = new(2022, 7, 1, 0, 0, 0, DateTimeKind.Utc) });
        t.Upsert(new()
            { TenantId = 2, Email = "a@c.cd", Name = "C", LastLogin = new(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc) });
        t.Upsert(new()
            { TenantId = 2, Email = "b@c.cd", Name = "D", LastLogin = new(2023, 7, 29, 0, 0, 0, DateTimeKind.Utc) });
        tr.Commit();
    }

    public class DataV1
    {
        [PrimaryKey(1)] public uint Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }
    }

    [PersistedName("Data")]
    public interface IDataV1Table : IRelation<DataV1>
    {
    }

    public class DataV2
    {
        [PrimaryKey(1)] public uint Id { get; set; }

        public string Name { get; set; }

        [InKeyValue(2)]
        public int Age { get; set; }
    }

    [PersistedName("Data")]
    public interface IDataV2Table : IRelation<DataV2>
    {
        void UpdateById(uint id, int age);
    }

    [Fact]
    public void PromotionValueToInKeyValueWorks()
    {
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IDataV1Table>();
            t.Upsert(new (){ Id = 1, Name = "A", Age = 11});
            t.Upsert(new (){ Id = 2, Name = "B", Age = 22});
            tr.Commit();
        }
        ReopenDb();
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IDataV2Table>();
            Assert.Equal(11, t.First().Age);
            t.UpdateById(1, 111);
            Assert.Equal(111, t.First().Age);
            tr.Commit();
        }
        ReopenDb();
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IDataV2Table>();
            Assert.Equal(111, t.First().Age);
        }
        ReopenDb();
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IDataV1Table>();
            Assert.Equal(11, t.First().Age);
        }
    }

    public class InvalidData
    {
        [PrimaryKey(1)]
        public uint Id { get; set; }

        [InKeyValue(2)]
        public int Age { get; set; }

        [PrimaryKey(3)]
        public string Name { get; set; }
    }

    public interface IInvalidDataTable : IRelation<InvalidData>
    {
    }

    [Fact]
    public void InvalidDataTest()
    {
        using var tr = _db.StartTransaction();
        Assert.Equal("PrimaryKey Name cannot be after InKeyValue Age",
            Assert.Throws<BTDBException>(() => tr.GetRelation<IInvalidDataTable>()).Message);
    }
}
