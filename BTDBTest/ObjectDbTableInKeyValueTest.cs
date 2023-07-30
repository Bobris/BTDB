using System;
using System.Collections.Generic;
using System.Linq;
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

        [PrimaryKey(3, InKeyValue = true)]
        public DateTime LastLogin { get; set; }

        public string? Name { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<string> email, Constraint<DateTime> lastLogin);
        bool UpdateById(uint tenantId, string email, DateTime lastLogin);
        bool UpdateById(uint tenantId, string email, DateTime lastLogin, string name);
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
}
