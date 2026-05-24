using System;
using System.Linq;
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
        public DateTime Uploaded
        {
            get { return _uploaded; }
            set { _uploaded = value; }
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
    }

    public interface IPersonProjection : IRelation<Person>
    {
        PersonProjection FindByIdOrDefault(ulong tenantId, string email);
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

        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Where(p => p.Age > 30).ToArray());
        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Where(p => (long)p.Age == 35).ToArray());
        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Where(p => (object)p.Age == (object)35).ToArray());
        Assert.Throws<NotSupportedException>(() => table.Query<Person>().Any());
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
