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

        [PrimaryKey(2)] public string? Email { get; set; }

        [InKeyValue(3)] public DateTime LastLogin { get; set; }

        public string? Name { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<string> email,
            Constraint<DateTime> lastLogin);

        bool UpdateById(uint tenantId, string email, DateTime lastLogin);
        bool UpdateById(uint tenantId, string email, DateTime lastLogin, string name);
        void Insert(Person person);
        void Update(Person person);
        Person FindById(uint tenantId, string email);
        void RemoveById(uint tenantId, string email);
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
    public void FindByIdWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        Assert.NotNull(t.FindById(1, "b@b.cd"));
    }

    [Fact]
    public void RemoveByIdWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        t.RemoveById(1, "b@b.cd");
        Assert.Equal(3, t.Count);
    }

    [Fact]
    public void FullCaseUpdateByIdWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        Assert.False(t.UpdateById(1, "c@b.cd", new DateTime(2023, 7, 30, 0, 0, 0, DateTimeKind.Utc),
            "does not matter"));
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
        t.Insert(new()
            { TenantId = 1, Email = "c@b.cd", Name = "BB", LastLogin = new(2023, 7, 30, 0, 0, 0, DateTimeKind.Utc) });
        Assert.Equal("BB",
            t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.Exact("c@b.cd"), Constraint<DateTime>.Any)
                .Single().Name);
        t.Update(new()
            { TenantId = 1, Email = "c@b.cd", Name = "CC", LastLogin = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        Assert.Equal("CC",
            t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.Exact("c@b.cd"),
                    Constraint.DateTime.Predicate(d => d.Year == 2024))
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

        [InKeyValue(2)] public int Age { get; set; }
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
            t.Upsert(new() { Id = 1, Name = "A", Age = 11 });
            t.Upsert(new() { Id = 2, Name = "B", Age = 22 });
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
        [PrimaryKey(1)] public uint Id { get; set; }

        [InKeyValue(2)] public int Age { get; set; }

        [PrimaryKey(3)] public string Name { get; set; }
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

    public class ApiKey
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong ApiKeyId { get; set; }

        [SecondaryKey("Key", IncludePrimaryKeyOrder = 1)]
        public string Key { get; set; }

        public string Name { get; set; }
        public string Description { get; set; }
        public DateTime CreatedDate { get; set; }
        [InKeyValue(3)] public DateTime? LastUsedDate { get; set; }
        [InKeyValue(4)] public DateTime? ExpirationDate { get; set; }
        public ISet<ulong> AdminRoleIds { get; set; } = new HashSet<ulong>();
        public ulong OwnerUserId { get; set; }
        public ApiKeyIpFiltering IpFiltering { get; set; }
    }

    public interface IApiKeyTable : IRelation<ApiKey>
    {
        void Insert(ApiKey apiKey);
        bool UpdateById(ulong companyId, ulong apiKeyId, DateTime? lastUsedDate, DateTime? expirationDate);
        int RemoveById(ulong companyId);
    }

    public class ApiKeyInKeyValues
    {
        public ulong CompanyId { get; set; }
        public ulong ApiKeyId { get; set; }
        public DateTime? LastUsedDate { get; set; }
        public DateTime? ExpirationDate { get; set; }
    }

    public class ApiKeyIpFiltering
    {
        public bool IsIpFilterActive { get; set; }
        public IList<ApiKeyIpFilter> IpAddresses { get; set; }
        public IList<ApiKeyIpFilterRange> IpAddressRanges { get; set; }
    }

    public class ApiKeyIpFilter
    {
        public string Name { get; set; }
        public string IpAddress { get; set; }
    }

    public class ApiKeyIpFilterRange
    {
        public string Name { get; set; }
        public string IpAddressFrom { get; set; }
        public string IpAddressTo { get; set; }
    }

    [Fact]
    void BreakIt()
    {
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IApiKeyTable>();
            for (var i = 0; i < 11; i++)
            {
                table.Insert(new ApiKey
                    { CompanyId = 0, Key = "K" + i, ApiKeyId = 2 + (ulong)i });
            }

            table.RemoveById(0);
            tr.Commit();
        }
        for (int j = 0; j < 5; j++)
        {
            {
                using var tr = _db.StartTransaction();
                var table = tr.GetRelation<IApiKeyTable>();
                for (var i = 0; i < 5; i++)
                {
                    table.Insert(new ApiKey
                        { CompanyId = 1, Key = "K" + i, ApiKeyId = (ulong)i });
                }
                tr.Commit();
            }
            {
                using var tr = _db.StartTransaction();
                var table = tr.GetRelation<IApiKeyTable>();
                table.UpdateById(1, (ulong)4, DateTime.UtcNow, null);
                tr.Commit();
            }
            {
                using var tr = _db.StartTransaction();
                var table = tr.GetRelation<IApiKeyTable>();
                Assert.Equal(5, table.RemoveById(1));
                tr.Commit();
            }
        }
    }
}
