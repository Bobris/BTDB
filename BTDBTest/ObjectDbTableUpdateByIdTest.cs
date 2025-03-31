using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using BTDB.Encrypted;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableUpdateByIdTest : ObjectDbTestBase
{
    public ObjectDbTableUpdateByIdTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Person
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }

        public string? Name { get; set; }
        public int Age { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        bool UpdateById(ulong tenantId, int age);
        bool UpdateById(ulong tenantId, string name);
    }

    [Fact]
    public void UpdateByIdSimpleIntWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        Assert.True(t.UpdateById(1, 43));
        var p = t.First(p => p.TenantId == 1);
        Assert.Equal("A", p.Name);
        Assert.Equal(43, p.Age);
        Assert.False(t.UpdateById(111, 43));
    }

    [Fact]
    public void UpdateByIdSimpleStringWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        Assert.True(t.UpdateById(1, "C"));
        var p = t.First(p => p.TenantId == 1);
        Assert.Equal("C", p.Name);
        Assert.Equal(42, p.Age);
        Assert.False(t.UpdateById(111, "C"));
    }

    void FillPersonData()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        t.Upsert(new() { TenantId = 1, Name = "A", Age = 42 });
        t.Upsert(new() { TenantId = 2, Name = "B", Age = 5 });
        tr.Commit();
    }

    public class PersonInvalid
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }

        public string? Name { get; set; }

        public string? ExpertSexChange { get; set; }
        public string? ExpertsExchange { get; set; }
    }

    public interface IPersonInvalid1Table : IRelation<PersonInvalid>
    {
        bool UpdateById(ulong tenantId, int age);
    }

    [Fact]
    public void UpdateByIdDetectsUnusedParameters()
    {
        using var tr = _db.StartTransaction();
        Assert.Contains(" age ", Assert.Throws<BTDBException>(() => tr.GetRelation<IPersonInvalid1Table>()).Message);
    }

    public interface IPersonInvalid2Table : IRelation<PersonInvalid>
    {
        bool UpdateById(ulong tenantId, int name);
    }

    [Fact]
    public void UpdateByIdDetectsValueParametersWithWrongTypes()
    {
        using var tr = _db.StartTransaction();
        Assert.Contains(" name ", Assert.Throws<BTDBException>(() => tr.GetRelation<IPersonInvalid2Table>()).Message);
    }

    public interface IPersonInvalid3Table : IRelation<PersonInvalid>
    {
        bool UpdateById(ulong tenantId, string expertsExchange);
    }

    [Fact]
    public void UpdateByIdDetectsNotUniqueParameterNameMatch()
    {
        using var tr = _db.StartTransaction();
        Assert.Contains(" expertsExchange ",
            Assert.Throws<BTDBException>(() => tr.GetRelation<IPersonInvalid3Table>()).Message);
    }

    public interface IPersonVoidTable : IRelation<Person>
    {
        void UpdateById(ulong tenantId, int age);
    }

    [Fact]
    public void VoidUpdateByIdSimpleIntThrowsIfNotFound()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonVoidTable>();
        Assert.Throws<BTDBException>(() => t.UpdateById(1, 43));
    }

    [Fact]
    public void VoidUpdateByIdSimpleIntWorksIfFound()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonVoidTable>();
        t.Upsert(new() { TenantId = 1, Age = 42, Name = "A" });
        t.UpdateById(1, 43);
        Assert.Equal(43, t.First().Age);
        Assert.Equal("A", t.First().Name);
    }

    public class ComplexPerson
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }

        [SecondaryKey("Name", IncludePrimaryKeyOrder = 1, Order = 2)]
        public string? Name { get; set; }

        public EncryptedString Secret { get; set; }

        public class Nested
        {
            public string? Name { get; set; }
        }

        public Nested? N1 { get; set; }
        public Nested? N2 { get; set; }
    }

    public interface IComplexPersonTable : IRelation<ComplexPerson>
    {
        bool UpdateById(ulong tenantId, ulong id, string name);
        bool UpdateByIdSecret(ulong tenantId, ulong id, string secret);
    }

    [Fact]
    public void ComplexPersonTest()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IComplexPersonTable>();
        var n = new ComplexPerson.Nested { Name = "N" };
        t.Upsert(new() { TenantId = 1, Id = 123456, N1 = n, N2 = n, Name = "A", Secret = "1331" });
        t.UpdateByIdSecret(1, 123456, "s3cr3t");
        var p = t.First();
        Assert.Equal("s3cr3t", p.Secret.Secret);
        Assert.Same(p.N1, p.N2);
    }

    public class PersonWithComputed
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }

        public string? Name { get; set; }

        [PrimaryKey(2)] public int Age { get; set; }

        [SecondaryKey("Computed", IncludePrimaryKeyOrder = 1, Order = 2)]
        public string ComputedName => $"{Name} ({Age})";
    }

    public interface IPersonWithComputedTable : IRelation<PersonWithComputed>
    {
        bool UpdateById(ulong tenantId, int age, string name);
    }

    [Fact]
    public void UpdateByIdWithComputedWorks()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonWithComputedTable>();
        t.Upsert(new() { TenantId = 1, Name = "A", Age = 42 });
        t.UpdateById(1, 42, "B");
        var p = t.First();
        Assert.Equal("B", p.Name);
        Assert.Equal(42, p.Age);
        Assert.Equal("B (42)", p.ComputedName);
    }
}
