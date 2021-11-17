using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableScanTest : ObjectDbTestBase
{
    public ObjectDbTableScanTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Person
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }

        [PrimaryKey(2)] public string? Email { get; set; }

        public string? Name { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<string> email);
    }

    [Fact]
    public void ScanByIdWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var p = t.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.StartsWith("a")).Single();
        Assert.Equal("A", p.Name);
    }

    [Theory]
    [InlineData("a", "AC")]
    [InlineData("b", "BD")]
    [InlineData("", "ABCD")]
    [InlineData("c", "")]
    public void ConstraintUnsignedAnyStringPrefixWorks(string prefix, string expectedNames)
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanById(Constraint.Unsigned.Any, Constraint.String.StartsWith(prefix))
            .Select(p => p.Name));
        Assert.Equal(expectedNames, names);
    }

    [Theory]
    [InlineData("a", "AC")]
    [InlineData("b", "ABD")]
    [InlineData("", "ABCD")]
    [InlineData(".c", "ABCD")]
    [InlineData("a@b.cd", "A")]
    [InlineData("a@c.cd", "C")]
    [InlineData("a@c.cd2", "")]
    public void ConstraintUnsignedAnyStringContainsWorks(string contain, string expectedNames)
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanById(Constraint.Unsigned.Any, Constraint.String.Contains(contain))
            .Select(p => p.Name));
        Assert.Equal(expectedNames, names);
    }

    [Theory]
    [InlineData("a", "")]
    public void ConstraintUnsignedAnyStringContainsWorksOnEmptyTable(string contain, string expectedNames)
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanById(Constraint.Unsigned.Any, Constraint.String.Contains(contain))
            .Select(p => p.Name));
        Assert.Equal(expectedNames, names);
    }

    void FillPersonData()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        t.Upsert(new() { TenantId = 1, Email = "a@b.cd", Name = "A" });
        t.Upsert(new() { TenantId = 1, Email = "b@b.cd", Name = "B" });
        t.Upsert(new() { TenantId = 2, Email = "a@c.cd", Name = "C" });
        t.Upsert(new() { TenantId = 2, Email = "b@c.cd", Name = "D" });
        tr.Commit();
    }

    public class ThreeUlongs
    {
        [PrimaryKey(1)] public ulong N1 { get; set; }
        [PrimaryKey(2)] public ulong N2 { get; set; }
        [PrimaryKey(3)] public ulong N3 { get; set; }

        public ulong Id { get; set; }
    }

    public interface IThreeUlongsTable : IRelation<ThreeUlongs>
    {
        IEnumerable<ThreeUlongs> ScanById(Constraint<ulong> n1, Constraint<ulong> n2);
        IEnumerable<ThreeUlongs> ScanById(Constraint<ulong> n1, Constraint<ulong> n2, Constraint<ulong> n3);

        ulong GatherById(List<ThreeUlongs> target, long skip, long take, Constraint<ulong> n1,
            Constraint<ulong> n2);
    }

    [Fact]
    public void ConstraintUnsignedAnyUnsignedPredicateWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        AssertSameCondition(t.Where(v => v.N2 > 3),
            t.ScanById(Constraint.Unsigned.Any, Constraint.Unsigned.Predicate(n => n > 3)));
    }

    [Fact]
    public void ConstraintUnsignedAnyUnsignedUpToWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        AssertSameCondition(t.Where(v => v.N2 <= 3),
            t.ScanById(Constraint.Unsigned.Any, Constraint.Unsigned.UpTo(3)));
    }

    [Fact]
    public void ConstraintUnsignedAnyUnsignedUpToExcludeWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        AssertSameCondition(t.Where(v => v.N2 < 3),
            t.ScanById(Constraint.Unsigned.Any, Constraint.Unsigned.UpTo(3, false)));
    }

    [Fact]
    public void ConstraintTripleUnsignedExactWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        AssertSameCondition(t.Where(v => v.N1 == 1 && v.N2 == 2 && v.N3 == 3),
            t.ScanById(Constraint.Unsigned.Exact(1), Constraint.Unsigned.Exact(2), Constraint.Unsigned.Exact(3)));
    }

    [Fact]
    public void GatherConstraintUnsignedAnyUnsignedPredicateWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count(v => v.N2 > 3),
            t.GatherById(dst, 1, 2, Constraint.Unsigned.Any, Constraint.Unsigned.Predicate(n => n > 3)));
        AssertSameCondition(t.Where(v => v.N2 > 3).Skip(1).Take(2), dst);
    }

    static void AssertSameCondition(IEnumerable<ThreeUlongs> expectedResult, IEnumerable<ThreeUlongs> scanResult)
    {
        var expected = string.Join(',', expectedResult.Select(v => v.Id));
        var test = string.Join(',', scanResult.Select(v => v.Id));
        Assert.Equal(expected, test);
    }

    void FillThreeUlongsData()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        for (var i = 1; i <= 5; i++)
        {
            for (var j = 1; j <= 5; j++)
            {
                for (var k = 1; k <= 5; k++)
                {
                    t.Upsert(new()
                    { N1 = (ulong)i, N2 = (ulong)j, N3 = (ulong)k, Id = (ulong)(i * 100 + j * 10 + k) });
                }
            }
        }

        tr.Commit();
    }

    public class ThingWithSK
    {
        public ThingWithSK(ulong tenant, string name, uint age)
        {
            Tenant = tenant;
            Name = name;
            Age = age;
        }

        [PrimaryKey(1)] public ulong Tenant { get; set; }

        [PrimaryKey(2)]
        [SecondaryKey("Name", IncludePrimaryKeyOrder = 0, Order = 1)]
        public string Name { get; set; }
        [SecondaryKey("Name", IncludePrimaryKeyOrder = 0, Order = 2)]
        public uint Age { get; set; }
    }

    public interface IThingWithSKTable : IRelation<ThingWithSK>
    {
        IEnumerable<ThingWithSK> ScanByName(Constraint<string> name, Constraint<ulong> age, Constraint<ulong> tenant);
    }

    [Fact]
    public void ScanBySecondaryKeyWorks()
    {
        FillThingWithSKData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        var p = t.ScanByName(Constraint.String.Exact("C"), Constraint.Unsigned.Any, Constraint.Unsigned.Any).Single();
        Assert.Equal("C", p.Name);
        p = t.ScanByName(Constraint.String.Any, Constraint.Unsigned.Any, Constraint.Unsigned.Exact(3)).Single();
        Assert.Equal("D", p.Name);
    }

    void FillThingWithSKData()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        t.Upsert(new(1, "A", 5));
        t.Upsert(new(1, "B", 6));
        t.Upsert(new(2, "C", 6));
        t.Upsert(new(3, "D", 7));
        tr.Commit();
    }

}
