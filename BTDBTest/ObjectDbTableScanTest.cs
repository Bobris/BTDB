using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableScanTest : ObjectDbTestBase
{
    public ObjectDbTableScanTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum SignedEnum
    {
        Opt1,
        Opt2 = -1
    }

    public enum UnsignedEnum : byte
    {
        Opt1,
        Opt2
    }

    public class Person
    {
        [PrimaryKey(1)] public uint TenantId { get; set; }

        [SecondaryKey("Email")]
        [PrimaryKey(2)]
        public string? Email { get; set; }

        [SecondaryKey("SignedEnum")] public SignedEnum Enum1 { get; set; }

        [SecondaryKey("UnsignedEnum")] public UnsignedEnum Enum2 { get; set; }

        public string? Name { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<string> email);
        IEnumerable<Person> ScanByEmail(Constraint<string> email);
        IEnumerable<Person> ScanBySignedEnum(Constraint<SignedEnum> enum1);
        IEnumerable<Person> ScanByUnsignedEnum(Constraint<UnsignedEnum> enum2);
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
    [InlineData("a", "AC")]
    [InlineData("b", "ABD")]
    [InlineData("", "ACBD")]
    [InlineData(".c", "ACBD")]
    [InlineData("a@b.cd", "A")]
    [InlineData("a@c.cd", "C")]
    [InlineData("a@c.cd2", "")]
    public void ConstraintStringContainsWorks(string contain, string expectedNames)
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanByEmail(Constraint.String.Contains(contain))
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

    [Theory]
    [InlineData(SignedEnum.Opt1, "AC")]
    [InlineData(SignedEnum.Opt2, "BD")]
    public void ConstraintSignedEnumExactWorks(SignedEnum enum1, string expectedNames)
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanBySignedEnum(Constraint.Enum<SignedEnum>.Exact(enum1))
            .Select(p => p.Name));
        Assert.Equal(expectedNames, names);
    }

    [Theory]
    [InlineData(SignedEnum.Opt1, "AC")]
    [InlineData(SignedEnum.Opt2, "BD")]
    public void ConstraintSignedEnumPredicateWorks(SignedEnum enum1, string expectedNames)
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanBySignedEnum(Constraint.Enum<SignedEnum>.Predicate(v => v == enum1))
            .Select(p => p.Name));
        Assert.Equal(expectedNames, names);
    }

    [Fact]
    public void ConstraintSignedEnumAnyWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanBySignedEnum(Constraint.Enum<SignedEnum>.Any)
            .Select(p => p.Name));
        Assert.Equal("BDAC", names);
        names = string.Concat(t.ScanBySignedEnum(Constraint<SignedEnum>.Any)
            .Select(p => p.Name));
        Assert.Equal("BDAC", names);
    }

    [Theory]
    [InlineData(UnsignedEnum.Opt1, "AB")]
    [InlineData(UnsignedEnum.Opt2, "CD")]
    public void ConstraintUnsignedEnumExactWorks(UnsignedEnum enum2, string expectedNames)
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanByUnsignedEnum(Constraint.Enum<UnsignedEnum>.Exact(enum2))
            .Select(p => p.Name));
        Assert.Equal(expectedNames, names);
    }

    [Theory]
    [InlineData(UnsignedEnum.Opt1, "AB")]
    [InlineData(UnsignedEnum.Opt2, "CD")]
    public void ConstraintUnsignedEnumPredicateWorks(UnsignedEnum enum2, string expectedNames)
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanByUnsignedEnum(Constraint.Enum<UnsignedEnum>.Predicate(v => v == enum2))
            .Select(p => p.Name));
        Assert.Equal(expectedNames, names);
    }

    [Fact]
    public void ConstraintUnsignedEnumAnyWorks()
    {
        FillPersonData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        var names = string.Concat(t.ScanByUnsignedEnum(Constraint.Enum<UnsignedEnum>.Any)
            .Select(p => p.Name));
        Assert.Equal("ABCD", names);
        names = string.Concat(t.ScanByUnsignedEnum(Constraint<UnsignedEnum>.Any)
            .Select(p => p.Name));
        Assert.Equal("ABCD", names);
    }

    void FillPersonData()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IPersonTable>();
        t.Upsert(new()
            { TenantId = 1, Email = "a@b.cd", Name = "A", Enum1 = SignedEnum.Opt1, Enum2 = UnsignedEnum.Opt1 });
        t.Upsert(new()
            { TenantId = 1, Email = "b@b.cd", Name = "B", Enum1 = SignedEnum.Opt2, Enum2 = UnsignedEnum.Opt1 });
        t.Upsert(new()
            { TenantId = 2, Email = "a@c.cd", Name = "C", Enum1 = SignedEnum.Opt1, Enum2 = UnsignedEnum.Opt2 });
        t.Upsert(new()
            { TenantId = 2, Email = "b@c.cd", Name = "D", Enum1 = SignedEnum.Opt2, Enum2 = UnsignedEnum.Opt2 });
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

        ulong GatherById(List<ThreeUlongs> target, long skip, long take, Constraint<ulong> n1,
            Constraint<ulong> n2, IOrderer[] orderers);
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

    [Fact]
    public void GatherConstraintFirst1Works()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count(v => v.N1 == 1 && v.N2 == 1 && v.N3 == 1),
            t.GatherById(dst, 0, 1000, Constraint.First(Constraint.Unsigned.Any), Constraint.Unsigned.Any));
        AssertSameCondition(t.Where(v => v.N1 == 1 && v.N2 == 1 && v.N3 == 1), dst);
        dst.Clear();
        Assert.Equal((ulong)t.Count(v => v.N1 == 1 && v.N2 == 1 && v.N3 == 1),
            t.GatherById(dst, 0, 1000, Constraint.First(Constraint.Unsigned.Any), Constraint.Unsigned.Any,
                new[] { Orderer.Ascending((ThreeUlongs v) => v.N1) }));
        AssertSameCondition(t.Where(v => v.N1 == 1 && v.N2 == 1 && v.N3 == 1), dst);
    }

    [Fact]
    public void GatherConstraintFirst2Works()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count(v => v.N2 == 1 && v.N3 == 1),
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any)));
        AssertSameCondition(t.Where(v => v.N2 == 1 && v.N3 == 1), dst);
        dst.Clear();
        Assert.Equal((ulong)t.Count(v => v.N1 <= 3 && v.N2 == 3 && v.N3 == 1),
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.UpTo(3),
                Constraint.First(Constraint.Unsigned.Predicate(v => v > 2))));
        AssertSameCondition(t.Where(v => v.N1 <= 3 && v.N2 == 3 && v.N3 == 1), dst);
        dst.Clear();
        Assert.Equal((ulong)t.Count(v => v.N2 == 1 && v.N3 == 1),
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any),
                new[] { Orderer.Ascending((ThreeUlongs v) => v.N1) }));
        AssertSameCondition(t.Where(v => v.N2 == 1 && v.N3 == 1), dst);
        dst.Clear();
        Assert.Equal((ulong)t.Count(v => v.N1 <= 3 && v.N2 == 3 && v.N3 == 1),
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.UpTo(3),
                Constraint.First(Constraint.Unsigned.Predicate(v => v > 2)),
                new[] { Orderer.Ascending((ThreeUlongs v) => v.N1) }));
        AssertSameCondition(t.Where(v => v.N1 <= 3 && v.N2 == 3 && v.N3 == 1), dst);
    }

    [Fact]
    public void GatherOrderingBySecondColumnWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count,
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Ascending((ThreeUlongs v) => v.N2) }));
        AssertSameCondition(t.OrderBy(v => v.N2).ThenBy(v => v.N1).ThenBy(v => v.N3), dst);
    }

    [Fact]
    public void GatherDescendingOrderingBySecondColumnWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count,
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Descending((ThreeUlongs v) => v.N2) }));
        AssertSameCondition(t.OrderByDescending(v => v.N2).ThenBy(v => v.N1).ThenBy(v => v.N3), dst);
    }

    [Fact]
    public void GatherOrderingByThirdColumnWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count,
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Ascending((ThreeUlongs v) => v.N3) }));
        AssertSameCondition(t.OrderBy(v => v.N3).ThenBy(v => v.N1).ThenBy(v => v.N2), dst);
    }

    [Fact]
    public void GatherOrderingByThirdAndSecondColumnWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count,
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Ascending((ThreeUlongs v) => v.N3), Orderer.Ascending((ThreeUlongs v) => v.N2) }));
        AssertSameCondition(t.OrderBy(v => v.N3).ThenBy(v => v.N2).ThenBy(v => v.N1), dst);
    }

    [Fact]
    public void GatherConstraintAndOrderingByThirdAndSecondColumnWorks()
    {
        FillThreeUlongsData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Equal((ulong)t.Count(v => v.N1 > 2),
            t.GatherById(dst, 0, 1000, Constraint.Unsigned.Predicate(v => v > 2), Constraint.Unsigned.Any,
                new[] { Orderer.Ascending((ThreeUlongs v) => v.N3), Orderer.Descending((ThreeUlongs v) => v.N2) }));
        AssertSameCondition(t.Where(v => v.N1 > 2).OrderBy(v => v.N3).ThenByDescending(v => v.N2).ThenBy(v => v.N1),
            dst);
    }

    [Fact]
    public void GatherOrderedByNonIndexedColumnThrows()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreeUlongsTable>();
        var dst = new List<ThreeUlongs>();
        Assert.Contains("Unmatched orderer[2] Id", Assert.Throws<BTDBException>(() =>
        {
            t.GatherById(dst, 0, 1, Constraint.Unsigned.Any, Constraint.Unsigned.Any,
                new[]
                {
                    Orderer.Ascending((ThreeUlongs v) => v.N3), Orderer.Descending((ThreeUlongs v) => v.N2),
                    Orderer.Ascending((ThreeUlongs v) => v.Id)
                });
        }).Message);
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

        ulong GatherByName(List<ThingWithSK> target, long skip, long take, Constraint<string> name,
            Constraint<ulong> age, IOrderer[] orderers);

        ThingWithSK? FirstByNameOrDefault(Constraint<string> name,
            Constraint<ulong> age, IOrderer[] orderers);

        ThingWithSK FirstByName(Constraint<string> name,
            Constraint<ulong> age, IOrderer[] orderers);
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

    [Fact]
    public void GatherBySecondaryKeyWithOrderersWorks()
    {
        FillThingWithSKData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        var target = new List<ThingWithSK>();
        Assert.Equal((ulong)t.Count,
            t.GatherByName(target, 0, 100, Constraint.String.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Descending((ThingWithSK v) => v.Tenant) }));
        Assert.Equal("DCAB", string.Concat(target.Select(v => v.Name)));
        target.Clear();
        Assert.Equal((ulong)t.Count,
            t.GatherByName(target, 1, 2, Constraint.String.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Descending((ThingWithSK v) => v.Tenant) }));
        Assert.Equal("CA", string.Concat(target.Select(v => v.Name)));
    }

    [Fact]
    public void FirstBySecondaryKeyWithOrderersWorks()
    {
        FillThingWithSKData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        Assert.Equal("D",
            t.FirstByName(Constraint.String.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Descending((ThingWithSK v) => v.Tenant) }).Name);
        Assert.Throws<BTDBException>(() =>
            t.FirstByName(Constraint.String.Exact("NotExisting"), Constraint.Unsigned.Any,
                new[] { Orderer.Descending((ThingWithSK v) => v.Tenant) }));
    }

    [Fact]
    public void FirstBySecondaryKeyOrDefaultWithOrderersWorks()
    {
        FillThingWithSKData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        Assert.Equal("D",
            t.FirstByNameOrDefault(Constraint.String.Any, Constraint.Unsigned.Any,
                new[] { Orderer.Descending((ThingWithSK v) => v.Tenant) })!.Name);
        Assert.Null(t.FirstByNameOrDefault(Constraint.String.Exact("NotExisting"), Constraint.Unsigned.Any,
            new[] { Orderer.Descending((ThingWithSK v) => v.Tenant) }));
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

    [Fact]
    public void GatherBySecondaryKeyWithLocaleOrderersWorks()
    {
        FillThingWithSKData2();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        var target = new List<ThingWithSK>();
        Assert.Equal((ulong)t.Count,
            t.GatherByName(target, 0, 100, Constraint.String.Any, Constraint.Unsigned.Any,
                new[]
                {
                    Orderer.AscendingStringByLocale((ThingWithSK v) => v.Name, new CultureInfo("cs").CompareInfo)
                }));
        Assert.Equal("3124", string.Concat(target.Select(v => v.Age.ToString())));
        target.Clear();
        Assert.Equal((ulong)t.Count,
            t.GatherByName(target, 0, 100, Constraint.String.Any, Constraint.Unsigned.Any,
                new[]
                {
                    Orderer.Backwards(Orderer.AscendingStringByLocale((ThingWithSK v) => v.Name,
                        new CultureInfo("cs").CompareInfo))
                }));
        Assert.Equal("4213", string.Concat(target.Select(v => v.Age.ToString())));
        target.Clear();
        Assert.Equal((ulong)t.Count,
            t.GatherByName(target, 0, 100, Constraint.String.Any, Constraint.Unsigned.Any,
                new[]
                {
                    Orderer.AscendingStringByLocale((ThingWithSK v) => v.Name, new CultureInfo("cs").CompareInfo,
                        CompareOptions.IgnoreSymbols)
                }));
        Assert.Equal("1324", string.Concat(target.Select(v => v.Age.ToString())));
        target.Clear();
    }

    void FillThingWithSKData2()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        t.Upsert(new(1, "Ada", 1));
        t.Upsert(new(1, "Ách", 2));
        t.Upsert(new(2, "!Áďo", 3));
        t.Upsert(new(3, "Bob", 4));
        tr.Commit();
    }

    public class TenantProp
    {
        public ulong Tenant { get; set; }
    }

    [Fact]
    public void GenericAscendingDescendingWorks()
    {
        FillThingWithSKData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThingWithSKTable>();
        var target = new List<ThingWithSK>();
        Assert.Equal((ulong)t.Count,
            t.GatherByName(target, 0, 100, Constraint.String.Any, Constraint.Unsigned.Any,
                new[] { Orderer.GenericDescending((TenantProp v) => v.Tenant) }));
        Assert.Equal("DCAB", string.Concat(target.Select(v => v.Name)));
        target.Clear();
        Assert.Equal((ulong)t.Count,
            t.GatherByName(target, 1, 2, Constraint.String.Any, Constraint.Unsigned.Any,
                new[] { Orderer.GenericAscending((TenantProp v) => v.Tenant) }));
        Assert.Equal("BC", string.Concat(target.Select(v => v.Name)));
    }

    public class ThreePrimaryKeys : IEquatable<ThreePrimaryKeys>
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }
        [PrimaryKey(2)] public ulong ItemId { get; set; }
        [PrimaryKey(3)] public ulong Version { get; set; }

        public ThreePrimaryKeys(ulong tenantId, ulong itemId, ulong version)
        {
            TenantId = tenantId;
            ItemId = itemId;
            Version = version;
        }

        public bool Equals(ThreePrimaryKeys? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TenantId == other.TenantId && ItemId == other.ItemId && Version == other.Version;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ThreePrimaryKeys)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(TenantId, ItemId, Version);
        }
    }

    public interface IThreePrimaryKeysTable : IRelation<ThreePrimaryKeys>
    {
        IEnumerable<ThreePrimaryKeys> ScanById(Constraint<ulong> tenantId, Constraint<ulong> itemId);

        IEnumerable<ThreePrimaryKeys> ScanById(Constraint<ulong> tenantId, Constraint<ulong> itemId,
            Constraint<ulong> version);

        ulong GatherById(ICollection<ThreePrimaryKeys> target, long skip, long take, Constraint<ulong> tenantId,
            Constraint<ulong> itemId);

        ulong GatherById(ICollection<ThreePrimaryKeys> target, long skip, long take, Constraint<ulong> tenantId,
            Constraint<ulong> itemId,
            Constraint<ulong> version);
    }

    void FillThreePrimaryKeysWithData()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreePrimaryKeysTable>();
        t.Upsert(new(1, 1, 1));
        t.Upsert(new(1, 1, 2));
        t.Upsert(new(1, 2, 1));
        t.Upsert(new(1, 2, 2));
        t.Upsert(new(2, 1, 1));
        t.Upsert(new(2, 1, 2));
        t.Upsert(new(2, 1, 3));
        t.Upsert(new(2, 1, 4));
        t.Upsert(new(2, 2, 1));
        t.Upsert(new(2, 2, 2));
        t.Upsert(new(2, 2, 3));
        t.Upsert(new(2, 2, 4));
        t.Upsert(new(2, 3, 1));
        t.Upsert(new(2, 3, 2));
        t.Upsert(new(2, 3, 3));
        t.Upsert(new(2, 3, 4));
        tr.Commit();
    }

    [Fact]
    public void CollectListOfUniquePrimaryKeys()
    {
        FillThreePrimaryKeysWithData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreePrimaryKeysTable>();

        var data = t.ScanById(Constraint.Unsigned.Exact(1), Constraint.Unsigned.Any,
            Constraint.First(Constraint.Unsigned.Any)).ToList();

        Assert.Equal(new[] { new ThreePrimaryKeys(1, 1, 1), new ThreePrimaryKeys(1, 2, 1) }, data);

        data.Clear();
        Assert.Equal(2ul, t.GatherById(data, 0, 100, Constraint.Unsigned.Exact(1), Constraint.Unsigned.Any,
            Constraint.First(Constraint.Unsigned.Any)));

        Assert.Equal(new[] { new ThreePrimaryKeys(1, 1, 1), new ThreePrimaryKeys(1, 2, 1) }, data);
    }

    [Fact]
    public void CollectListOfUniquePrimaryKeysOnNonlastLevel()
    {
        FillThreePrimaryKeysWithData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreePrimaryKeysTable>();

        var data = t.ScanById(Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any)).ToList();

        Assert.Equal(new[] { new ThreePrimaryKeys(1, 1, 1), new ThreePrimaryKeys(2, 1, 1) }, data);

        data.Clear();
        Assert.Equal(2ul,
            t.GatherById(data, 0, 100, Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any)));

        Assert.Equal(new[] { new ThreePrimaryKeys(1, 1, 1), new ThreePrimaryKeys(2, 1, 1) }, data);
    }

    [Fact]
    public void CollectListOfUniquePrimaryKeysOnNonlastLevel2()
    {
        FillThreePrimaryKeysWithData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreePrimaryKeysTable>();

        var data = t.ScanById(Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any),
            Constraint.Unsigned.Exact(3)).ToList();

        Assert.Equal(new[] { new ThreePrimaryKeys(2, 1, 3) }, data);

        data.Clear();
        Assert.Equal(1ul,
            t.GatherById(data, 0, 100, Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any),
                Constraint.Unsigned.Exact(3)));

        Assert.Equal(new[] { new ThreePrimaryKeys(2, 1, 3) }, data);
    }

    [Fact]
    public void CollectListOfUniquePrimaryKeysOnNonlastLevel3()
    {
        FillThreePrimaryKeysWithData();

        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IThreePrimaryKeysTable>();

        var data = t.ScanById(Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any),
            Constraint.Unsigned.Exact(2)).ToList();

        Assert.Equal(new[] { new ThreePrimaryKeys(1, 1, 2), new ThreePrimaryKeys(2, 1, 2) }, data);

        data.Clear();
        Assert.Equal(2ul,
            t.GatherById(data, 0, 100, Constraint.Unsigned.Any, Constraint.First(Constraint.Unsigned.Any),
                Constraint.Unsigned.Exact(2)));

        Assert.Equal(new[] { new ThreePrimaryKeys(1, 1, 2), new ThreePrimaryKeys(2, 1, 2) }, data);
    }

    public class Record
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }

        [PrimaryKey(2)] public ulong BatchId { get; set; }

        [PrimaryKey(3)] public ulong MessageId { get; set; }

        [SecondaryKey("Recipient", IncludePrimaryKeyOrder = 1)]
        public string Recipient { get; set; }
    }

    public interface IRecordTable : IRelation<Record>
    {
        ulong GatherByRecipient(ICollection<Record> target, long skip, long take, Constraint<ulong> companyId,
            Constraint<string> recipient);
    }

    [Fact]
    public void CollectListOfSecondaryKeysOnNonlastLevel()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IRecordTable>();
        var r = new Random(1);
        for (var i = 0u; i < 1000u; i++)
        {
            var batches = r.Next(1, 2);
            for (var j = 0u; j < batches; j++)
            {
                var messages = r.Next(1, 1);
                for (var k = 0u; k < messages; k++)
                {
                    var recipient = r.Next(0, 3) switch
                    {
                        1 => "a",
                        2 => "ab",
                        _ => ""
                    };
                    t.Upsert(new()
                    {
                        CompanyId = i + 1000000, BatchId = j + 1000000, MessageId = k + 1000000, Recipient = recipient
                    });
                }
            }
        }

        var res = new List<Record>();
        Assert.Equal(1000u,
            t.GatherByRecipient(res, 0, 1000000, Constraint<ulong>.Any, Constraint.First(Constraint<string>.Any)));
        for (var i = 0u; i < res.Count; i++)
        {
            var record = res[(int)i];
            Assert.Equal(i + 1000000, record.CompanyId);
        }
    }
}
