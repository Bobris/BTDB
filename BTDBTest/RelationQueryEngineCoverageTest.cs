using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Targets relation query paths which the random differential test does not reach: streaming in descending primary
/// key order, checked arithmetic, sets of every value type, enumerator API and unsupported query shapes. Every query
/// result is compared with LINQ-to-Objects over the materialized relation, including thrown exception types.
public class RelationQueryEngineCoverageTest : ObjectDbTestBase
{
    public RelationQueryEngineCoverageTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum Color : byte
    {
        Red,
        Green,
        Blue
    }

    [Generate]
    public class KeyRow
    {
        [PrimaryKey(1)] public uint Group { get; set; }
        [PrimaryKey(2)] public int Item { get; set; }
        [SecondaryKey("Tag")] public string? Tag { get; set; }
        [SecondaryKey("Val")] public int Val { get; set; }
        public long Big { get; set; }
        public double Ratio { get; set; }
        public float Single { get; set; }
        public decimal Price { get; set; }
        public TimeSpan Span { get; set; }
        public DateTimeOffset Seen { get; set; }
        public DateTime When { get; set; }
        public Guid Token { get; set; }
        public short Small { get; set; }
        public ushort USmall { get; set; }
        public sbyte Tiny { get; set; }
        public byte UTiny { get; set; }
        public bool Flag { get; set; }
        public Color Color { get; set; }
        public int? MaybeInt { get; set; }
        public long? MaybeLong { get; set; }
        public double? MaybeDouble { get; set; }
        public decimal? MaybePrice { get; set; }
        public Guid? MaybeToken { get; set; }
        public DateTime? MaybeWhen { get; set; }
        public TimeSpan? MaybeSpan { get; set; }
        public DateTimeOffset? MaybeSeen { get; set; }
        public float? MaybeSingle { get; set; }
        public ulong? MaybeULong { get; set; }
        public string? Note { get; set; }

        public override string ToString() => $"{Group}/{Item}";
    }

    public interface IKeyRowTable : IRelation<KeyRow>
    {
        void Insert(KeyRow row);
    }

    static readonly DateTime BaseDate = new(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    static readonly Guid[] Tokens = [Guid.Empty, new("6f9619ff-8b86-d011-b42d-00cf4fc964ff"), new("00000000-0000-0000-0000-000000000002")];

    List<KeyRow> Fill(int seed, int count = 300)
    {
        var random = new Random(seed);
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IKeyRowTable>();
        var keys = new HashSet<(uint, int)>();
        while (keys.Count < count)
            keys.Add(((uint)random.Next(0, 6), random.Next(-50, 50)));
        foreach (var (group, item) in keys)
        {
            table.Insert(new()
            {
                Group = group,
                Item = item,
                Tag = random.Next(5) == 0 ? null : "t" + random.Next(4),
                Val = random.Next(-5, 6),
                Big = random.Next(4) == 0 ? long.MaxValue - random.Next(3) : random.Next(-1000, 1000),
                Ratio = random.Next(10) == 0 ? double.NaN : random.NextDouble() * 10 - 5,
                Single = (float)(random.NextDouble() * 4 - 2),
                Price = random.Next(-300, 300) / 4m,
                Span = TimeSpan.FromMinutes(random.Next(-100, 100)),
                Seen = new(DateTime.SpecifyKind(BaseDate.AddHours(random.Next(48)), DateTimeKind.Unspecified), TimeSpan.FromHours(random.Next(-2, 3))),
                When = BaseDate.AddDays(random.Next(-10, 10)),
                Token = Tokens[random.Next(Tokens.Length)],
                Small = (short)random.Next(short.MinValue, short.MaxValue),
                USmall = (ushort)random.Next(0, 10),
                Tiny = (sbyte)random.Next(-128, 128),
                UTiny = (byte)random.Next(0, 256),
                Flag = random.Next(2) == 0,
                Color = (Color)random.Next(3),
                MaybeInt = random.Next(3) == 0 ? null : random.Next(-10, 10),
                MaybeLong = random.Next(3) == 0 ? null : random.Next(-10, 10),
                MaybeDouble = random.Next(3) == 0 ? null : random.Next(-10, 10) / 2.0,
                MaybePrice = random.Next(3) == 0 ? null : random.Next(-10, 10) / 4m,
                MaybeToken = random.Next(3) == 0 ? null : Tokens[random.Next(Tokens.Length)],
                MaybeWhen = random.Next(3) == 0 ? null : BaseDate.AddDays(random.Next(3)),
                MaybeSpan = random.Next(3) == 0 ? null : TimeSpan.FromSeconds(random.Next(3)),
                MaybeSeen = random.Next(3) == 0 ? null : new DateTimeOffset(BaseDate.AddDays(random.Next(3))),
                MaybeSingle = random.Next(3) == 0 ? null : random.Next(-3, 3) / 2f,
                MaybeULong = random.Next(3) == 0 ? null : (ulong)random.Next(0, 5),
                Note = random.Next(4) == 0 ? null : "n" + random.Next(10)
            });
        }

        tr.Commit();
        using var readTr = _db.StartReadOnlyTransaction();
        return readTr.GetRelation<IKeyRowTable>().ToList();
    }

    readonly record struct Outcome(string Value, Type? Exception);

    static Outcome Run<TResult>(Func<TResult> func)
    {
        try
        {
            var result = func();
            return new(result switch
            {
                null => "null",
                IEnumerable<KeyRow> rows => string.Join(",", rows),
                IEnumerable enumerable and not string => string.Join(",", enumerable.Cast<object?>().Select(Format)),
                _ => Format(result)
            }, null);
        }
        catch (Exception e) when (e is not NotSupportedException)
        {
            return new("", e.GetType());
        }
    }

    static string Format(object? value) => value switch
    {
        null => "null",
        double d => d.ToString("R"),
        float f => f.ToString("R"),
        _ => value.ToString()!
    };

    void AssertSame<TResult>(List<KeyRow> rows, string name, Func<IQueryable<KeyRow>, TResult> query)
    {
        var expected = Run(() => query(rows.AsQueryable()));
        using var tr = _db.StartReadOnlyTransaction();
        var actual = Run(() => query(tr.GetRelation<IKeyRowTable>().Query<KeyRow>()));
        Assert.True(expected == actual, $"{name}\nexpected {expected.Exception?.Name}: {expected.Value}\nactual   {actual.Exception?.Name}: {actual.Value}");
    }

    public static IEnumerable<object[]> Seeds => Enumerable.Range(1, 4).Select(s => new object[] { s });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void DescendingPrimaryKeyOrderMatchesLinq(int seed)
    {
        var rows = Fill(seed);
        uint[] groups = [1, 3, 4];
        int[] items = [-7, 0, 13, 49];
        var minItem = -20;
        var maxItem = 20;
        var filters = new (string, Expression<Func<KeyRow, bool>>?)[]
        {
            ("none", null),
            ("Group == 2", r => r.Group == 2),
            ("Group < 3", r => r.Group < 3),
            ("Group <= 3", r => r.Group <= 3),
            ("Group > 1 && Group < 5", r => r.Group > 1 && r.Group < 5),
            ("Group >= 2 && Group <= 2", r => r.Group >= 2 && r.Group <= 2),
            ("groups.Contains(Group)", r => groups.Contains(r.Group)),
            ("Group == 1 || Group == 4", r => r.Group == 1 || r.Group == 4),
            ("Group == 2 && Item < 0", r => r.Group == 2 && r.Item < 0),
            ("Group == 2 && Item <= 10", r => r.Group == 2 && r.Item <= 10),
            ("Group == 2 && Item > minItem && Item < maxItem", r => r.Group == 2 && r.Item > minItem && r.Item < maxItem),
            ("Group == 5 && Item >= 100", r => r.Group == 5 && r.Item >= 100),
            ("groups.Contains(Group) && items.Contains(Item)", r => groups.Contains(r.Group) && items.Contains(r.Item)),
            ("Tag == t1", r => r.Tag == "t1"),
            ("Val == 3", r => r.Val == 3),
            ("Val == 3 && Flag", r => r.Val == 3 && r.Flag),
            ("Tag == t2 && Group == 1", r => r.Tag == "t2" && r.Group == 1),
            ("Flag && Price > 0", r => r.Flag && r.Price > 0),
            ("Group > 9", r => r.Group > 9)
        };
        foreach (var (name, filter) in filters)
        {
            IQueryable<KeyRow> Filtered(IQueryable<KeyRow> q) => filter == null ? q : q.Where(filter);
            AssertSame(rows, $"{name} desc", q => Filtered(q).OrderByDescending(r => r.Group).ThenByDescending(r => r.Item).ToList());
            AssertSame(rows, $"{name} desc skip/take", q => Filtered(q).OrderByDescending(r => r.Group).ThenByDescending(r => r.Item).Skip(3).Take(5).ToList());
            AssertSame(rows, $"{name} desc first", q => Filtered(q).OrderByDescending(r => r.Group).ThenByDescending(r => r.Item).FirstOrDefault());
            AssertSame(rows, $"{name} desc last", q => Filtered(q).OrderByDescending(r => r.Group).ThenByDescending(r => r.Item).LastOrDefault());
            AssertSame(rows, $"{name} desc select", q => Filtered(q).OrderByDescending(r => r.Group).ThenByDescending(r => r.Item).Select(r => r.Item).ToList());
            AssertSame(rows, $"{name} asc", q => Filtered(q).OrderBy(r => r.Group).ThenBy(r => r.Item).ToList());
            AssertSame(rows, $"{name} asc skip", q => Filtered(q).OrderBy(r => r.Group).ThenBy(r => r.Item).Skip(7).ToList());
            AssertSame(rows, $"{name} mixed", q => Filtered(q).OrderBy(r => r.Group).ThenByDescending(r => r.Item).ToList());
            AssertSame(rows, $"{name} Item desc", q => Filtered(q).OrderByDescending(r => r.Item).ToList());
        }

        // With the leading key field fixed, descending order of the remaining field streams backwards too.
        AssertSame(rows, "fixed group, Item desc", q => q.Where(r => r.Group == 3).OrderByDescending(r => r.Item).ToList());
        AssertSame(rows, "fixed group, Item desc range", q => q.Where(r => r.Group == 3 && r.Item < 5).OrderByDescending(r => r.Item).Take(4).ToList());
        AssertSame(rows, "fixed group, Group desc", q => q.Where(r => r.Group == 3).OrderByDescending(r => r.Group).ToList());
    }

    [Fact]
    public void DescendingEnumerationCanBeRestartedAndEnumeratedNonGenerically()
    {
        var rows = Fill(11, 60);
        using var tr = _db.StartReadOnlyTransaction();
        var query = tr.GetRelation<IKeyRowTable>().Query<KeyRow>().Where(r => r.Group < 4)
            .OrderByDescending(r => r.Group).ThenByDescending(r => r.Item);
        var expected = rows.Where(r => r.Group < 4).OrderByDescending(r => r.Group).ThenByDescending(r => r.Item)
            .Select(r => r.ToString()).ToList();
        var nonGeneric = new List<string>();
        foreach (var item in (IEnumerable)query) nonGeneric.Add(item!.ToString()!);
        Assert.Equal(expected, nonGeneric);
        Assert.Equal(expected, query.AsEnumerable().Select(r => r.ToString()).ToList());
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void CheckedAndUncheckedArithmeticMatchesLinq(int seed)
    {
        var rows = Fill(seed, 120);
        var three = 3;
        AssertSame(rows, "checked Big + 10", q => q.Count(r => checked(r.Big + 10) > 0));
        AssertSame(rows, "checked Big - x", q => q.Count(r => checked(r.Big - -three) > 0));
        AssertSame(rows, "checked Big * 2", q => q.Count(r => checked(r.Big * 2) > 0));
        AssertSame(rows, "checked -Big", q => q.Count(r => checked(-r.Big) < 0));
        AssertSame(rows, "checked Val * huge", q => q.Count(r => checked(r.Val * 1_000_000_000) > 0));
        AssertSame(rows, "checked Small - big", q => q.Count(r => checked(r.Small - int.MaxValue) < 0));
        AssertSame(rows, "unchecked Big * 2", q => q.Count(r => unchecked(r.Big * 2) > 0));
        AssertSame(rows, "Val % 3", q => q.Count(r => r.Val % 3 == 1));
        AssertSame(rows, "Val % three", q => q.Count(r => r.Val % three == -1));
        AssertSame(rows, "Item % Val", q => q.Count(r => r.Item % r.Val == 0));
        AssertSame(rows, "Price % 2", q => q.Count(r => r.Price % 2m == 0.5m));
        AssertSame(rows, "Ratio % 2", q => q.Count(r => r.Ratio % 2 > 1));
        AssertSame(rows, "+Val", q => q.Count(r => +r.Val > 0));
        AssertSame(rows, "-Price", q => q.Count(r => -r.Price > 10));
        AssertSame(rows, "checked uint", q => q.Count(r => checked(r.Group - 2u) > 0));
        AssertSame(rows, "checked short sum", q => q.Count(r => checked((short)(r.Small + r.Small)) > 0));
        AssertSame(rows, "Select checked", q => q.Select(r => checked(r.Big + 1)).ToList());
        AssertSame(rows, "Sum checked", q => q.Sum(r => checked(r.Val * 2)));
        AssertSame(rows, "nullable arithmetic", q => q.Count(r => checked(r.MaybeInt * 3) > 4));
        AssertSame(rows, "nullable modulo", q => q.Count(r => r.MaybeLong % 3 == 0));
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void SetsOfEveryValueTypeMatchLinq(int seed)
    {
        var rows = Fill(seed, 120);
        var prices = new[] { 0m, 0.25m, -1.5m, 10m };
        var spans = new[] { TimeSpan.Zero, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(-7) };
        var seen = rows.Take(5).Select(r => r.Seen).ToArray();
        var whens = new[] { BaseDate, BaseDate.AddDays(1), BaseDate.AddDays(-3) };
        var tokens = new HashSet<Guid> { Tokens[1], Tokens[2] };
        var bools = new HashSet<bool> { true };
        var bytes = new HashSet<byte> { 1, 2, 200 };
        var sbytes = new HashSet<sbyte> { -1, 5, 100 };
        var ushorts = new HashSet<ushort> { 0, 3 };
        var shorts = new HashSet<short> { 0, rows[0].Small };
        var uints = new HashSet<uint> { 0, 2 };
        var ints = new HashSet<int> { -3, 0, 4 };
        var doubles = new HashSet<double> { rows[1].Ratio, double.NaN };
        var floats = new HashSet<float> { rows[2].Single };
        var decimalSet = new HashSet<decimal>(prices);
        var spanSet = new HashSet<TimeSpan>(spans);
        var seenSet = new HashSet<DateTimeOffset>(seen);
        var whenSet = new HashSet<DateTime>(whens);
        var colors = new[] { Color.Blue, Color.Red };
        AssertSame(rows, "decimal[]", q => q.Count(r => prices.Contains(r.Price)));
        AssertSame(rows, "TimeSpan[]", q => q.Count(r => spans.Contains(r.Span)));
        AssertSame(rows, "DateTimeOffset[]", q => q.Count(r => seen.Contains(r.Seen)));
        AssertSame(rows, "DateTime[]", q => q.Count(r => whens.Contains(r.When)));
        AssertSame(rows, "HashSet<Guid>", q => q.Count(r => tokens.Contains(r.Token)));
        AssertSame(rows, "HashSet<bool>", q => q.Count(r => bools.Contains(r.Flag)));
        AssertSame(rows, "HashSet<byte>", q => q.Count(r => bytes.Contains(r.UTiny)));
        AssertSame(rows, "HashSet<sbyte>", q => q.Count(r => sbytes.Contains(r.Tiny)));
        AssertSame(rows, "HashSet<ushort>", q => q.Count(r => ushorts.Contains(r.USmall)));
        AssertSame(rows, "HashSet<short>", q => q.Count(r => shorts.Contains(r.Small)));
        AssertSame(rows, "HashSet<uint>", q => q.Count(r => uints.Contains(r.Group)));
        AssertSame(rows, "HashSet<int>", q => q.Count(r => ints.Contains(r.Val)));
        AssertSame(rows, "HashSet<double>", q => q.Count(r => doubles.Contains(r.Ratio)));
        AssertSame(rows, "HashSet<float>", q => q.Count(r => floats.Contains(r.Single)));
        AssertSame(rows, "HashSet<decimal>", q => q.Count(r => decimalSet.Contains(r.Price)));
        AssertSame(rows, "HashSet<TimeSpan>", q => q.Count(r => spanSet.Contains(r.Span)));
        AssertSame(rows, "HashSet<DateTimeOffset>", q => q.Count(r => seenSet.Contains(r.Seen)));
        AssertSame(rows, "HashSet<DateTime>", q => q.Count(r => whenSet.Contains(r.When)));
        AssertSame(rows, "enum[]", q => q.Count(r => colors.Contains(r.Color)));
        AssertSame(rows, "!decimal[]", q => q.Where(r => !prices.Contains(r.Price)).Select(r => r.Item).ToList());
    }

    [Fact]
    public void ContainsOnSetsOfBoolWorks()
    {
        var rows = Fill(7, 40);
        var trueSet = new HashSet<bool> { true };
        bool[] bothArray = [false, true];
        bool[] emptyArray = [];
        AssertSame(rows, "HashSet<bool>", q => q.Count(r => trueSet.Contains(r.Flag)));
        AssertSame(rows, "!HashSet<bool>", q => q.Where(r => !trueSet.Contains(r.Flag)).Select(r => r.Item).ToList());
        AssertSame(rows, "bool[] both", q => q.Count(r => bothArray.Contains(r.Flag)));
        AssertSame(rows, "bool[] empty", q => q.Count(r => emptyArray.Contains(r.Flag)));
        Assert.InRange(rows.Count(r => r.Flag), 1, rows.Count - 1);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void NullableSelectorsAndAggregatesOfEveryTypeMatchLinq(int seed)
    {
        var rows = Fill(seed, 120);
        AssertSame(rows, "MaybeLong", q => q.Select(r => r.MaybeLong).ToList());
        AssertSame(rows, "MaybeULong", q => q.Select(r => r.MaybeULong).ToList());
        AssertSame(rows, "MaybeDouble", q => q.Select(r => r.MaybeDouble).ToList());
        AssertSame(rows, "MaybeSingle", q => q.Select(r => r.MaybeSingle).ToList());
        AssertSame(rows, "MaybePrice", q => q.Select(r => r.MaybePrice).ToList());
        AssertSame(rows, "MaybeToken", q => q.Select(r => r.MaybeToken).ToList());
        AssertSame(rows, "MaybeWhen", q => q.Select(r => r.MaybeWhen).ToList());
        AssertSame(rows, "MaybeSpan", q => q.Select(r => r.MaybeSpan).ToList());
        AssertSame(rows, "MaybeSeen", q => q.Select(r => r.MaybeSeen).ToList());
        AssertSame(rows, "MaybeInt + 1", q => q.Select(r => r.MaybeInt + 1).ToList());
        AssertSame(rows, "(long?)MaybeInt", q => q.Select(r => (long?)r.MaybeInt).ToList());
        AssertSame(rows, "(double?)MaybeLong", q => q.Select(r => (double?)r.MaybeLong).ToList());
        AssertSame(rows, "Min MaybeSpan", q => q.Min(r => r.MaybeSpan));
        AssertSame(rows, "Max MaybeSeen", q => q.Max(r => r.MaybeSeen));
        AssertSame(rows, "Max MaybeToken", q => q.Max(r => r.MaybeToken));
        AssertSame(rows, "Min Seen", q => q.Min(r => r.Seen));
        AssertSame(rows, "Max Span", q => q.Max(r => r.Span));
        AssertSame(rows, "Average MaybePrice", q => q.Average(r => r.MaybePrice));
        AssertSame(rows, "Average MaybeSingle", q => q.Average(r => r.MaybeSingle));
        AssertSame(rows, "Sum MaybeSingle", q => q.Sum(r => r.MaybeSingle));
        AssertSame(rows, "LongCount", q => q.LongCount());
        AssertSame(rows, "LongCount pred", q => q.LongCount(r => r.Group == 2));
        AssertSame(rows, "All key range", q => q.All(r => r.Group < 3));
        AssertSame(rows, "Where All key range", q => q.Where(r => r.Group == 1).All(r => r.Group == 1));
        AssertSame(rows, "Select LongCount", q => q.Select(r => r.Val).LongCount());
        AssertSame(rows, "Select Count", q => q.Select(r => r.Val).Count());
        AssertSame(rows, "MaybeInt.Value", q => q.Where(r => r.Group == 1).Select(r => r.MaybeInt!.Value).ToList());
        AssertSame(rows, "MaybeInt.HasValue", q => q.Count(r => r.MaybeInt.HasValue && r.MaybeInt.Value > 0));
    }

    public class Projection
    {
        public int Item { get; set; }
        public long? MaybeLong { get; set; }
        public decimal Price { get; set; }
        public Guid? MaybeToken { get; set; }
        public Color Color { get; set; }
        public bool Flag { get; set; }
        public string? Note { get; set; }
        public double Ratio;

        public override string ToString() => $"{Item}:{MaybeLong}:{Price}:{MaybeToken}:{Color}:{Flag}:{Note}:{Ratio:R}";
    }

    public record ProjectionRecord(int Item, decimal? MaybePrice);

    [Theory]
    [MemberData(nameof(Seeds))]
    public void MemberInitProjectionsMatchLinq(int seed)
    {
        var rows = Fill(seed, 80);
        AssertSame(rows, "member init", q => q.Select(r => new Projection
        {
            Item = r.Item, MaybeLong = r.MaybeLong, Price = r.Price * 2, MaybeToken = r.MaybeToken, Color = r.Color,
            Flag = r.Flag, Note = r.Note, Ratio = r.Ratio
        }).ToList());
        AssertSame(rows, "record", q => q.Where(r => r.Group > 2).Select(r => new ProjectionRecord(r.Item, r.MaybePrice)).ToList());
        AssertSame(rows, "single arg tuple", q => q.Select(r => Tuple.Create(r.Item)).ToList());
        AssertSame(rows, "computed nullable", q => q.Select(r => new Projection { Item = r.Item + 1, MaybeLong = r.MaybeLong * 2 }).ToList());
    }

    // Each shape is one intercepted call site, executed both over LINQ-to-Objects and over the relation, so cached
    // plans must pick up new captured values and the current content of captured collections on every execution.
    static List<string> MemberInitShape(IQueryable<KeyRow> q, long offset, int minVal) =>
        q.Where(r => r.Val >= minVal).Select(r => new Projection { Item = r.Item, MaybeLong = r.Big + offset, Price = r.Price })
            .AsEnumerable().Select(p => p.ToString()).ToList();

    static List<string> AnonymousShape(IQueryable<KeyRow> q, int factor) =>
        q.Select(r => new { r.Item, X = r.Val * factor, r.Note }).AsEnumerable().Select(a => a.ToString()).ToList();

    static int ArrayShape(IQueryable<KeyRow> q, int[] set) => q.Count(r => set.Contains(r.Val));

    static int HashSetShape(IQueryable<KeyRow> q, HashSet<int> set) => q.Count(r => set.Contains(r.Val));

    static int ListShape(IQueryable<KeyRow> q, List<uint> groups) => q.Count(r => groups.Contains(r.Group));

    static List<int> PagingShape(IQueryable<KeyRow> q, int skip, int take) =>
        q.OrderBy(r => r.Val).ThenBy(r => r.Group).ThenBy(r => r.Item).Skip(skip).Take(take).Select(r => r.Item).ToList();

    static string FirstOrDefaultShape(IQueryable<KeyRow> q, uint group, KeyRow fallback) =>
        q.Where(r => r.Group == group).FirstOrDefault(fallback).ToString();

    static string MinShape(IQueryable<KeyRow> q, int threshold) => Format(q.Where(r => r.Val > threshold).Min(r => r.MaybeInt));

    static List<string> NotesShape(IQueryable<KeyRow> q, string prefix) =>
        q.Where(r => r.Note != null && r.Note.StartsWith(prefix, StringComparison.Ordinal)).Select(r => r.Note!).ToList();

    [Fact]
    public void ReusedCallSitesSeeNewCapturedValuesAndCollectionContent()
    {
        var rows = Fill(13, 150);
        using var tr = _db.StartReadOnlyTransaction();
        var relation = tr.GetRelation<IKeyRowTable>();
        var linq = rows.AsQueryable();
        var array = new[] { 1, 2 };
        var hashSet = new HashSet<int> { 0 };
        var list = new List<uint> { 1 };
        var fallback = new KeyRow { Group = 99, Item = 99 };
        for (var i = 0; i < 6; i++)
        {
            // The same collection instances change their content between executions.
            array[i % 2] = i - 3;
            hashSet.Add(i - 2);
            if (i % 2 == 0) list.Add((uint)i);
            else list.Remove((uint)(i - 1));
            Assert.Equal(MemberInitShape(linq, i * 10, i - 3), MemberInitShape(relation.Query<KeyRow>(), i * 10, i - 3));
            Assert.Equal(AnonymousShape(linq, i), AnonymousShape(relation.Query<KeyRow>(), i));
            Assert.Equal(ArrayShape(linq, array), ArrayShape(relation.Query<KeyRow>(), array));
            Assert.Equal(HashSetShape(linq, hashSet), HashSetShape(relation.Query<KeyRow>(), hashSet));
            Assert.Equal(ListShape(linq, list), ListShape(relation.Query<KeyRow>(), list));
            Assert.Equal(PagingShape(linq, i * 3, 5 - i % 3), PagingShape(relation.Query<KeyRow>(), i * 3, 5 - i % 3));
            Assert.Equal(FirstOrDefaultShape(linq, (uint)i, fallback), FirstOrDefaultShape(relation.Query<KeyRow>(), (uint)i, fallback));
            Assert.Equal(MinShape(linq, i - 4), MinShape(relation.Query<KeyRow>(), i - 4));
            Assert.Equal(NotesShape(linq, "n" + i), NotesShape(relation.Query<KeyRow>(), "n" + i));
            Assert.Equal(ArrayShape(linq, [i, -i]), ArrayShape(relation.Query<KeyRow>(), [i, -i]));
        }
    }

    [Fact]
    public void QueryableApiOfRelationQueryWorks()
    {
        Fill(3, 20);
        using var tr = _db.StartReadOnlyTransaction();
        var query = tr.GetRelation<IKeyRowTable>().Query<KeyRow>();
        Assert.Equal(typeof(KeyRow), query.ElementType);
        var filtered = query.Where(r => r.Group == 1);
        Assert.Equal(typeof(KeyRow), filtered.ElementType);
        var projected = filtered.Select(r => r.Item);
        Assert.Equal(typeof(int), projected.ElementType);
        Assert.Equal(filtered.Count(), ((IEnumerable)projected).Cast<object>().Count());
        var nonGeneric = query.Provider.CreateQuery(filtered.Expression);
        Assert.Equal(filtered.Count(), ((IEnumerable)nonGeneric).Cast<KeyRow>().Count());
        Assert.Equal(filtered.Count(), (int)query.Provider.Execute(Expression.Call(typeof(Queryable), nameof(Queryable.Count),
            [typeof(KeyRow)], filtered.Expression))!);
        Assert.Throws<NotSupportedException>(() => query.Provider.Execute(Expression.Constant(1)));
        Assert.Throws<NotSupportedException>(() => query.Provider.CreateQuery(
            Expression.Call(typeof(Queryable), nameof(Queryable.Distinct), [typeof(KeyRow)], query.Expression)));

        using var enumerator = filtered.GetEnumerator();
        var first = new List<string>();
        while (enumerator.MoveNext()) first.Add(enumerator.Current.ToString());
        enumerator.Reset();
        var second = new List<string>();
        while (enumerator.MoveNext()) second.Add(((IEnumerator)enumerator).Current!.ToString()!);
        Assert.Equal(first, second);

        using var projectedEnumerator = projected.GetEnumerator();
        Assert.True(projectedEnumerator.MoveNext() || first.Count == 0);
        if (first.Count > 0) Assert.IsType<int>(((IEnumerator)projectedEnumerator).Current);
        Assert.Throws<NotSupportedException>(() => projectedEnumerator.Reset());

        using var sorted = query.OrderBy(r => r.Val).GetEnumerator();
        Assert.True(sorted.MoveNext());
        Assert.NotNull(((IEnumerator)sorted).Current);
        Assert.Throws<NotSupportedException>(() => sorted.Reset());
    }

    [Fact]
    public void UnsupportedQueryShapesThrowNotSupportedException()
    {
        var rows = Fill(5, 40);
        using var tr = _db.StartReadOnlyTransaction();
        var query = tr.GetRelation<IKeyRowTable>().Query<KeyRow>();
        var ordinalIgnoreCase = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "t1" };
        var customInts = new HashSet<int>(EqualityComparer<int>.Create((a, b) => a == b, a => a)) { 1 };
        var nullableSet = new[] { (int?)1, null };
        var colorSet = new HashSet<Color> { Color.Red };
        Func<int, bool> isPositive = v => v > 0;
        var parameter = Expression.Parameter(typeof(KeyRow), "r");
        var cases = new (string Name, Action Run)[]
        {
            ("Count lambda after Select", () => query.Select(r => r.Val).Count(v => v > 0)),
            ("Distinct", () => query.Distinct().ToList()),
            ("Distinct after Select", () => query.Select(r => r.Val).Distinct().ToList()),
            ("Reverse", () => query.Reverse().ToList()),
            ("OrderBy after Skip", () => query.Skip(1).OrderBy(r => r.Val).ToList()),
            ("OrderBy Note.Length", () => query.OrderBy(r => r.Note!.Length + "").ToList()),
            ("OrderBy throwing key", () => query.OrderBy(r => r.Item / r.Val).ToList()),
            ("string.Compare culture", () => query.Count(r => string.Compare(r.Note, "x", StringComparison.CurrentCulture) > 0)),
            ("string.Concat", () => query.Count(r => string.Concat(r.Note, "x") == "y")),
            ("ToUpper", () => query.Count(r => r.Note!.ToUpper() == "N1")),
            ("IndexOf", () => query.Count(r => r.Note!.IndexOf('n') == 0)),
            ("StartsWith row arg", () => query.Count(r => r.Note!.StartsWith(r.Tag!))),
            ("Equals char", () => query.Count(r => r.Note!.Equals((object)"x"))),
            ("Contains with comparer", () => query.Count(r => ordinalIgnoreCase.Contains(r.Tag))),
            ("Contains custom int comparer", () => query.Count(r => customInts.Contains(r.Val))),
            ("Contains enum HashSet", () => query.Count(r => colorSet.Contains(r.Color))),
            ("Contains nullable", () => query.Count(r => nullableSet.Contains(r.MaybeInt))),
            ("Conditional", () => query.Count(r => (r.Flag ? r.Val : r.Item) > 0)),
            ("Coalesce", () => query.Count(r => (r.MaybeInt ?? 0) > 0)),
            ("Select string concat", () => query.Select(r => r.Note + "x").ToList()),
            ("bitwise and", () => query.Count(r => (r.Val & 1) == 1)),
            ("ToString", () => query.Count(r => r.Val.ToString() == "1")),
            ("Select into list", () => query.Select(r => new List<int> { r.Val }).ToList()),
            ("Any with string compare", () => query.Any(r => r.Note!.CompareTo("x") > 0)),
            ("type test", () => query.Count(r => (object?)r.Note is string)),
            ("delegate invocation", () => query.Count(r => isPositive(r.Val))),
            ("Select negated bool", () => query.Select(r => !r.Flag).ToList())
        };
        var failures = new List<string>();
        foreach (var (name, run) in cases)
        {
            try
            {
                run();
                failures.Add(name + ": no exception");
            }
            catch (NotSupportedException)
            {
            }
            catch (Exception e)
            {
                failures.Add($"{name}: {e.GetType().Name} {e.Message}");
            }
        }

        Assert.Empty(failures);
        var equalsDefault = Expression.Lambda<Func<KeyRow, bool>>(
            Expression.Equal(Expression.Property(parameter, nameof(KeyRow.Val)), Expression.Default(typeof(int))),
            parameter);
        Assert.Equal(rows.Count(r => r.Val == 0), query.Count(equalsDefault));
    }
}
