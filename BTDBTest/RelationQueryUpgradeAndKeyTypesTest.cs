using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using BTDB;
using BTDB.ODBLayer;
using Microsoft.Extensions.Primitives;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Relation queries over rows stored by an older relation version (fields added later are substituted by their
/// default values and folded into constants), over primary keys of non-integer types, and over rows whose skipped value
/// fields use less common field handlers. Results are compared with LINQ-to-Objects over the materialized relation.
public class RelationQueryUpgradeAndKeyTypesTest : ObjectDbTestBase
{
    public RelationQueryUpgradeAndKeyTypesTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum Shade
    {
        None,
        Light,
        Dark
    }

    [Generate]
    public class UpV1
    {
        [PrimaryKey(1)] public uint Id { get; set; }
        public int A { get; set; }
        public string? S { get; set; }
    }

    public interface IUpV1Table : IRelation<UpV1>
    {
        void Insert(UpV1 row);
    }

    [Generate]
    public class UpV2
    {
        [PrimaryKey(1)] public uint Id { get; set; }
        public int A { get; set; }
        public string? S { get; set; }
        public int N { get; set; }
        public long? NL { get; set; }
        public string? NS { get; set; }
        public bool NB { get; set; }
        public Shade NC { get; set; }
        public decimal ND { get; set; }
        public double NDbl { get; set; }
        public DateTime NDt { get; set; }
        public Guid NG { get; set; }
        public TimeSpan NTs { get; set; }
        public DateTimeOffset NDto { get; set; }
        public float NF { get; set; }
        public byte NU8 { get; set; }
        public sbyte NI8 { get; set; }
        public short NI16 { get; set; }
        public ushort NU16 { get; set; }
        public ulong NU64 { get; set; }
        public bool? NBN { get; set; }

        public override string ToString() => Id.ToString();
    }

    public interface IUpV2Table : IRelation<UpV2>
    {
        void Insert(UpV2 row);
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
                string s => s,
                System.Collections.IEnumerable e => string.Join(",", e.Cast<object?>().Select(o => o?.ToString() ?? "null")),
                _ => result.ToString()!
            }, null);
        }
        catch (Exception e) when (e is not NotSupportedException)
        {
            return new("", e.GetType());
        }
    }

    static void AssertSame<TRow, TResult>(List<TRow> rows, IQueryable<TRow> query, string name,
        Func<IQueryable<TRow>, TResult> run)
    {
        var expected = Run(() => run(rows.AsQueryable()));
        var actual = Run(() => run(query));
        Assert.True(expected == actual,
            $"{name}\nexpected {expected.Exception?.Name}: {expected.Value}\nactual   {actual.Exception?.Name}: {actual.Value}");
    }

    List<UpV2> PrepareUpgradedRelation(bool newRowsFirst)
    {
        // Old rows are stored with ids 1..30 by version 1; new rows get ids before or after them.
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IUpV1Table>("Up")(tr);
            for (var i = 1u; i <= 30; i++)
                table.Insert(new() { Id = i + 100, A = (int)i % 7, S = i % 3 == 0 ? null : "s" + i });
            tr.Commit();
        }

        ReopenDb();
        var random = new Random(5);
        using (var tr = _db.StartTransaction())
        {
            _upCreator = tr.InitRelation<IUpV2Table>("Up");
            var table = _upCreator(tr);
            for (var i = 1u; i <= 30; i++)
            {
                table.Insert(new()
                {
                    Id = newRowsFirst ? i : i + 200,
                    A = random.Next(7),
                    S = "x" + i,
                    N = random.Next(-3, 4),
                    NL = random.Next(3) == 0 ? null : random.Next(5),
                    NS = random.Next(3) == 0 ? null : "ab"[random.Next(2)] + "q" + random.Next(3),
                    NB = random.Next(2) == 0,
                    NC = (Shade)random.Next(3),
                    ND = random.Next(-4, 4) / 2m,
                    NDbl = random.Next(-4, 4) / 2.0,
                    NDt = new DateTime(2024, 1, 1).AddDays(random.Next(3)),
                    NG = random.Next(2) == 0 ? Guid.Empty : new Guid("11111111-2222-3333-4444-555555555555"),
                    NTs = TimeSpan.FromSeconds(random.Next(3)),
                    NDto = new DateTimeOffset(new DateTime(2024, 1, 1).AddHours(random.Next(3)), TimeSpan.Zero),
                    NF = random.Next(-2, 3),
                    NU8 = (byte)random.Next(3),
                    NI8 = (sbyte)random.Next(-2, 3),
                    NI16 = (short)random.Next(-2, 3),
                    NU16 = (ushort)random.Next(3),
                    NU64 = (ulong)random.Next(3),
                    NBN = random.Next(3) == 0 ? null : random.Next(2) == 0
                });
            }

            tr.Commit();
        }

        using var readTr = _db.StartReadOnlyTransaction();
        return _upCreator(readTr).ToList();
    }

    Func<IObjectDBTransaction, IUpV2Table> _upCreator = null!;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void QueriesOverRowsOfOlderVersionMatchLinq(bool newRowsFirst)
    {
        var rows = PrepareUpgradedRelation(newRowsFirst);
        Assert.Equal(60, rows.Count);
        using var tr = _db.StartReadOnlyTransaction();
        var query = _upCreator(tr).Query<UpV2>();
        var guid = new Guid("11111111-2222-3333-4444-555555555555");
        var date = new DateTime(2024, 1, 2);
        var predicates = new (string, System.Linq.Expressions.Expression<Func<UpV2, bool>>)[]
        {
            ("N == 0", r => r.N == 0),
            ("N != 0", r => r.N != 0),
            ("N > 1", r => r.N > 1),
            ("N + A > 2", r => r.N + r.A > 2),
            ("A > 2 && N == 0", r => r.A > 2 && r.N == 0),
            ("A > 2 || N < 0", r => r.A > 2 || r.N < 0),
            ("!(N == 0)", r => !(r.N == 0)),
            ("!!(N == 0)", r => !!(r.N == 0)),
            ("NL == null", r => r.NL == null),
            ("NL != null", r => r.NL != null),
            ("NL.HasValue", r => r.NL.HasValue),
            ("!NL.HasValue", r => !r.NL.HasValue),
            ("NL > 1", r => r.NL > 1),
            ("NL.Value > 1", r => r.NL!.Value > 1),
            ("NL.HasValue && NL.Value > 1", r => r.NL.HasValue && r.NL.Value > 1),
            ("(long)NL == 2", r => (long)r.NL! == 2),
            ("NL * 2 == 4", r => r.NL * 2 == 4),
            ("(int?)N == null", r => (int?)r.N == null),
            ("NS == null", r => r.NS == null),
            ("NS != null", r => r.NS != null),
            ("NS == aq1", r => r.NS == "aq1"),
            ("string.Equals(NS, null)", r => string.Equals(r.NS, null)),
            ("string.Equals(NS, aq1, OrdinalIgnoreCase)", r => string.Equals(r.NS, "AQ1", StringComparison.OrdinalIgnoreCase)),
            ("string.IsNullOrEmpty(NS)", r => string.IsNullOrEmpty(r.NS)),
            ("NS.StartsWith(a)", r => r.NS!.StartsWith("a", StringComparison.Ordinal)),
            ("NS.EndsWith('1')", r => r.NS!.EndsWith('1')),
            ("NS.Contains(q, ord)", r => r.NS!.Contains("q", StringComparison.Ordinal)),
            ("NS.Equals(aq1)", r => r.NS!.Equals("aq1")),
            ("NS.Length > 2", r => r.NS!.Length > 2),
            ("string.CompareOrdinal(NS, b) < 0", r => string.CompareOrdinal(r.NS, "b") < 0),
            ("string.CompareOrdinal(b, NS) < 0", r => string.CompareOrdinal("b", r.NS) < 0),
            ("NS in set", r => new[] { "aq0", null, "bq2" }.Contains(r.NS)),
            ("NS not in set", r => !new[] { "aq0", "bq2" }.Contains(r.NS)),
            ("NB", r => r.NB),
            ("!NB", r => !r.NB),
            ("NB == false", r => r.NB == false),
            ("NBN == true", r => r.NBN == true),
            ("NBN == null", r => r.NBN == null),
            ("NBN.Value", r => r.NBN!.Value),
            ("NC == Light", r => r.NC == Shade.Light),
            ("NC in set", r => new[] { Shade.None, Shade.Dark }.Contains(r.NC)),
            ("ND > 0", r => r.ND > 0),
            ("ND == 0", r => r.ND == 0m),
            ("NDbl < 0", r => r.NDbl < 0),
            ("NDt == date", r => r.NDt == date),
            ("NDt < date", r => r.NDt < date),
            ("NG == guid", r => r.NG == guid),
            ("NG == Empty", r => r.NG == Guid.Empty),
            ("NTs > 0", r => r.NTs > TimeSpan.Zero),
            ("NDto == default", r => r.NDto == default),
            ("NF >= 0", r => r.NF >= 0),
            ("NU8 == 0", r => r.NU8 == 0),
            ("NI8 < 0", r => r.NI8 < 0),
            ("NI16 > 0", r => r.NI16 > 0),
            ("NU16 == 1", r => r.NU16 == 1),
            ("NU64 == 0", r => r.NU64 == 0),
            ("N in set", r => new[] { 0, 2 }.Contains(r.N)),
            ("checked(N - 1) < 0", r => checked(r.N - 1) < 0),
            ("1 / N > 0", r => 1 / r.N > 0),
            ("N / 1 == 0", r => r.N / 1 == 0)
        };
        foreach (var (name, predicate) in predicates)
        {
            AssertSame(rows, query, name, q => q.Where(predicate).Select(r => r.Id).ToList());
            AssertSame(rows, query, name + " count", q => q.Count(predicate));
            AssertSame(rows, query, name + " any", q => q.Any(predicate));
            AssertSame(rows, query, name + " first", q => q.FirstOrDefault(predicate));
        }

        AssertSame(rows, query, "Select N", q => q.Select(r => r.N).ToList());
        AssertSame(rows, query, "Select NL", q => q.Select(r => r.NL).ToList());
        AssertSame(rows, query, "Select NS", q => q.Select(r => r.NS).ToList());
        AssertSame(rows, query, "Select NC", q => q.Select(r => r.NC).ToList());
        AssertSame(rows, query, "Select N * 2", q => q.Select(r => r.N * 2).ToList());
        AssertSame(rows, query, "Select NL + 1", q => q.Select(r => r.NL + 1).ToList());
        AssertSame(rows, query, "Select NL.Value", q => q.Select(r => r.NL!.Value).ToList());
        AssertSame(rows, query, "Select (N, NS)", q => q.Select(r => new { r.Id, r.N, r.NS, r.ND }).ToList());
        AssertSame(rows, query, "OrderBy N", q => q.OrderBy(r => r.N).ThenBy(r => r.Id).Select(r => r.Id).ToList());
        AssertSame(rows, query, "OrderByDescending NDt", q => q.OrderByDescending(r => r.NDt).Select(r => r.Id).ToList());
        AssertSame(rows, query, "OrderBy NS", q => q.OrderBy(r => r.NS).Select(r => r.Id).ToList());
        AssertSame(rows, query, "Sum N", q => q.Sum(r => r.N));
        AssertSame(rows, query, "Sum NL", q => q.Sum(r => r.NL));
        AssertSame(rows, query, "Average ND", q => q.Average(r => r.ND));
        AssertSame(rows, query, "Min NDt", q => q.Min(r => r.NDt));
        AssertSame(rows, query, "Max NL", q => q.Max(r => r.NL));
        AssertSame(rows, query, "Max NG", q => q.Max(r => r.NG));
        AssertSame(rows, query, "Where then Sum", q => q.Where(r => r.N >= 0).Sum(r => r.N + r.A));
    }

    [Generate]
    public class KeyDateTime
    {
        [PrimaryKey(1)] public DateTime K { get; set; }
        public int V { get; set; }
    }

    public interface IKeyDateTimeTable : IRelation<KeyDateTime>
    {
        void Insert(KeyDateTime row);
    }

    [Generate]
    public class KeyGuid
    {
        [PrimaryKey(1)] public Guid K { get; set; }
        public int V { get; set; }
    }

    public interface IKeyGuidTable : IRelation<KeyGuid>
    {
        void Insert(KeyGuid row);
    }

    [Generate]
    public class KeyTimeSpan
    {
        [PrimaryKey(1)] public TimeSpan K { get; set; }
        [PrimaryKey(2)] public int Sub { get; set; }
        public int V { get; set; }
    }

    public interface IKeyTimeSpanTable : IRelation<KeyTimeSpan>
    {
        void Insert(KeyTimeSpan row);
    }

    [Generate]
    public class KeyDateTimeOffset
    {
        [PrimaryKey(1)] public DateTimeOffset K { get; set; }
        [PrimaryKey(2)] public int Sub { get; set; }
        public int V { get; set; }
    }

    public interface IKeyDateTimeOffsetTable : IRelation<KeyDateTimeOffset>
    {
        void Insert(KeyDateTimeOffset row);
    }

    [Generate]
    public class KeySByte
    {
        [PrimaryKey(1)] public sbyte K { get; set; }
        [PrimaryKey(2)] public int Sub { get; set; }
        public sbyte V { get; set; }
        public float F { get; set; }
        public IPAddress? Ip { get; set; }
        public Version? Ver { get; set; }
        public StringValues Sv { get; set; }
        public int After { get; set; }
    }

    public interface IKeySByteTable : IRelation<KeySByte>
    {
        void Insert(KeySByte row);
    }

    [Generate]
    public class ValueEqualityKeys
    {
        [PrimaryKey(1)] public int Group { get; set; }
        [PrimaryKey(2)] public DateTimeOffset At { get; set; }
        [PrimaryKey(3)] public DateTime When { get; set; }
        public decimal Amount { get; set; }
        [SecondaryKey("Seen")] public DateTimeOffset Seen { get; set; }
        [SecondaryKey("Stamp")] public DateTime Stamp { get; set; }
        public int V { get; set; }

        public override string ToString() => V.ToString();
    }

    public interface IValueEqualityKeysTable : IRelation<ValueEqualityKeys>
    {
        void Insert(ValueEqualityKeys row);
    }

    /// C# equality of DateTimeOffset ignores the offset and of DateTime ignores Kind, while their stored key bytes keep
    /// them; equality conditions on such key fields must still match like LINQ. Decimal equality ignores scale too, but
    /// decimal key fields are refused, so it is checked on a value.
    [Fact]
    public void EqualityOnKeysWithNonCanonicalEncodingMatchesLinq()
    {
        var instant = new DateTime(2024, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IValueEqualityKeysTable>();
            var v = 0;
            var group = 0;
            foreach (var price in new[] { 1.5m, 2.50m, 3m })
            {
                group++;
                foreach (var hours in new[] { 0, 2 })
                {
                    table.Insert(new()
                    {
                        Group = group, At = new(instant.AddHours(hours)), When = instant.AddDays(hours),
                        Amount = price, Seen = new(instant.AddHours(hours)), Stamp = instant.AddDays(hours), V = v++
                    });
                }
            }

            tr.Commit();
        }

        using var readTr = _db.StartReadOnlyTransaction();
        var relation = readTr.GetRelation<IValueEqualityKeysTable>();
        var rows = relation.ToList();
        var query = relation.Query<ValueEqualityKeys>();
        var price250 = 2.500m;
        var price15 = 1.50m;
        var shiftedAt = new DateTimeOffset(DateTime.SpecifyKind(instant.AddHours(2 + 3), DateTimeKind.Unspecified),
            TimeSpan.FromHours(3));
        var unspecifiedWhen = DateTime.SpecifyKind(instant.AddDays(2), DateTimeKind.Unspecified);
        var prices = new[] { 1.500m, 3.0m };
        var ats = new[] { shiftedAt };
        var whens = new[] { unspecifiedWhen };
        AssertSame(rows, query, "Amount == 1.50m && At", q => q.Where(r => r.Amount == price15 && r.At == shiftedAt).ToList());
        AssertSame(rows, query, "Group, At, When", q => q.Where(r => r.Group == 3 && r.At == shiftedAt && r.When == unspecifiedWhen).ToList());
        AssertSame(rows, query, "Amount == 2.500m", q => q.Where(r => r.Amount == price250).ToList());
        AssertSame(rows, query, "Amount in", q => q.Count(r => prices.Contains(r.Amount)));
        AssertSame(rows, query, "Seen == shifted", q => q.Where(r => r.Seen == shiftedAt).ToList());
        AssertSame(rows, query, "Seen in", q => q.Count(r => ats.Contains(r.Seen)));
        AssertSame(rows, query, "Stamp == unspecified", q => q.Where(r => r.Stamp == unspecifiedWhen).ToList());
        AssertSame(rows, query, "Stamp in", q => q.Count(r => whens.Contains(r.Stamp)));
        AssertSame(rows, query, "Amount == 2.5m exact scale", q => q.Where(r => r.Amount == 2.50m).ToList());
    }

    [Fact]
    public void QueriesOnNonIntegerPrimaryKeysMatchLinq()
    {
        var baseDate = new DateTime(2024, 5, 1, 0, 0, 0, DateTimeKind.Utc);
        var guids = Enumerable.Range(0, 8).Select(i => new Guid(i, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).ToArray();
        using (var tr = _db.StartTransaction())
        {
            var dates = tr.GetRelation<IKeyDateTimeTable>();
            var guidTable = tr.GetRelation<IKeyGuidTable>();
            var spans = tr.GetRelation<IKeyTimeSpanTable>();
            var offsets = tr.GetRelation<IKeyDateTimeOffsetTable>();
            var sbytes = tr.GetRelation<IKeySByteTable>();
            for (var i = 0; i < 8; i++)
            {
                dates.Insert(new() { K = baseDate.AddDays(i), V = i });
                guidTable.Insert(new() { K = guids[i], V = i });
                for (var sub = 0; sub < 3; sub++)
                {
                    spans.Insert(new() { K = TimeSpan.FromMinutes(i - 4), Sub = sub, V = i + sub });
                    offsets.Insert(new() { K = new DateTimeOffset(baseDate.AddHours(i)), Sub = sub, V = i + sub });
                    sbytes.Insert(new()
                    {
                        K = (sbyte)(i * 30 - 120), Sub = sub, V = (sbyte)(sub - 1), F = i / 2f,
                        Ip = sub == 0 ? null : IPAddress.Parse("10.0.0." + i), Ver = sub == 1 ? null : new Version(1, i),
                        Sv = sub == 2 ? StringValues.Empty : new StringValues(["a", "b" + i]), After = i * 10 + sub
                    });
                }
            }

            tr.Commit();
        }

        using var readTr = _db.StartReadOnlyTransaction();
        {
            var table = readTr.GetRelation<IKeyDateTimeTable>();
            var rows = table.ToList();
            var key = baseDate.AddDays(3);
            var keys = new[] { baseDate, baseDate.AddDays(5), baseDate.AddDays(30) };
            AssertSame(rows, table.Query<KeyDateTime>(), "DateTime ==", q => q.Where(r => r.K == key).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyDateTime>(), "DateTime in", q => q.Where(r => keys.Contains(r.K)).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyDateTime>(), "DateTime <", q => q.Where(r => r.K < key).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyDateTime>(), "DateTime desc", q => q.OrderByDescending(r => r.K).Select(r => r.V).ToList());
        }
        {
            var table = readTr.GetRelation<IKeyGuidTable>();
            var rows = table.ToList();
            var key = guids[4];
            AssertSame(rows, table.Query<KeyGuid>(), "Guid ==", q => q.Where(r => r.K == key).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyGuid>(), "Guid !=", q => q.Where(r => r.K != key).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyGuid>(), "Guid in", q => q.Count(r => guids.Take(3).ToArray().Contains(r.K)));
        }
        {
            var table = readTr.GetRelation<IKeyTimeSpanTable>();
            var rows = table.ToList();
            var key = TimeSpan.FromMinutes(-2);
            AssertSame(rows, table.Query<KeyTimeSpan>(), "TimeSpan ==", q => q.Where(r => r.K == key).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyTimeSpan>(), "TimeSpan >=", q => q.Where(r => r.K >= key).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyTimeSpan>(), "TimeSpan == && Sub ==", q => q.Count(r => r.K == key && r.Sub == 2));
        }
        {
            var table = readTr.GetRelation<IKeyDateTimeOffsetTable>();
            var rows = table.ToList();
            var key = new DateTimeOffset(baseDate.AddHours(2));
            AssertSame(rows, table.Query<KeyDateTimeOffset>(), "DateTimeOffset ==", q => q.Where(r => r.K == key).Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeyDateTimeOffset>(), "DateTimeOffset == && Sub", q => q.Count(r => r.K == key && r.Sub == 1));
        }
        {
            var table = readTr.GetRelation<IKeySByteTable>();
            var rows = table.ToList();
            sbyte key = -60;
            AssertSame(rows, table.Query<KeySByte>(), "sbyte ==", q => q.Where(r => r.K == key).Select(r => r.After).ToList());
            AssertSame(rows, table.Query<KeySByte>(), "sbyte <", q => q.Where(r => r.K < 0).Select(r => r.After).ToList());
            AssertSame(rows, table.Query<KeySByte>(), "sbyte >= && V", q => q.Where(r => r.K >= -60 && r.V < 0).Select(r => r.After).ToList());
            AssertSame(rows, table.Query<KeySByte>(), "sbyte desc", q => q.OrderByDescending(r => r.K).ThenByDescending(r => r.Sub).Select(r => r.After).ToList());
            AssertSame(rows, table.Query<KeySByte>(), "select V", q => q.Select(r => r.V).ToList());
            AssertSame(rows, table.Query<KeySByte>(), "skip exotic fields", q => q.Where(r => r.After % 2 == 0).Select(r => r.After).ToList());
            AssertSame(rows, table.Query<KeySByte>(), "float then After", q => q.Count(r => r.F > 1 && r.After > 20));
            AssertSame(rows, table.Query<KeySByte>(), "sum After", q => q.Sum(r => r.After));
        }
    }
}
