using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Compares constraint based queries (ScanBy/GatherBy/FirstBy/LastBy on primary and secondary keys) against a LINQ
// reference model over the same data, using boundary values of all supported key types. Every test runs both with
// source generated relations and with IL emitted relations (RelationBuilder).
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class RelationConstraintDifferentialTest : ObjectDbTestBase
{
    public RelationConstraintDifferentialTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum SignedKind
    {
        Neg = -100,
        MinusOne = -1,
        Zero = 0,
        One = 1,
        Big = 1000
    }

    public enum UnsignedKind : ushort
    {
        None = 0,
        A = 1,
        B = 200,
        Max = ushort.MaxValue
    }

    public class CRow
    {
        [PrimaryKey(1)] public ulong A { get; set; }
        [PrimaryKey(2)] public long B { get; set; }
        [PrimaryKey(3)] public string S { get; set; } = "";

        [SecondaryKey("Mix", Order = 1)] public SignedKind E { get; set; }
        [SecondaryKey("Mix", Order = 2)] public bool Flag { get; set; }
        [SecondaryKey("Mix", Order = 3)] public DateTime Dt { get; set; }

        [SecondaryKey("Other", Order = 1)] public UnsignedKind U { get; set; }
        [SecondaryKey("Other", Order = 2)] public Guid G { get; set; }

        public int Payload { get; set; }

        public override string ToString() => $"{A}|{B}|{S}|{E}|{Flag}|{Dt:O}|{U}|{G}|{Payload}";
    }

    public interface ICRowTable : IRelation<CRow>
    {
        void Insert(CRow row);
        IEnumerable<CRow> ScanById(Constraint<ulong> a);
        IEnumerable<CRow> ScanById(Constraint<ulong> a, Constraint<long> b);
        IEnumerable<CRow> ScanById(Constraint<ulong> a, Constraint<long> b, Constraint<string> s);

        ulong GatherById(List<CRow> target, long skip, long take, Constraint<ulong> a, Constraint<long> b,
            Constraint<string> s);

        ulong GatherById(List<CRow> target, long skip, long take, Constraint<ulong> a, Constraint<long> b,
            Constraint<string> s, IOrderer[] orderers);

        CRow? FirstByIdOrDefault(Constraint<ulong> a, Constraint<long> b, Constraint<string> s);
        CRow? LastByIdOrDefault(Constraint<ulong> a, Constraint<long> b, Constraint<string> s);
        CRow FirstById(Constraint<ulong> a, Constraint<long> b, Constraint<string> s);
        CRow LastById(Constraint<ulong> a, Constraint<long> b, Constraint<string> s);

        CRow? FirstByIdOrDefault(Constraint<ulong> a, Constraint<long> b, Constraint<string> s,
            IOrderer[] orderers);

        CRow? LastByIdOrDefault(Constraint<ulong> a, Constraint<long> b, Constraint<string> s,
            IOrderer[] orderers);

        IEnumerable<CRow> ScanByMix(Constraint<SignedKind> e);
        IEnumerable<CRow> ScanByMix(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt);

        IEnumerable<CRow> ScanByMix(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt,
            Constraint<ulong> a);

        ulong GatherByMix(List<CRow> target, long skip, long take, Constraint<SignedKind> e, Constraint<bool> flag,
            Constraint<DateTime> dt);

        ulong GatherByMix(List<CRow> target, long skip, long take, Constraint<SignedKind> e, Constraint<bool> flag,
            Constraint<DateTime> dt, IOrderer[] orderers);

        CRow? FirstByMixOrDefault(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt);
        CRow? LastByMixOrDefault(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt);

        CRow? FirstByMixOrDefault(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt,
            IOrderer[] orderers);

        CRow? LastByMixOrDefault(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt,
            IOrderer[] orderers);

        CRow FirstByMix(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt);
        CRow LastByMix(Constraint<SignedKind> e, Constraint<bool> flag, Constraint<DateTime> dt);

        IEnumerable<CRow> ScanByOther(Constraint<UnsignedKind> u, Constraint<Guid> g);
    }

    record C<T>(string Name, Constraint<T> Constraint, Func<T, bool> Predicate);

    static readonly ulong[] AValues = [0, 1, 2, 127, 128, 16383, 16384, ulong.MaxValue - 1, ulong.MaxValue];

    static readonly long[] BValues =
        [long.MinValue, long.MinValue + 1, -65, -64, -1, 0, 1, 63, 64, 8192, long.MaxValue - 1, long.MaxValue];

    static readonly string[] SValues =
        ["", "a", "aa", "ab", "abc", "b", "B", "Ab", "é", "ž", "až", "中文", "zzz", "a b"];

    static readonly SignedKind[] EValues = Enum.GetValues<SignedKind>();
    static readonly UnsignedKind[] UValues = Enum.GetValues<UnsignedKind>();

    static readonly DateTime[] DtValues =
    [
        DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc),
        new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        new(2000, 2, 29, 12, 0, 0, DateTimeKind.Utc),
        new(2020, 5, 5, 0, 0, 0, DateTimeKind.Utc),
        DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc)
    ];

    static readonly Guid[] GValues =
    [
        Guid.Empty, new("00000000-0000-0000-0000-000000000001"), new("10000000-0000-0000-0000-000000000000"),
        new("ffffffff-ffff-ffff-ffff-ffffffffffff")
    ];

    static List<C<ulong>> UlongConstraints() =>
    [
        new("Any", Constraint.Unsigned.Any, _ => true),
        new("GenericAny", Constraint<ulong>.Any, _ => true),
        new("Exact0", Constraint.Unsigned.Exact(0), v => v == 0),
        new("Exact128", Constraint.Unsigned.Exact(128), v => v == 128),
        new("ExactMax", Constraint.Unsigned.Exact(ulong.MaxValue), v => v == ulong.MaxValue),
        new("ExactMissing", Constraint.Unsigned.Exact(3), v => v == 3),
        new("GenericExact2", Constraint.Exact<ulong>(2), v => v == 2),
        new("UpTo127", Constraint.Unsigned.UpTo(127), v => v <= 127),
        new("UpTo128Excl", Constraint.Unsigned.UpTo(128, false), v => v < 128),
        new("UpTo0Excl", Constraint.Unsigned.UpTo(0, false), _ => false),
        new("UpToMax", Constraint.Unsigned.UpTo(ulong.MaxValue), _ => true),
        new("UpToMaxExcl", Constraint.Unsigned.UpTo(ulong.MaxValue, false), v => v < ulong.MaxValue),
        new("PredicateOdd", Constraint.Unsigned.Predicate(v => v % 2 == 1), v => v % 2 == 1),
    ];

    static List<C<long>> LongConstraints() =>
    [
        new("Any", Constraint.Signed.Any, _ => true),
        new("ExactMin", Constraint.Signed.Exact(long.MinValue), v => v == long.MinValue),
        new("ExactMinus1", Constraint.Signed.Exact(-1), v => v == -1),
        new("Exact0", Constraint.Signed.Exact(0), v => v == 0),
        new("ExactMax", Constraint.Signed.Exact(long.MaxValue), v => v == long.MaxValue),
        new("GenericExact64", Constraint.Exact<long>(64), v => v == 64),
        new("UpToMinus65", Constraint.Signed.UpTo(-65), v => v <= -65),
        new("UpToMinus64Excl", Constraint.Signed.UpTo(-64, false), v => v < -64),
        new("UpTo0", Constraint.Signed.UpTo(0), v => v <= 0),
        new("UpTo63Excl", Constraint.Signed.UpTo(63, false), v => v < 63),
        new("UpToMin", Constraint.Signed.UpTo(long.MinValue), v => v == long.MinValue),
        new("UpToMinExcl", Constraint.Signed.UpTo(long.MinValue, false), _ => false),
        new("UpToMax", Constraint.Signed.UpTo(long.MaxValue), _ => true),
        new("PredicateNeg", Constraint.Signed.Predicate(v => v < 0), v => v < 0),
    ];

    static List<C<string>> StringConstraints() =>
    [
        new("Any", Constraint.String.Any, _ => true),
        new("ExactEmpty", Constraint.String.Exact(""), v => v == ""),
        new("ExactAb", Constraint.String.Exact("ab"), v => v == "ab"),
        new("ExactUnicode", Constraint.String.Exact("až"), v => v == "až"),
        new("ExactMissing", Constraint.String.Exact("ac"), v => v == "ac"),
        new("GenericExactA", Constraint.Exact("a"), v => v == "a"),
        new("StartsWithEmpty", Constraint.String.StartsWith(""), _ => true),
        new("StartsWithA", Constraint.String.StartsWith("a"), v => v.StartsWith('a')),
        new("StartsWithAb", Constraint.String.StartsWith("ab"), v => v.StartsWith("ab", StringComparison.Ordinal)),
        new("StartsWithUnicode", Constraint.String.StartsWith("中"),
            v => v.StartsWith("中", StringComparison.Ordinal)),
        new("StartsWithMissing", Constraint.String.StartsWith("q"), v => v.StartsWith('q')),
        new("EndsWithB", Constraint.String.EndsWith("b"), v => v.EndsWith('b')),
        new("EndsWithEmpty", Constraint.String.EndsWith(""), _ => true),
        new("ContainsA", Constraint.String.Contains("a"), v => v.Contains('a')),
        new("ContainsEmpty", Constraint.String.Contains(""), _ => true),
        new("ContainsCI", Constraint.String.ContainsCaseInsensitive("B"),
            v => v.Contains("b", StringComparison.OrdinalIgnoreCase)),
        new("ContainsCIEmpty", Constraint.String.ContainsCaseInsensitive(""), _ => true),
        new("ExactCI", Constraint.String.ExactCaseInsensitive("AB"),
            v => v.Equals("ab", StringComparison.OrdinalIgnoreCase)),
        new("PredicateLen2", Constraint.String.Predicate((in ReadOnlySpan<char> v) => v.Length == 2),
            v => v.Length == 2),
        new("PredicateSlow", Constraint.String.PredicateSlow(v => v.Length > 2), v => v.Length > 2),
        new("UpToAb", Constraint.String.UpTo("ab"), v => string.CompareOrdinal(v, "ab") <= 0),
        new("UpToAbExcl", Constraint.String.UpTo("ab", false), v => string.CompareOrdinal(v, "ab") < 0),
        new("UpToEmpty", Constraint.String.UpTo(""), v => v == ""),
        new("UpToEmptyExcl", Constraint.String.UpTo("", false), _ => false),
        new("UpToUnicode", Constraint.String.UpTo("é"), v => string.CompareOrdinal(v, "é") <= 0),
    ];

    static List<C<SignedKind>> EnumConstraints() =>
    [
        new("Any", Constraint.Enum<SignedKind>.Any, _ => true),
        new("ExactNeg", Constraint.Enum<SignedKind>.Exact(SignedKind.Neg), v => v == SignedKind.Neg),
        new("ExactZero", Constraint.Enum<SignedKind>.Exact(SignedKind.Zero), v => v == SignedKind.Zero),
        new("GenericExactBig", Constraint.Exact(SignedKind.Big), v => v == SignedKind.Big),
        new("PredicateNonNeg", Constraint.Enum<SignedKind>.Predicate(v => v >= 0), v => v >= 0),
    ];

    static List<C<bool>> BoolConstraints() =>
    [
        new("Any", Constraint.Bool.Any, _ => true),
        new("True", Constraint.Bool.Exact(true), v => v),
        new("False", Constraint.Bool.Exact(false), v => !v),
        new("GenericTrue", Constraint.Exact(true), v => v),
    ];

    static List<C<DateTime>> DateTimeConstraints() =>
    [
        new("Any", Constraint.DateTime.Any, _ => true),
        new("ExactMin", Constraint.DateTime.Exact(DtValues[0]), v => v == DtValues[0]),
        new("Exact2000", Constraint.DateTime.Exact(DtValues[2]), v => v == DtValues[2]),
        new("ExactMax", Constraint.DateTime.Exact(DtValues[4]), v => v == DtValues[4]),
        new("UpTo2000", Constraint.DateTime.UpTo(DtValues[2]), v => v <= DtValues[2]),
        new("UpTo2000Excl", Constraint.DateTime.UpTo(DtValues[2], false), v => v < DtValues[2]),
        new("UpToMax", Constraint.DateTime.UpTo(DtValues[4]), _ => true),
        new("Range", Constraint.DateTime.Range(DtValues[1], DtValues[3]),
            v => v >= DtValues[1] && v <= DtValues[3]),
        new("RangeExcl", Constraint.DateTime.Range(DtValues[1], DtValues[3], false),
            v => v >= DtValues[1] && v < DtValues[3]),
        new("RangeEmpty", Constraint.DateTime.Range(DtValues[3], DtValues[1]), _ => false),
        new("PredicateYear", Constraint.DateTime.Predicate(v => v.Year >= 2000), v => v.Year >= 2000),
    ];

    List<CRow> FillData(int seed, int count)
    {
        var rnd = new Random(seed);
        var rows = new Dictionary<(ulong, long, string), CRow>();
        while (rows.Count < count)
        {
            var row = new CRow
            {
                A = AValues[rnd.Next(AValues.Length)],
                B = BValues[rnd.Next(BValues.Length)],
                S = SValues[rnd.Next(SValues.Length)],
                E = EValues[rnd.Next(EValues.Length)],
                Flag = rnd.Next(2) == 0,
                Dt = DtValues[rnd.Next(DtValues.Length)],
                U = UValues[rnd.Next(UValues.Length)],
                G = GValues[rnd.Next(GValues.Length)],
                Payload = rows.Count
            };
            rows.TryAdd((row.A, row.B, row.S), row);
        }

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<ICRowTable>();
        foreach (var row in rows.Values) table.Insert(row);
        tr.Commit();
        return rows.Values.ToList();
    }

    static IOrderedEnumerable<CRow> PkOrder(IEnumerable<CRow> rows) =>
        rows.OrderBy(r => r.A).ThenBy(r => r.B).ThenBy(r => r.S, StringComparer.Ordinal);

    static IOrderedEnumerable<CRow> MixOrder(IEnumerable<CRow> rows) =>
        rows.OrderBy(r => (int)r.E).ThenBy(r => r.Flag).ThenBy(r => r.Dt.Ticks).ThenBy(r => r.A).ThenBy(r => r.B)
            .ThenBy(r => r.S, StringComparer.Ordinal);

    // LastBy with orderers is implemented as FirstBy with inverted orderers, so among rows with equal ordered values
    // it returns the first one in index order, not the last one of the stably sorted Gather result.
    static CRow? LastWithOrderers(List<CRow> sorted, Func<CRow, object> tieKey)
    {
        if (sorted.Count == 0) return null;
        var lastKey = tieKey(sorted[^1]);
        return sorted.First(r => Equals(tieKey(r), lastKey));
    }

    static string Format(IEnumerable<CRow?> rows) => string.Join("\n", rows.Select(r => r?.ToString() ?? "null"));

    static void RunInMode(bool noEmit, Action action)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            action();
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    [InlineData(false, 2)]
    public void PrimaryKeyConstraintsMatchLinq(bool noEmit, int seed)
    {
        RunInMode(noEmit, () =>
        {
            var rows = FillData(seed, 400);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<ICRowTable>();
            var rnd = new Random(seed * 31);
            var ulongs = UlongConstraints();
            var longs = LongConstraints();
            var strings = StringConstraints();

            foreach (var ca in ulongs)
            {
                var expected = PkOrder(rows.Where(r => ca.Predicate(r.A))).ToList();
                Assert.Equal(Format(expected), Format(table.ScanById(ca.Constraint)));
            }

            foreach (var ca in ulongs)
            foreach (var cb in longs)
            {
                var expected = PkOrder(rows.Where(r => ca.Predicate(r.A) && cb.Predicate(r.B))).ToList();
                Assert.True(Format(expected) == Format(table.ScanById(ca.Constraint, cb.Constraint)),
                    $"ScanById({ca.Name}, {cb.Name})");
            }

            foreach (var ca in ulongs)
            foreach (var cb in longs)
            foreach (var cs in strings)
            {
                if (rnd.Next(4) != 0 && ca.Name != "Any") continue;
                var name = $"({ca.Name}, {cb.Name}, {cs.Name})";
                var expected = PkOrder(rows.Where(r =>
                    ca.Predicate(r.A) && cb.Predicate(r.B) && cs.Predicate(r.S))).ToList();
                Assert.True(Format(expected) == Format(table.ScanById(ca.Constraint, cb.Constraint, cs.Constraint)),
                    "ScanById" + name);

                var skip = rnd.Next(3);
                var take = rnd.Next(1, 5);
                var gathered = new List<CRow>();
                var total = table.GatherById(gathered, skip, take, ca.Constraint, cb.Constraint, cs.Constraint);
                Assert.True((ulong)expected.Count == total, "GatherById count" + name);
                Assert.True(Format(expected.Skip(skip).Take(take)) == Format(gathered), "GatherById" + name);

                Assert.True(Format([expected.FirstOrDefault()]) ==
                            Format([table.FirstByIdOrDefault(ca.Constraint, cb.Constraint, cs.Constraint)]),
                    "FirstByIdOrDefault" + name);
                Assert.True(Format([expected.LastOrDefault()]) ==
                            Format([table.LastByIdOrDefault(ca.Constraint, cb.Constraint, cs.Constraint)]),
                    "LastByIdOrDefault" + name);
                if (expected.Count == 0)
                {
                    Assert.Throws<BTDBException>(() => table.FirstById(ca.Constraint, cb.Constraint, cs.Constraint));
                    Assert.Throws<BTDBException>(() => table.LastById(ca.Constraint, cb.Constraint, cs.Constraint));
                }
                else
                {
                    Assert.Equal(expected[0].Payload,
                        table.FirstById(ca.Constraint, cb.Constraint, cs.Constraint).Payload);
                    Assert.Equal(expected[^1].Payload,
                        table.LastById(ca.Constraint, cb.Constraint, cs.Constraint).Payload);
                }
            }
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PrimaryKeyGatherWithOrderersMatchesLinq(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rows = FillData(7, 300);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<ICRowTable>();
            var orderers = new (string Name, IOrderer[] Orderers, Func<IEnumerable<CRow>, IEnumerable<CRow>> Sort,
                Func<CRow, object> TieKey)[]
            {
                ("B asc", [Orderer.Ascending((CRow r) => r.B)],
                    rs => rs.OrderBy(r => r.B).ThenBy(r => r.A).ThenBy(r => r.S, StringComparer.Ordinal),
                    r => r.B),
                ("B desc", [Orderer.Descending((CRow r) => r.B)],
                    rs => rs.OrderByDescending(r => r.B).ThenBy(r => r.A)
                        .ThenBy(r => r.S, StringComparer.Ordinal), r => r.B),
                ("S desc, A asc", [Orderer.Descending((CRow r) => r.S), Orderer.Ascending((CRow r) => r.A)],
                    rs => rs.OrderByDescending(r => r.S, StringComparer.Ordinal).ThenBy(r => r.A)
                        .ThenBy(r => r.B), r => (r.S, r.A)),
                ("A desc, B desc", [Orderer.Descending((CRow r) => r.A), Orderer.Descending((CRow r) => r.B)],
                    rs => rs.OrderByDescending(r => r.A).ThenByDescending(r => r.B)
                        .ThenBy(r => r.S, StringComparer.Ordinal), r => (r.A, r.B)),
            };
            var constraintSets = new (string Name, Constraint<ulong> A, Constraint<long> B, Constraint<string> S,
                Func<CRow, bool> Predicate)[]
                {
                    ("all", Constraint.Unsigned.Any, Constraint.Signed.Any, Constraint.String.Any, _ => true),
                    ("A<=128,B<0", Constraint.Unsigned.UpTo(128), Constraint.Signed.Predicate(v => v < 0),
                        Constraint.String.Any, r => r.A <= 128 && r.B < 0),
                    ("A=1,S^a", Constraint.Unsigned.Exact(1), Constraint.Signed.Any,
                        Constraint.String.StartsWith("a"), r => r.A == 1 && r.S.StartsWith('a')),
                    ("none", Constraint.Unsigned.Exact(3), Constraint.Signed.Any, Constraint.String.Any, _ => false),
                };
            foreach (var o in orderers)
            foreach (var c in constraintSets)
            {
                var expected = o.Sort(rows.Where(c.Predicate)).ToList();
                for (var skip = -1; skip < 3; skip++)
                {
                    var gathered = new List<CRow>();
                    var total = table.GatherById(gathered, skip, 7, c.A, c.B, c.S, o.Orderers);
                    Assert.Equal((ulong)expected.Count, total);
                    var expectedPage = skip < 0 ? expected.Take(7 + skip) : expected.Skip(skip).Take(7);
                    Assert.True(Format(expectedPage) == Format(gathered), $"{o.Name} {c.Name} skip {skip}");
                }

                Assert.True(Format([expected.FirstOrDefault()]) ==
                            Format([table.FirstByIdOrDefault(c.A, c.B, c.S, o.Orderers)]),
                    $"First {o.Name} {c.Name}");
                Assert.True(Format([LastWithOrderers(expected, o.TieKey)]) ==
                            Format([table.LastByIdOrDefault(c.A, c.B, c.S, o.Orderers)]),
                    $"Last {o.Name} {c.Name}");
            }
        });
    }

    [Theory]
    [InlineData(true, 3)]
    [InlineData(false, 3)]
    [InlineData(true, 4)]
    [InlineData(false, 4)]
    public void SecondaryKeyConstraintsMatchLinq(bool noEmit, int seed)
    {
        RunInMode(noEmit, () =>
        {
            var rows = FillData(seed, 400);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<ICRowTable>();
            var rnd = new Random(seed * 17);
            var enums = EnumConstraints();
            var bools = BoolConstraints();
            var dates = DateTimeConstraints();
            var ulongs = UlongConstraints();

            foreach (var ce in enums)
            {
                var expected = MixOrder(rows.Where(r => ce.Predicate(r.E))).ToList();
                Assert.True(Format(expected) == Format(table.ScanByMix(ce.Constraint)), "ScanByMix " + ce.Name);
            }

            foreach (var ce in enums)
            foreach (var cf in bools)
            foreach (var cd in dates)
            {
                var name = $"({ce.Name}, {cf.Name}, {cd.Name})";
                var expected = MixOrder(rows.Where(r =>
                    ce.Predicate(r.E) && cf.Predicate(r.Flag) && cd.Predicate(r.Dt))).ToList();
                Assert.True(Format(expected) == Format(table.ScanByMix(ce.Constraint, cf.Constraint, cd.Constraint)),
                    "ScanByMix" + name);

                var ca = ulongs[rnd.Next(ulongs.Count)];
                var expectedWithA = expected.Where(r => ca.Predicate(r.A)).ToList();
                Assert.True(
                    Format(expectedWithA) ==
                    Format(table.ScanByMix(ce.Constraint, cf.Constraint, cd.Constraint, ca.Constraint)),
                    "ScanByMix" + name + " A " + ca.Name);

                var skip = rnd.Next(3);
                var take = rnd.Next(1, 5);
                var gathered = new List<CRow>();
                var total = table.GatherByMix(gathered, skip, take, ce.Constraint, cf.Constraint, cd.Constraint);
                Assert.True((ulong)expected.Count == total, "GatherByMix count" + name);
                Assert.True(Format(expected.Skip(skip).Take(take)) == Format(gathered), "GatherByMix" + name);

                Assert.True(Format([expected.FirstOrDefault()]) ==
                            Format([table.FirstByMixOrDefault(ce.Constraint, cf.Constraint, cd.Constraint)]),
                    "FirstByMixOrDefault" + name);
                Assert.True(Format([expected.LastOrDefault()]) ==
                            Format([table.LastByMixOrDefault(ce.Constraint, cf.Constraint, cd.Constraint)]),
                    "LastByMixOrDefault" + name);
                if (expected.Count == 0)
                {
                    Assert.Throws<BTDBException>(() => table.FirstByMix(ce.Constraint, cf.Constraint, cd.Constraint));
                    Assert.Throws<BTDBException>(() => table.LastByMix(ce.Constraint, cf.Constraint, cd.Constraint));
                }
                else
                {
                    Assert.Equal(expected[0].Payload,
                        table.FirstByMix(ce.Constraint, cf.Constraint, cd.Constraint).Payload);
                    Assert.Equal(expected[^1].Payload,
                        table.LastByMix(ce.Constraint, cf.Constraint, cd.Constraint).Payload);
                }
            }
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SecondaryKeyGatherWithOrderersMatchesLinq(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rows = FillData(11, 300);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<ICRowTable>();
            var orderers = new (string Name, IOrderer[] Orderers, Func<IEnumerable<CRow>, IEnumerable<CRow>> Sort,
                Func<CRow, object> TieKey)[]
            {
                ("Dt desc", [Orderer.Descending((CRow r) => r.Dt)],
                    rs => MixOrder(rs).ToList().OrderByDescending(r => r.Dt.Ticks), r => r.Dt),
                ("Flag asc, E desc", [Orderer.Ascending((CRow r) => r.Flag), Orderer.Descending((CRow r) => r.E)],
                    rs => MixOrder(rs).ToList().OrderBy(r => r.Flag).ThenByDescending(r => (int)r.E),
                    r => (r.Flag, r.E)),
                ("A asc", [Orderer.Ascending((CRow r) => r.A)], rs => MixOrder(rs).ToList().OrderBy(r => r.A),
                    r => r.A),
            };
            foreach (var o in orderers)
            {
                foreach (var ce in EnumConstraints())
                {
                    var expected = o.Sort(rows.Where(r => ce.Predicate(r.E) && r.Flag)).ToList();
                    var gathered = new List<CRow>();
                    var total = table.GatherByMix(gathered, 1, 10, ce.Constraint, Constraint.Bool.Exact(true),
                        Constraint.DateTime.Any, o.Orderers);
                    Assert.Equal((ulong)expected.Count, total);
                    Assert.True(Format(expected.Skip(1).Take(10)) == Format(gathered), $"{o.Name} {ce.Name}");
                    Assert.True(Format([expected.FirstOrDefault()]) ==
                                Format([
                                    table.FirstByMixOrDefault(ce.Constraint, Constraint.Bool.Exact(true),
                                        Constraint.DateTime.Any, o.Orderers)
                                ]), $"First {o.Name} {ce.Name}");
                    Assert.True(Format([LastWithOrderers(expected, o.TieKey)]) ==
                                Format([
                                    table.LastByMixOrDefault(ce.Constraint, Constraint.Bool.Exact(true),
                                        Constraint.DateTime.Any, o.Orderers)
                                ]), $"Last {o.Name} {ce.Name}");
                }
            }
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UnsignedEnumAndGuidSecondaryKeyMatchesLinq(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rows = FillData(5, 200);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<ICRowTable>();
            var enumConstraints = new List<C<UnsignedKind>>
            {
                new("Any", Constraint.Enum<UnsignedKind>.Any, _ => true),
                new("ExactMax", Constraint.Enum<UnsignedKind>.Exact(UnsignedKind.Max), v => v == UnsignedKind.Max),
                new("GenericExactB", Constraint.Exact(UnsignedKind.B), v => v == UnsignedKind.B),
                new("Predicate", Constraint.Enum<UnsignedKind>.Predicate(v => v is UnsignedKind.A or UnsignedKind.None),
                    v => v is UnsignedKind.A or UnsignedKind.None),
            };
            var guidConstraints = new List<C<Guid>>
            {
                new("Any", Constraint<Guid>.Any, _ => true),
                new("ExactEmpty", Constraint.Guid.Exact(Guid.Empty), v => v == Guid.Empty),
                new("ExactMax", Constraint.Guid.Exact(GValues[3]), v => v == GValues[3]),
                new("GenericExact1", Constraint.Exact(GValues[1]), v => v == GValues[1]),
                new("Missing", Constraint.Guid.Exact(Guid.Parse("12345678-0000-0000-0000-000000000000")), _ => false),
            };
            foreach (var cu in enumConstraints)
            foreach (var cg in guidConstraints)
            {
                var expected = rows.Where(r => cu.Predicate(r.U) && cg.Predicate(r.G))
                    .OrderBy(r => r.U).ThenBy(r => r.G.ToByteArray(), ByteArrayComparer.Instance).ThenBy(r => r.A)
                    .ThenBy(r => r.B).ThenBy(r => r.S, StringComparer.Ordinal).ToList();
                Assert.True(Format(expected) == Format(table.ScanByOther(cu.Constraint, cg.Constraint)),
                    $"ScanByOther({cu.Name}, {cg.Name})");
            }
        });
    }

    sealed class ByteArrayComparer : IComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();
        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }
}
