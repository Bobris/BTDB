using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Generates random rows and random predicates and checks that relation queries return the same rows, in the same
/// order, as LINQ-to-Objects over the materialized relation, including throwing the same exception at the same row.
/// Extend `RelationQueryPredicateGenerator` whenever the query layer learns a new expression shape or field type.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbRelationQueryDifferentialTest : ObjectDbTestBase
{
    const int RowCount = 400;
    const int PredicatesPerSeed = 200;
    static readonly DateTime BaseDate = new(2025, 3, 1, 0, 0, 0, DateTimeKind.Utc);
    static readonly Guid[] Tokens = [Guid.Empty, new("6f9619ff-8b86-d011-b42d-00cf4fc964ff"), new("00000000-0000-0000-0000-000000000001")];
    static readonly string?[] Names = ["n0", "n1", "n2", "Ádám", "😀x", null];
    static readonly string?[] Notes = ["", "abc", "Abc", "x😀", "žluť", "hello world", null];

    public ObjectDbRelationQueryDifferentialTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum DiffKind
    {
        A,
        B,
        C,
        D
    }

    [Generate]
    public class DiffRow
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public string Code { get; set; } = "";

        // Secondary indexes let the planner choose index scans: a single field, two fields, and one including the
        // leading primary key field.
        [SecondaryKey("Name", Order = 1)] public string? Name { get; set; }
        [SecondaryKey("Age")] public int Age { get; set; }
        [SecondaryKey("Name", Order = 2)] public long Score { get; set; }
        [SecondaryKey("Rank", IncludePrimaryKeyOrder = 1)] public uint Rank { get; set; }
        public bool Active { get; set; }
        public DateTime Created { get; set; }
        public double Ratio { get; set; }
        public byte Level { get; set; }
        public short Delta { get; set; }
        public DiffKind Kind { get; set; }
        public int? Bonus { get; set; }
        public bool? Flag { get; set; }
        public Guid Token { get; set; }
        public decimal Price { get; set; }
        public TimeSpan Duration { get; set; }
        public DateTimeOffset Seen { get; set; }
        public string? Note { get; set; }
    }

    public interface IDiffRowTable : IRelation<DiffRow>
    {
        void Insert(DiffRow row);
    }

    public static IEnumerable<object[]> Seeds => Enumerable.Range(1, 10).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void QueryMatchesLinqToObjects(int seed)
    {
        var random = new Random(seed);
        FillRows(random);

        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var generator = new RelationQueryPredicateGenerator(random);
        var selectivePredicates = 0;
        var throwingPredicates = 0;
        for (var i = 0; i < PredicatesPerSeed; i++)
        {
            var predicate = generator.Generate();
            var expected = Run(() => rows.Where(predicate.Compile()));
            if (expected.Exception != null) throwingPredicates++;
            else if (expected.Rows.Count > 0 && expected.Rows.Count < rows.Count) selectivePredicates++;
            var actual = Run(() => generator.ApplyAsWhereChain(table.Query<DiffRow>(), predicate).AsEnumerable());
            if (actual.Exception is NotSupportedException notSupported)
                throw new InvalidOperationException($"Seed {seed}, predicate #{i} is not supported: {predicate}",
                    notSupported);
            if (expected.Rows.SequenceEqual(actual.Rows) && expected.Exception?.GetType() == actual.Exception?.GetType())
                continue;
            Assert.Fail($"Seed {seed}, predicate #{i}: {predicate}\n" +
                        $"expected ({expected.Rows.Count}, {expected.Exception?.GetType().Name}): {string.Join(", ", expected.Rows.Take(5))}\n" +
                        $"actual ({actual.Rows.Count}, {actual.Exception?.GetType().Name}): {string.Join(", ", actual.Rows.Take(5))}\n" +
                        $"{actual.Exception}");
        }

        // Guards the generator itself: predicates that match nothing or everything would hide query bugs.
        Assert.True(selectivePredicates >= PredicatesPerSeed / 4,
            $"Only {selectivePredicates} of {PredicatesPerSeed} predicates selected a proper subset of rows.");
        Assert.True(throwingPredicates < PredicatesPerSeed / 2,
            $"{throwingPredicates} of {PredicatesPerSeed} predicates threw.");
    }

    static readonly Expression<Func<DiffRow, bool>>[] EdgeArithmeticPredicates =
    [
        p => (p.Score - p.Score + long.MinValue) % -1 == 0,
        p => (p.Age - p.Age + int.MinValue) / -1 == 0,
        p => (p.Age - p.Age + int.MinValue) % -1 == 0,
        p => p.Age % (p.Age - p.Age) == 0,
        p => p.Score / (p.Score - p.Score) == 0,
        p => checked((byte)(p.Age + 300)) == 5,
        p => (byte)(p.Age + 300) == 44,
        p => checked(-(p.Age - p.Age + int.MinValue)) == 0,
        p => -(p.Age - p.Age + int.MinValue) == int.MinValue,
        p => (int)(p.Ratio * double.NaN) == 0,
        p => (int)(p.Ratio + 1e20) == 0,
        p => checked((int)(p.Ratio + 1e20)) == 0,
        p => (long)(p.Ratio - 1e30) < 0,
        p => (uint)(p.Ratio - 5) > 0,
        p => (p.Delta * p.Delta) % 7 == 1,
        p => (long)p.Rank * -1 < 0,
        p => checked(p.Rank - 4000000000u) > 0,
        p => p.Rank - 4000000000u > 0,
        p => p.Level - 1 < 0,
        p => checked((uint)(p.Age - 1000)) > 0,
        p => (ulong)(p.Age - 1000) > 0,
        p => p.Price % 0.3m == 0.1m,
        p => p.Price / (p.Price - p.Price) > 0,
        p => p.Price * 1e28m > 0,
        p => (int)p.Price == 3,
        p => (int)(p.Price * 1e20m) == 0,
        p => (float)p.Ratio == 0.5f,
        p => p.Bonus * 2 > 10,
        p => p.Bonus % (p.Age - p.Age) == 0,
        p => -p.Delta < 0,
        p => (short)(p.Delta * 1000) < 0,
        p => checked((short)(p.Delta * 1000)) < 0,
        p => (sbyte)p.Level < 0,
        p => checked((sbyte)p.Level) < 0,
        p => (double)p.Score / 0 > 0,
        p => p.Ratio % 0 == 0,
        p => (decimal)p.Ratio > 1m,
        p => (decimal)(p.Ratio * double.PositiveInfinity) > 1m,
        p => (long)p.Price * p.Score > 0,
        p => checked(p.Score * p.Score) > 0,
        p => checked(p.Age * p.Age * p.Age * p.Age) > 0,
        p => checked(p.Level + p.Level) > 300,
        p => checked(p.Score + long.MaxValue) > 0,
        p => checked(p.Score - long.MaxValue) < 0,
        p => string.CompareOrdinal(p.Note, "a") < 0,
        p => p.Note!.Length > 2,
        p => p.Note!.StartsWith(""),
        p => p.Note!.EndsWith("C", StringComparison.OrdinalIgnoreCase),
        p => p.Note!.Contains("😀"),
        p => p.Note != null && p.Note.StartsWith("x\uD83D"),
        p => string.IsNullOrEmpty(p.Note),
        p => p.Bonus > p.Age,
        p => p.Bonus == null || p.Bonus.Value > 3,
        p => p.Flag == true,
        p => p.Flag != false,
        p => p.Seen < DateTimeOffset.MinValue.AddYears(1),
        p => p.Kind > DiffKind.B,
        p => (int)p.Kind * 2 == 2,
        p => (DiffKind)(p.Age % 4) == p.Kind,
    ];

    // Arithmetic and conversions at the limits of their types: overflow, division by zero, NaN and infinities; and
    // null strings, surrogates, nullable values and dates at their limits.
    [Fact]
    public void EdgeArithmeticMatchesLinqToObjects()
    {
        FillRows(new Random(7));
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var failures = new List<string>();
        foreach (var predicate in EdgeArithmeticPredicates)
        {
            var expected = Run(() => rows.Where(predicate.Compile()));
            var actual = Run(() => table.Query<DiffRow>().Where(predicate).AsEnumerable());
            if (expected.Rows.SequenceEqual(actual.Rows) && expected.Exception?.GetType() == actual.Exception?.GetType())
                continue;
            failures.Add($"{predicate}: expected ({expected.Rows.Count}, {expected.Exception?.GetType().Name}) actual ({actual.Rows.Count}, {actual.Exception?.GetType().Name} {actual.Exception?.Message})");
        }

        Assert.True(failures.Count == 0, string.Join("\n", failures));
    }

    static readonly Expression<Func<DiffRow, bool>>[] KeyLimitPredicates =
    [
        p => p.Tenant > ulong.MaxValue - 1,
        p => p.Tenant >= ulong.MaxValue,
        p => p.Tenant == ulong.MaxValue,
        p => p.Tenant <= 0,
        p => p.Tenant < 1,
        p => p.Tenant > 0 && p.Tenant < ulong.MaxValue,
        p => p.Tenant == ulong.MaxValue && p.Code.StartsWith("\uFFFF"),
        p => p.Tenant == ulong.MaxValue && p.Code.StartsWith("\uD83D"),
        p => p.Tenant == ulong.MaxValue && p.Code.StartsWith("😀"),
        p => p.Tenant == ulong.MaxValue && p.Code.StartsWith(""),
        p => p.Tenant == ulong.MaxValue && p.Code == "",
        p => p.Tenant == ulong.MaxValue && string.CompareOrdinal(p.Code, "\uFFFF") >= 0,
        p => p.Age >= int.MaxValue,
        p => p.Age > int.MaxValue - 1,
        p => p.Age <= int.MinValue,
        p => p.Age < int.MinValue + 1,
        p => p.Age > int.MinValue && p.Age < int.MaxValue,
        p => p.Age == int.MinValue || p.Age == int.MaxValue,
        p => p.Rank == uint.MaxValue,
        p => p.Rank >= uint.MaxValue - 1,
        p => p.Rank < 1,
        p => p.Name == null,
        p => p.Name == "",
        p => p.Name == "\uFFFF\uFFFF",
        p => p.Name!.StartsWith("\uFFFF"),
        p => p.Name!.StartsWith(""),
        p => p.Name == null && p.Score == long.MinValue,
        p => p.Name == "\uFFFF\uFFFF" && p.Score >= long.MaxValue,
        p => p.Name == "\uFFFF\uFFFF" && p.Score > long.MinValue,
        p => p.Name == "" && p.Score <= long.MinValue,
    ];

    // Constants at the limits of key types, against rows that hold those limits, so key ranges end at the first or last
    // possible key.
    [Fact]
    public void KeyLimitQueriesMatchLinqToObjects()
    {
        FillRows(new Random(3));
        using (var tr = _db.StartTransaction())
        {
            var insert = tr.GetRelation<IDiffRowTable>();
            var codes = new[] { "", "\uFFFF", "\uFFFF\uFFFF", "😀", "\uD83D\uDE00x", "a" };
            for (var i = 0; i < codes.Length; i++)
            {
                foreach (var tenant in new[] { 0UL, ulong.MaxValue })
                {
                    insert.Insert(new()
                    {
                        Tenant = tenant, Code = codes[i] + (tenant == 0 ? "~" : ""),
                        Name = i % 3 == 0 ? null : i % 3 == 1 ? "" : "\uFFFF\uFFFF",
                        Age = i % 2 == 0 ? int.MinValue : int.MaxValue, Score = i % 2 == 0 ? long.MinValue : long.MaxValue,
                        Rank = i % 2 == 0 ? 0 : uint.MaxValue
                    });
                }
            }

            tr.Commit();
        }

        using var readTr = _db.StartReadOnlyTransaction();
        var table = readTr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var failures = new List<string>();
        foreach (var predicate in KeyLimitPredicates)
        {
            var expected = Run(() => rows.Where(predicate.Compile()));
            var actual = Run(() => table.Query<DiffRow>().Where(predicate).AsEnumerable());
            if (expected.Rows.SequenceEqual(actual.Rows) && expected.Exception?.GetType() == actual.Exception?.GetType())
                continue;
            failures.Add($"{predicate}: expected ({expected.Rows.Count}, {expected.Exception?.GetType().Name}) actual ({actual.Rows.Count}, {actual.Exception?.GetType().Name} {actual.Exception?.Message})");
        }

        Assert.True(failures.Count == 0, string.Join("\n", failures));
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void TerminalOperatorsMatchLinqToObjects(int seed)
    {
        var random = new Random(seed);
        FillRows(random);

        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var generator = new RelationQueryPredicateGenerator(random);
        var terminals = Terminals();
        for (var i = 0; i < PredicatesPerSeed; i++)
        {
            var source = random.Next(4) == 0 ? null : generator.Generate();
            var argument = generator.Generate();
            var (name, terminal) = terminals[random.Next(terminals.Count)];
            var expected = RunTerminal(() => terminal(source == null ? rows.AsQueryable() : rows.AsQueryable().Where(source),
                argument));
            var actual = RunTerminal(() => terminal(source == null
                ? table.Query<DiffRow>()
                : generator.ApplyAsWhereChain(table.Query<DiffRow>(), source), argument));
            if (actual.Exception is NotSupportedException notSupported)
                throw new InvalidOperationException($"Seed {seed}, #{i} {name} is not supported: {source} / {argument}",
                    notSupported);
            if (expected.Value == actual.Value && expected.Exception?.GetType() == actual.Exception?.GetType() &&
                expected.Exception?.Message == actual.Exception?.Message)
                continue;
            Assert.Fail($"Seed {seed}, #{i} {name}: {source} / {argument}\n" +
                        $"expected {expected.Value} {expected.Exception?.GetType().Name}: {expected.Exception?.Message}\n" +
                        $"actual {actual.Value} {actual.Exception?.GetType().Name}: {actual.Exception?.Message}\n" +
                        $"{actual.Exception}");
        }
    }

    // Each case returns a query over the captured values it picks; calling it again builds the same expression shape
    // with other values, as a method running a query with its arguments does.
    static List<(string Name, Func<Random, Func<IQueryable<DiffRow>, object?>> Case)> CapturedValueCases(
        List<int> sharedAges) =>
    [
        ("tenant", r =>
        {
            var t = (ulong)r.Next(5);
            return q => q.Where(p => p.Tenant == t).ToList();
        }),
        ("tenant and code", r =>
        {
            var t = (ulong)r.Next(4);
            var c = r.Next(3) == 0 ? "missing" : $"c{r.Next(RowCount):D3}";
            return q => q.Where(p => p.Tenant == t && p.Code == c).ToList();
        }),
        ("tenant range", r =>
        {
            var lo = (ulong)r.Next(4);
            var hi = lo + (ulong)r.Next(3);
            return q => q.Where(p => p.Tenant > lo && p.Tenant <= hi).Select(p => p.Code).ToList();
        }),
        ("tenant or", r =>
        {
            var a = (ulong)r.Next(4);
            var b = (ulong)r.Next(4);
            return q => q.Where(p => p.Tenant == a || p.Tenant == b).Select(p => p.Code).ToList();
        }),
        ("captured arithmetic", r =>
        {
            var t = (ulong)r.Next(3);
            return q => q.Count(p => p.Tenant == t + 1);
        }),
        ("rank index", r =>
        {
            var t = (ulong)r.Next(4);
            var k = (uint)r.Next(8);
            return q => q.Where(p => p.Tenant == t && p.Rank == k).ToList();
        }),
        ("age index", r =>
        {
            var a = 20 + r.Next(11);
            return q => q.Where(p => p.Age == a).ToList();
        }),
        ("age bounds", r =>
        {
            var a = 19 + r.Next(11);
            var b = a + r.Next(4);
            return q => q.Where(p => p.Age >= a && p.Age < b).Select(p => p.Code).ToList();
        }),
        ("price", r =>
        {
            var x = r.Next(-5, 6) * 1.25m;
            return q => q.Count(p => p.Price > x);
        }),
        ("created", r =>
        {
            var d = BaseDate.AddDays(r.Next(6));
            return q => q.Count(p => p.Created < d);
        }),
        ("kind", r =>
        {
            var k = (DiffKind)r.Next(4);
            return q => q.Where(p => p.Kind == k).Select(p => p.Code).ToList();
        }),
        ("bonus", r =>
        {
            int? b = r.Next(3) == 0 ? null : r.Next(-2, 3);
            return q => q.Where(p => p.Bonus == b).Select(p => p.Code).ToList();
        }),
        ("bool and key", r =>
        {
            var f = r.Next(2) == 0;
            var t = (ulong)r.Next(4);
            return q => q.Count(p => p.Active == f && p.Tenant == t);
        }),
        ("name", r =>
        {
            var n = Names[r.Next(Names.Length)];
            return q => q.Where(p => p.Name == n).Select(p => p.Code).ToList();
        }),
        ("starts with", r =>
        {
            var prefix = r.Next(3) switch { 0 => "c0", 1 => "ž", _ => "c01" };
            return q => q.Count(p => p.Code != null && p.Code.StartsWith(prefix, StringComparison.Ordinal));
        }),
        ("mutated list", r =>
        {
            // The same list instance with other contents: a captured collection must never be reused.
            sharedAges.Clear();
            for (var i = r.Next(4); i > 0; i--) sharedAges.Add(20 + r.Next(10));
            return q => q.Where(p => sharedAges.Contains(p.Age)).Select(p => p.Code).ToList();
        }),
        ("page", r =>
        {
            var skip = r.Next(15);
            var take = r.Next(10);
            return q => q.OrderBy(p => p.Age).ThenBy(p => p.Score).Skip(skip).Take(take).Select(p => p.Code).ToList();
        }),
        ("key page", r =>
        {
            var t = (ulong)r.Next(4);
            var skip = r.Next(10);
            return q => q.Where(p => p.Tenant == t).Skip(skip).Take(5).Select(p => p.Code).ToList();
        }),
        ("selected value", r =>
        {
            var add = r.Next(100);
            return q => q.Where(p => p.Tenant == 2).Select(p => p.Age + add).ToList();
        }),
        ("anonymous", r =>
        {
            var m = (long)r.Next(1, 4);
            var t = (ulong)r.Next(4);
            return q => q.Where(p => p.Tenant == t).Select(p => new { p.Code, X = p.Score * m }).ToList();
        }),
        ("sum", r =>
        {
            var m = r.Next(1, 4);
            return q => q.Sum(p => p.Age * m);
        }),
        ("first or default", r =>
        {
            var a = 20 + r.Next(12);
            return q => q.Where(p => p.Age == a).OrderBy(p => p.Score).FirstOrDefault();
        })
    ];

    [Theory]
    [MemberData(nameof(Seeds))]
    public void CapturedValuesReuseCachedPlansLikeLinq(int seed)
    {
        var random = new Random(seed);
        FillRows(random);
        var sharedAges = new List<int>();
        var cases = CapturedValueCases(sharedAges);
        for (var round = 0; round < 8; round++)
        {
            // Every round uses a new transaction, so plans are reused across transactions too.
            using var tr = _db.StartReadOnlyTransaction();
            var table = tr.GetRelation<IDiffRowTable>();
            var rows = table.ToList();
            foreach (var (name, createCase) in cases)
            {
                var query = createCase(random);
                var expected = Format(() => query(rows.AsQueryable()));
                var actual = Format(() => query(table.Query<DiffRow>()));
                Assert.True(expected == actual, $"Seed {seed}, round {round}, {name}:\nexpected {expected}\nactual {actual}");
            }
        }

        using var lastTr = _db.StartReadOnlyTransaction();
        var cache = ((IRelationDbManipulator)lastTr.GetRelation<IDiffRowTable>()).RelationInfo.QueryCache;
        Assert.True(cache.Hits > cache.Misses, $"hits {cache.Hits}, misses {cache.Misses}");
    }

    static string Format(Func<object?> run)
    {
        try
        {
            return run() switch
            {
                null => "null",
                DiffRow row => Describe(row),
                string text => text,
                IEnumerable items => string.Join(", ", items.Cast<object?>()
                    .Select(item => item is DiffRow row ? Describe(row) : item?.ToString() ?? "null")),
                var value => $"{value.GetType().Name}:{value}"
            };
        }
        catch (Exception exception)
        {
            return $"{exception.GetType().Name}: {exception.Message}";
        }
    }

    [Fact]
    public void CachedPlansAreReusedForNewCapturedValues()
    {
        FillRows(new(3));
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var cache = ((IRelationDbManipulator)table).RelationInfo.QueryCache;
        var rows = table.ToList();

        int CountTenant(ulong tenant) => table.Query<DiffRow>().Count(p => p.Tenant == tenant);
        Assert.Equal(rows.Count(p => p.Tenant == 0), CountTenant(0));
        var misses = cache.Misses;
        for (var tenant = 0ul; tenant < 6; tenant++)
            Assert.Equal(rows.Count(p => p.Tenant == tenant), CountTenant(tenant));
        Assert.Equal(misses, cache.Misses);

        // A string compared to a field without an index is part of the program, so each value is compiled once; an
        // indexed one is a key range bound for each execution.
        int CountNote(string? note) => table.Query<DiffRow>().Count(p => p.Note == note);
        var notes = rows.Select(p => p.Note).Where(n => n != null).Distinct().Take(2).ToArray();
        Assert.Equal(rows.Count(p => p.Note == notes[0]), CountNote(notes[0]));
        misses = cache.Misses;
        Assert.Equal(rows.Count(p => p.Note == notes[1]), CountNote(notes[1]));
        Assert.True(cache.Misses > misses);
        misses = cache.Misses;
        Assert.Equal(rows.Count(p => p.Note == notes[0]), CountNote(notes[0]));
        Assert.Equal(rows.Count(p => p.Note == notes[1]), CountNote(notes[1]));
        Assert.Equal(misses, cache.Misses);
        int CountName(string? name) => table.Query<DiffRow>().Count(p => p.Name == name);
        Assert.Equal(rows.Count(p => p.Name == "n1"), CountName("n1"));
        misses = cache.Misses;
        Assert.Equal(rows.Count(p => p.Name == "n2"), CountName("n2"));
        Assert.Equal(misses, cache.Misses);
        // Skip and Take counts are parameters too, so every page reuses the plan.
        List<string> Page(int skip, int take) =>
            table.Query<DiffRow>().Where(p => p.Tenant == 1).Skip(skip).Take(take).Select(p => p.Code).ToList();
        Assert.Equal(rows.Where(p => p.Tenant == 1).Skip(1).Take(2).Select(p => p.Code), Page(1, 2));
        misses = cache.Misses;
        for (var skip = 2; skip < 6; skip++)
            Assert.Equal(rows.Where(p => p.Tenant == 1).Skip(skip).Take(skip).Select(p => p.Code), Page(skip, skip));
        Assert.Equal(misses, cache.Misses);
        // Skip(0) is dropped, so Where may follow it.
        Assert.Equal(rows.Count(p => p.Tenant == 1), table.Query<DiffRow>().Skip(0).Where(p => p.Tenant == 1).Count());
    }

    [Fact]
    public void CachedPlansReadKeysAfterPrefixOfEachExecution()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IDiffRowTable>();
            // Tenants whose keys encode to 1, 3 and 5 bytes.
            foreach (var tenant in new ulong[] { 1, 100_000, 1ul << 33 })
            for (var i = 0; i < 12; i++)
                table.Insert(new() { Tenant = tenant, Code = $"c{i:D2}", Age = 20 + i });
            tr.Commit();
        }

        using var readTr = _db.StartReadOnlyTransaction();
        var rows = readTr.GetRelation<IDiffRowTable>().ToList();
        // The tenant is in the scanned key prefix; the Code test reads the key right after it.
        List<string> Codes(ulong tenant) => readTr.GetRelation<IDiffRowTable>().Query<DiffRow>()
            .Where(p => p.Tenant == tenant && p.Code.EndsWith("1", StringComparison.Ordinal)).Select(p => p.Code)
            .ToList();
        foreach (var tenant in new ulong[] { 1, 100_000, 1ul << 33, 1 })
            Assert.Equal(rows.Where(p => p.Tenant == tenant && p.Code.EndsWith('1')).Select(p => p.Code), Codes(tenant));
    }

    [Fact]
    public void CachedPlansAreSharedBetweenThreads()
    {
        FillRows(new(5));
        List<DiffRow> rows;
        using (var tr = _db.StartReadOnlyTransaction()) rows = tr.GetRelation<IDiffRowTable>().ToList();
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        System.Threading.Tasks.Parallel.For(0, 8, thread =>
        {
            var random = new Random(thread);
            var cases = CapturedValueCases([]).Where(c => c.Name != "mutated list").ToList();
            for (var i = 0; i < 40; i++)
            {
                using var tr = _db.StartReadOnlyTransaction();
                var table = tr.GetRelation<IDiffRowTable>();
                var (name, createCase) = cases[random.Next(cases.Count)];
                var query = createCase(random);
                var expected = Format(() => query(rows.AsQueryable()));
                var actual = Format(() => query(table.Query<DiffRow>()));
                if (expected != actual) failures.Enqueue($"{name}: expected {expected}, actual {actual}");
            }
        });
        Assert.Empty(failures);
    }

    // The chains below are written inline, so BTDB.SourceGenerator intercepts them; LINQ-to-Objects runs the same
    // operators over the rows.
    [Theory]
    [MemberData(nameof(Seeds))]
    public void InterceptedCallSitesMatchLinq(int seed)
    {
        var random = new Random(seed);
        FillRows(random);
        var sharedAges = new List<int>();
        var replays = new Dictionary<string, int>();
        for (var round = 0; round < 8; round++)
        {
            using var tr = _db.StartReadOnlyTransaction();
            var table = tr.GetRelation<IDiffRowTable>();
            var cache = ((IRelationDbManipulator)table).RelationInfo.QueryCache;
            var rows = table.ToList();
            var t = (ulong)random.Next(5);
            var lo = (ulong)random.Next(4);
            var hi = lo + (ulong)random.Next(3);
            var b = (ulong)random.Next(4);
            var code = random.Next(3) == 0 ? "missing" : $"c{random.Next(RowCount):D3}";
            var age = 19 + random.Next(12);
            var ageTo = age + random.Next(4);
            var price = random.Next(-5, 6) * 1.25m;
            var kind = (DiffKind)random.Next(4);
            int? bonus = random.Next(3) == 0 ? null : random.Next(-2, 3);
            var name = Names[random.Next(Names.Length)];
            var prefix = random.Next(2) == 0 ? "c0" : "ž";
            var skip = random.Next(-1, 15);
            var take = random.Next(10);
            var add = random.Next(100);
            var factor = (long)random.Next(1, 4);
            var fallback = rows[random.Next(rows.Count)];
            sharedAges.Clear();
            for (var i = random.Next(4); i > 0; i--) sharedAges.Add(20 + random.Next(10));

            void Check(string caseName, Func<object?> expected, Func<object?> actual)
            {
                var expectedText = Format(expected);
                var before = cache.Replays;
                var actualText = Format(actual);
                replays[caseName] = replays.GetValueOrDefault(caseName) + (cache.Replays > before ? 1 : 0);
                Assert.True(expectedText == actualText,
                    $"Seed {seed}, round {round}, {caseName}:\nexpected {expectedText}\nactual {actualText}");
            }

            Check("tenant", () => rows.Where(p => p.Tenant == t).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Tenant == t).ToList());
            Check("tenant and code", () => rows.Where(p => p.Tenant == t && p.Code == code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Tenant == t && p.Code == code).ToList());
            Check("tenant range", () => rows.Where(p => p.Tenant > lo && p.Tenant <= hi).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Tenant > lo && p.Tenant <= hi).Select(p => p.Code).ToList());
            Check("tenant or", () => rows.Where(p => p.Tenant == t || p.Tenant == b).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Tenant == t || p.Tenant == b).Select(p => p.Code).ToList());
            Check("captured arithmetic", () => rows.Count(p => p.Tenant == lo + 1),
                () => table.Query<DiffRow>().Count(p => p.Tenant == lo + 1));
            Check("age index", () => rows.Where(p => p.Age == age).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Age == age).ToList());
            Check("age bounds", () => rows.Where(p => p.Age >= age && p.Age < ageTo).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Age >= age && p.Age < ageTo).Select(p => p.Code).ToList());
            Check("price", () => rows.Count(p => p.Price > price), () => table.Query<DiffRow>().Count(p => p.Price > price));
            Check("kind", () => rows.Where(p => p.Kind == kind).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Kind == kind).Select(p => p.Code).ToList());
            Check("bonus", () => rows.Where(p => p.Bonus == bonus).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Bonus == bonus).Select(p => p.Code).ToList());
            Check("name", () => rows.Where(p => p.Name == name).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Name == name).Select(p => p.Code).ToList());
            Check("starts with",
                () => rows.Count(p => p.Code != null && p.Code.StartsWith(prefix, StringComparison.Ordinal)),
                () => table.Query<DiffRow>()
                    .Count(p => p.Code != null && p.Code.StartsWith(prefix, StringComparison.Ordinal)));
            Check("mutated list", () => rows.Where(p => sharedAges.Contains(p.Age)).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => sharedAges.Contains(p.Age)).Select(p => p.Code).ToList());
            Check("page",
                () => rows.OrderBy(p => p.Age).ThenBy(p => p.Score).Skip(skip).Take(take).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().OrderBy(p => p.Age).ThenBy(p => p.Score).Skip(skip).Take(take)
                    .Select(p => p.Code).ToList());
            Check("key page", () => rows.Where(p => p.Tenant == t).Skip(skip).Take(5).Select(p => p.Code).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Tenant == t).Skip(skip).Take(5).Select(p => p.Code).ToList());
            Check("selected value", () => rows.Where(p => p.Tenant == 2).Select(p => p.Age + add).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Tenant == 2).Select(p => p.Age + add).ToList());
            Check("anonymous", () => rows.Where(p => p.Tenant == t).Select(p => new { p.Code, X = p.Score * factor }).ToList(),
                () => table.Query<DiffRow>().Where(p => p.Tenant == t).Select(p => new { p.Code, X = p.Score * factor })
                    .ToList());
            Check("projected page", () => rows.Select(p => p.Age + add).Skip(skip).Take(take).ToList(),
                () => table.Query<DiffRow>().Select(p => p.Age + add).Skip(skip).Take(take).ToList());
            Check("sum", () => rows.Sum(p => p.Age * factor), () => table.Query<DiffRow>().Sum(p => p.Age * factor));
            Check("first or default", () => rows.Where(p => p.Age == age).OrderBy(p => p.Score).FirstOrDefault(),
                () => table.Query<DiffRow>().Where(p => p.Age == age).OrderBy(p => p.Score).FirstOrDefault());
            Check("default value", () => rows.FirstOrDefault(p => p.Age == age && p.Tenant == t, fallback),
                () => table.Query<DiffRow>().FirstOrDefault(p => p.Age == age && p.Tenant == t, fallback));
            Check("any and all", () => $"{rows.Any(p => p.Age == age)}/{rows.All(p => p.Age < ageTo)}",
                () => $"{table.Query<DiffRow>().Any(p => p.Age == age)}/{table.Query<DiffRow>().All(p => p.Age < ageTo)}");
            Check("single", () => rows.Single(p => p.Tenant == t && p.Code == code),
                () => table.Query<DiffRow>().Single(p => p.Tenant == t && p.Code == code));
            Check("max of projection", () => rows.Where(p => p.Tenant == t).Select(p => p.Score + add).DefaultIfEmpty().Max(),
                () => table.Query<DiffRow>().Where(p => p.Tenant == t).Select(p => p.Score + add).AsEnumerable()
                    .DefaultIfEmpty().Max());
        }

        using var lastTr = _db.StartReadOnlyTransaction();
        var query = lastTr.GetRelation<IDiffRowTable>().Query<DiffRow>().Where(p => p.Age > 1);
        Assert.NotNull(((IRelationQueryChain)query).Link);
        // Every call site reads its values without building the expression once its variants are known.
        foreach (var (caseName, count) in replays) Assert.True(count >= 4, $"{caseName} replayed {count} times");
    }

    [Fact]
    public void InterceptedChainsWhichRelationQueriesDoNotSupportFailLikeRegularOnes()
    {
        FillRows(new(6));
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        // A query in a variable is not intercepted, so it shows how regular Queryable calls fail.
        var regular = table.Query<DiffRow>();
        // OrderBy after Select is not a link, so Queryable builds it, exactly as without interception.
        Assert.Equal(Record.Exception(() => regular.Select(p => p.Code).OrderBy(c => c).Take(3).ToList())?.GetType(),
            Record.Exception(() => table.Query<DiffRow>().Select(p => p.Code).OrderBy(c => c).Take(3).ToList())
                ?.GetType());
        Assert.Throws<NotSupportedException>(() => table.Query<DiffRow>().Where((p, i) => i > 1).Count());
    }

    [Fact]
    public void QueryCacheIsBounded()
    {
        FillRows(new(4));
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var cache = ((IRelationDbManipulator)table).RelationInfo.QueryCache;
        var row = Expression.Parameter(typeof(DiffRow), "p");
        for (var i = 0; i < RelationQueryCache.Capacity + 100; i++)
        {
            // Literals are part of the shape, so every query has a template of its own.
            var predicate = Expression.Lambda<Func<DiffRow, bool>>(
                Expression.Equal(Expression.Property(row, nameof(DiffRow.Score)), Expression.Constant((long)i)), row);
            Assert.Equal(i is >= -5 and <= 5 ? table.Count(p => p.Score == i) : 0,
                table.Query<DiffRow>().Count(predicate));
        }

        Assert.InRange(cache.Count, 1, RelationQueryCache.Capacity);
    }

    [Fact]
    public void PlannerChoosesKeyRangesAndSecondaryIndexes()
    {
        FillRows(new(1));
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();

        RelationQueryPlan Plan(Expression<Func<DiffRow, bool>> predicate, bool orderSensitive = true)
        {
            var relationInfo = ((IRelationDbManipulator)table).RelationInfo;
            var ast = RelationQueryAstBuilder.Build(table.Query<DiffRow>().Where(predicate).Expression,
                new(relationInfo.Id, typeof(DiffRow)));
            return new RelationQueryCompiler(relationInfo, typeof(DiffRow))
                .Compile((IInternalObjectDBTransaction)tr, ast, null, orderSensitive);
        }

        var tenants = new[] { 3ul, 1ul };
        Assert.Equal(2, Plan(p => tenants.Contains(p.Tenant)).Ranges.Length);
        Assert.Equal(2, Plan(p => p.Tenant == 1 || p.Tenant == 2).Ranges.Length);
        var bounded = Plan(p => p.Tenant > 1 && p.Tenant <= 2);
        Assert.Null(bounded.SecondaryKey);
        Assert.True(bounded.Ranges[0].ExcludeStart);
        Assert.True(bounded.Ranges[0].StopInclusive);
        Assert.False(bounded.HasPrograms);
        // StartsWith throws for a null key, so it stays as a residual test and the null keys get a range too.
        var startsWith = Plan(p => p.Tenant == 2 && p.Code.StartsWith("c0", StringComparison.Ordinal));
        Assert.Equal(2, startsWith.Ranges.Length);
        Assert.True(startsWith.Ranges[0].IsWholePrefix);
        // The Age index key is Age, Tenant, Code, so it uses both equalities and beats the primary key.
        Assert.NotNull(Plan(p => p.Tenant == 1 && p.Age == 25).SecondaryKey);
        // Ties go to the primary key: Tenant is also the first field of the Rank index.
        Assert.Null(Plan(p => p.Tenant == 1 && p.Active).SecondaryKey);
        var age = Plan(p => p.Age == 25 && p.Active);
        Assert.NotNull(age.SecondaryKey);
        Assert.True(age.SecondaryKey!.SortCandidates);
        Assert.True(Plan(p => p.Age > 25, false).SecondaryKey!.Covering);
        Assert.NotNull(Plan(p => p.Name == "n1" && p.Score > 0).SecondaryKey);
        Assert.NotNull(Plan(p => p.Tenant == 1 && p.Rank == 3).SecondaryKey);
        // A throwing conjunct written first keeps every row in play.
        var guarded = Plan(p => p.Note!.Length > 2 && p.Age == 25);
        Assert.Null(guarded.SecondaryKey);
        Assert.True(guarded.Ranges[0].IsWholePrefix);

        Assert.Equal(table.Count(p => p.Age > 25), table.Query<DiffRow>().Count(p => p.Age > 25));
        Assert.Equal(table.Count(p => p.Tenant == 1 && p.Rank == 3),
            table.Query<DiffRow>().Count(p => p.Tenant == 1 && p.Rank == 3));
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void OrderingAndPagingMatchLinqToObjects(int seed)
    {
        var random = new Random(seed);
        FillRows(random);

        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var generator = new RelationQueryPredicateGenerator(random);
        var firstKeys = FirstOrderingKeys();
        var nextKeys = NextOrderingKeys();
        var terminals = WindowTerminals();
        for (var i = 0; i < PredicatesPerSeed; i++)
        {
            var source = random.Next(3) == 0 ? null : generator.Generate();
            var shape = new List<string>();
            var steps = new List<Func<IQueryable<DiffRow>, IQueryable<DiffRow>>>();
            if (random.Next(4) != 0)
            {
                var (firstName, first) = firstKeys[random.Next(firstKeys.Count)];
                shape.Add(firstName);
                var thenSteps = new List<Func<IOrderedQueryable<DiffRow>, IOrderedQueryable<DiffRow>>>();
                for (var k = random.Next(3); k > 0; k--)
                {
                    var (thenName, then) = nextKeys[random.Next(nextKeys.Count)];
                    shape.Add(thenName);
                    thenSteps.Add(then);
                }

                steps.Add(q =>
                {
                    var ordered = first(q);
                    foreach (var then in thenSteps) ordered = then(ordered);
                    return ordered;
                });
            }

            for (var k = random.Next(3); k > 0; k--)
            {
                var count = random.Next(-1, 40);
                if (random.Next(2) == 0)
                {
                    shape.Add($"Skip({count})");
                    steps.Add(q => q.Skip(count));
                }
                else
                {
                    shape.Add($"Take({count})");
                    steps.Add(q => q.Take(count));
                }
            }

            var (terminalName, terminal) = terminals[random.Next(terminals.Count)];
            var description = $"{source} / {string.Join(".", shape)} / {terminalName}";

            IQueryable<DiffRow> Apply(IQueryable<DiffRow> q)
            {
                foreach (var step in steps) q = step(q);
                return q;
            }

            var expected = RunTerminal(() => terminal(Apply(source == null ? rows.AsQueryable() : rows.AsQueryable().Where(source))));
            var actual = RunTerminal(() => terminal(Apply(source == null
                ? table.Query<DiffRow>()
                : generator.ApplyAsWhereChain(table.Query<DiffRow>(), source))));
            if (actual.Exception is NotSupportedException notSupported)
                throw new InvalidOperationException($"Seed {seed}, #{i} is not supported: {description}", notSupported);
            if (expected.Value == actual.Value && expected.Exception?.GetType() == actual.Exception?.GetType() &&
                expected.Exception?.Message == actual.Exception?.Message)
                continue;
            Assert.Fail($"Seed {seed}, #{i}: {description}\n" +
                        $"expected {expected.Value} {expected.Exception?.GetType().Name}: {expected.Exception?.Message}\n" +
                        $"actual {actual.Value} {actual.Exception?.GetType().Name}: {actual.Exception?.Message}\n" +
                        $"{actual.Exception}");
        }
    }

    static List<(string, Func<IQueryable<DiffRow>, IOrderedQueryable<DiffRow>>)> FirstOrderingKeys() =>
    [
        ("OrderBy(Tenant)", q => q.OrderBy(r => r.Tenant)),
        ("OrderByDescending(Tenant)", q => q.OrderByDescending(r => r.Tenant)),
        ("OrderBy(Code)", q => q.OrderBy(r => r.Code)),
        ("OrderBy(Age)", q => q.OrderBy(r => r.Age)),
        ("OrderByDescending(Score)", q => q.OrderByDescending(r => r.Score)),
        ("OrderBy(Name)", q => q.OrderBy(r => r.Name)),
        ("OrderByDescending(Created)", q => q.OrderByDescending(r => r.Created)),
        ("OrderBy(Bonus)", q => q.OrderBy(r => r.Bonus)),
        ("OrderBy(Kind)", q => q.OrderBy(r => r.Kind)),
        ("OrderBy(Ratio)", q => q.OrderBy(r => r.Ratio)),
        ("OrderByDescending(Price)", q => q.OrderByDescending(r => r.Price)),
        ("OrderBy(Age + Score)", q => q.OrderBy(r => r.Age + r.Score)),
    ];

    static List<(string, Func<IOrderedQueryable<DiffRow>, IOrderedQueryable<DiffRow>>)> NextOrderingKeys() =>
    [
        ("ThenBy(Tenant)", q => q.ThenBy(r => r.Tenant)),
        ("ThenByDescending(Code)", q => q.ThenByDescending(r => r.Code)),
        ("ThenBy(Code)", q => q.ThenBy(r => r.Code)),
        ("ThenByDescending(Age)", q => q.ThenByDescending(r => r.Age)),
        ("ThenBy(Note)", q => q.ThenBy(r => r.Note)),
        ("ThenBy(Token)", q => q.ThenBy(r => r.Token)),
        ("ThenByDescending(Seen)", q => q.ThenByDescending(r => r.Seen)),
    ];

    static List<(string, Func<IQueryable<DiffRow>, object?>)> WindowTerminals() =>
    [
        ("Enumerate", q => string.Join(",", q.AsEnumerable().Select(Describe))),
        ("Enumerate", q => string.Join(",", q.AsEnumerable().Select(Describe))),
        ("Count", q => q.Count()),
        ("Any", q => q.Any()),
        ("First", q => q.First()),
        ("FirstOrDefault", q => q.FirstOrDefault()),
        ("Last", q => q.Last()),
        ("SingleOrDefault", q => q.SingleOrDefault()),
        ("Min(Age)", q => q.Min(r => r.Age)),
        ("Sum(Score)", q => q.Sum(r => r.Score)),
        ("Average(Ratio)", q => q.Average(r => r.Ratio)),
    ];

    [Generate]
    public class DiffDto
    {
        public string? Code { get; set; }
        public int Age { get; set; }
        public DiffKind Kind { get; set; }
        public int? Bonus { get; set; }
        public override string ToString() => $"DiffDto {Code} {Age} {Kind} {Bonus}";
    }

    public record DiffRecord(string Code, long Score);

    [Generate]
    public class DiffValuesDto
    {
        public decimal Price { get; set; }
        public DateTime Created { get; set; }
        public Guid Token { get; set; }
        public short Delta { get; set; }
        public byte Level { get; set; }
        public DiffKind Kind { get; set; }
        public bool Active { get; set; }
        public long Score { get; set; }
        public DateTimeOffset Seen { get; set; }

        public override string ToString() =>
            $"DiffValuesDto {Price} {Created:O} {Token} {Delta} {Level} {Kind} {Active} {Score} {Seen:O}";
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void ProjectionsMatchLinqToObjects(int seed)
    {
        var random = new Random(seed);
        FillRows(random);

        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var generator = new RelationQueryPredicateGenerator(random);
        var firstKeys = FirstOrderingKeys();
        var projections = Projections();
        for (var i = 0; i < PredicatesPerSeed; i++)
        {
            var source = random.Next(3) == 0 ? null : generator.Generate();
            var (orderName, order) = random.Next(2) == 0 ? ("", null) : firstKeys[random.Next(firstKeys.Count)];
            var skip = random.Next(4) == 0 ? random.Next(20) : -1;
            var take = random.Next(4) == 0 ? random.Next(20) : -1;
            var pageAfterSelect = random.Next(2) == 0;
            var (projectionName, projection) = projections[random.Next(projections.Count)];
            var terminal = random.Next(9);
            var description = $"{source} / {orderName} / skip {skip} take {take} after {pageAfterSelect} / {projectionName} / {terminal}";

            IQueryable<DiffRow> Prepare(IQueryable<DiffRow> q)
            {
                if (order != null) q = order(q);
                if (!pageAfterSelect && skip >= 0) q = q.Skip(skip);
                if (!pageAfterSelect && take >= 0) q = q.Take(take);
                return q;
            }

            var expected = RunTerminal(() => projection(Prepare(source == null ? rows.AsQueryable() : rows.AsQueryable().Where(source)),
                new ProjectionTerminal(terminal, pageAfterSelect, skip, take)));
            var actual = RunTerminal(() => projection(Prepare(source == null
                ? table.Query<DiffRow>()
                : generator.ApplyAsWhereChain(table.Query<DiffRow>(), source)), new ProjectionTerminal(terminal, pageAfterSelect, skip, take)));
            if (actual.Exception is NotSupportedException notSupported)
                throw new InvalidOperationException($"Seed {seed}, #{i} is not supported: {description}", notSupported);
            if (expected.Value == actual.Value && expected.Exception?.GetType() == actual.Exception?.GetType() &&
                expected.Exception?.Message == actual.Exception?.Message)
                continue;
            Assert.Fail($"Seed {seed}, #{i}: {description}\n" +
                        $"expected {expected.Value} {expected.Exception?.GetType().Name}: {expected.Exception?.Message}\n" +
                        $"actual {actual.Value} {actual.Exception?.GetType().Name}: {actual.Exception?.Message}\n" +
                        $"{actual.Exception}");
        }
    }

    sealed record ProjectionTerminal(int Kind, bool PageAfterSelect, int Skip, int Take)
    {
        public object? Run<T>(IQueryable<T> q)
        {
            if (PageAfterSelect && Skip >= 0) q = q.Skip(Skip);
            if (PageAfterSelect && Take >= 0) q = q.Take(Take);
            return Kind switch
            {
                0 or 1 => string.Join("|", q.AsEnumerable().Select(v => Format(v))),
                2 => q.Count(),
                3 => Format(q.First()),
                4 => Format(q.FirstOrDefault()),
                5 => Format(q.Last()),
                6 => q.Any(),
                7 => Format(q.SingleOrDefault()),
                _ => string.Join("|", q.Skip(1).Take(3).AsEnumerable().Select(v => Format(v)))
            };
        }

        static string Format(object? value) => value is DiffRow row ? Describe(row) : value?.ToString() ?? "null";
    }

    static List<(string, Func<IQueryable<DiffRow>, ProjectionTerminal, object?>)> Projections() =>
    [
        ("Age", (q, t) => t.Run(q.Select(r => r.Age))),
        ("Name", (q, t) => t.Run(q.Select(r => r.Name))),
        ("Bonus", (q, t) => t.Run(q.Select(r => r.Bonus))),
        ("Kind", (q, t) => t.Run(q.Select(r => r.Kind))),
        ("Kind?", (q, t) => t.Run(q.Select(r => (DiffKind?)r.Kind))),
        ("anonymous enum", (q, t) => t.Run(q.Select(r => new { r.Kind, r.Level, r.Delta }))),
        ("Price * 2", (q, t) => t.Run(q.Select(r => r.Price * 2))),
        ("Code", (q, t) => t.Run(q.Select(r => r.Code))),
        ("anonymous", (q, t) => t.Run(q.Select(r => new { r.Tenant, r.Code, r.Note, r.Seen }))),
        ("tuple", (q, t) => t.Run(q.Select(r => ValueTuple.Create(r.Age, r.Created, r.Flag)))),
        ("tuple literal", (q, t) => t.Run(q.Select(r => new ValueTuple<string?, Guid>(r.Name, r.Token)))),
        ("dto", (q, t) => t.Run(q.Select(r => new DiffDto { Code = r.Code, Age = r.Age, Kind = r.Kind, Bonus = r.Bonus }))),
        ("record", (q, t) => t.Run(q.Select(r => new DiffRecord(r.Code, r.Score)))),
        ("anonymous values", (q, t) => t.Run(q.Select(r => new
        {
            r.Price, r.Duration, r.Bonus, r.Ratio, r.Rank, Kind = (DiffKind?)r.Kind, r.Active
        }))),
        ("dto values", (q, t) => t.Run(q.Select(r => new DiffValuesDto
        {
            Price = r.Price, Created = r.Created, Token = r.Token, Delta = r.Delta, Level = r.Level, Kind = r.Kind,
            Active = r.Active, Score = r.Score, Seen = r.Seen
        }))),
        ("Sum(Select(Age))", (q, _) => q.Select(r => r.Age).Sum()),
        ("Average(Select(Ratio))", (q, _) => q.Select(r => r.Ratio).Average()),
        ("Max(Select(Created))", (q, _) => q.Select(r => r.Created).Max()),
        ("Min(Select(Bonus))", (q, _) => q.Select(r => r.Bonus).Min()),
        ("Max(Select(Kind))", (q, _) => q.Select(r => r.Kind).Max()),
    ];

    static readonly DiffRow DefaultRow = new() { Code = "default" };

    // Each terminal gets the source and a predicate it may use as its argument.
    static List<(string, Func<IQueryable<DiffRow>, Expression<Func<DiffRow, bool>>, object?>)> Terminals() =>
    [
        ("Count", (q, _) => q.Count()),
        ("Count(p)", (q, p) => q.Count(p)),
        ("LongCount(p)", (q, p) => q.LongCount(p)),
        ("Any", (q, _) => q.Any()),
        ("Any(p)", (q, p) => q.Any(p)),
        ("All(p)", (q, p) => q.All(p)),
        ("First", (q, _) => q.First()),
        ("First(p)", (q, p) => q.First(p)),
        ("FirstOrDefault", (q, _) => q.FirstOrDefault()),
        ("FirstOrDefault(p, d)", (q, p) => q.FirstOrDefault(p, DefaultRow)),
        ("Single", (q, _) => q.Single()),
        ("Single(p)", (q, p) => q.Single(p)),
        ("SingleOrDefault(p)", (q, p) => q.SingleOrDefault(p)),
        ("Last", (q, _) => q.Last()),
        ("Last(p)", (q, p) => q.Last(p)),
        ("LastOrDefault(d)", (q, _) => q.LastOrDefault(DefaultRow)),
        ("Sum(Age)", (q, _) => q.Sum(r => r.Age)),
        ("Sum(Score)", (q, _) => q.Sum(r => r.Score)),
        ("Sum(Ratio)", (q, _) => q.Sum(r => r.Ratio)),
        ("Sum(Price)", (q, _) => q.Sum(r => r.Price)),
        ("Sum(Bonus)", (q, _) => q.Sum(r => r.Bonus)),
        ("Sum(float)", (q, _) => q.Sum(r => (float)r.Ratio)),
        ("Sum(Level)", (q, _) => q.Sum(r => r.Level)),
        ("Average(Age)", (q, _) => q.Average(r => r.Age)),
        ("Average(Score)", (q, _) => q.Average(r => r.Score)),
        ("Average(Ratio)", (q, _) => q.Average(r => r.Ratio)),
        ("Average(Price)", (q, _) => q.Average(r => r.Price)),
        ("Average(Bonus)", (q, _) => q.Average(r => r.Bonus)),
        ("Average(float)", (q, _) => q.Average(r => (float?)r.Ratio)),
        ("Min(Age)", (q, _) => q.Min(r => r.Age)),
        ("Max(Score * 2)", (q, _) => q.Max(r => r.Score * 2)),
        ("Min(Created)", (q, _) => q.Min(r => r.Created)),
        ("Max(Kind)", (q, _) => q.Max(r => r.Kind)),
        ("Min(Bonus)", (q, _) => q.Min(r => r.Bonus)),
        ("Max(Bonus)", (q, _) => q.Max(r => r.Bonus)),
        ("Max(Token)", (q, _) => q.Max(r => r.Token)),
        ("Min(Seen)", (q, _) => q.Min(r => r.Seen)),
        ("Max(Duration)", (q, _) => q.Max(r => r.Duration)),
        ("Min(Price)", (q, _) => q.Min(r => r.Price)),
        ("Max(Ratio)", (q, _) => q.Max(r => r.Ratio)),
        ("Min(Active)", (q, _) => q.Min(r => r.Active)),
        ("Max(Tenant)", (q, _) => q.Max(r => r.Tenant)),
    ];

    static (string? Value, Exception? Exception) RunTerminal(Func<object?> terminal)
    {
        try
        {
            var value = terminal();
            return (value is DiffRow row ? Describe(row) : value is null ? "null" : $"{value.GetType().Name}:{value}",
                null);
        }
        catch (Exception exception)
        {
            return (null, exception);
        }
    }

    static (List<string> Rows, Exception? Exception) Run(Func<IEnumerable<DiffRow>> query)
    {
        var result = new List<string>();
        try
        {
            foreach (var row in query()) result.Add(Describe(row));
            return (result, null);
        }
        catch (Exception exception)
        {
            return (result, exception);
        }
    }

    /// Last after Select returns the projection of the last row of the window. Before the fix a projection computed
    /// while scanning kept the values of the last row scanned: one past Take, or the last tested row of the scan.
    [Fact]
    public void LastAfterSelectProjectsTheLastRowOfTheWindow()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IDiffRowTable>();
            for (var i = 0; i < 6; i++)
                table.Insert(new() { Tenant = (ulong)(i / 2), Code = $"c{i}", Age = 20 + i, Bonus = i % 3 == 0 ? i : null });
            tr.Commit();
        }

        using var read = _db.StartReadOnlyTransaction();
        var rows = read.GetRelation<IDiffRowTable>();
        Assert.Equal(22, rows.Query<DiffRow>().Take(3).Select(r => r.Age).Last());
        Assert.Equal(23, rows.Query<DiffRow>().Where(r => r.Bonus != null).OrderBy(r => r.Tenant).Select(r => r.Age).Last());
        Assert.Equal("c3", rows.Query<DiffRow>().Where(r => r.Bonus != null).OrderBy(r => r.Tenant)
            .Select(r => new { r.Code }).Last().Code);
    }


    /// Queries written as chains on `Query<T>()` are intercepted by the generated code at compile time, unlike the
    /// queries above, which are built at runtime; they must return what the same chains return over the rows.
    [Theory]
    [MemberData(nameof(Seeds))]
    public void InterceptedQueryChainsMatchLinq(int seed)
    {
        var random = new Random(seed);
        FillRows(random);
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        var rows = table.ToList();
        var failures = new List<string>();
        var t = (ulong)random.Next(4);
        var a = 20 + random.Next(12);
        var n = Names[random.Next(Names.Length)];
        var k = (DiffKind)random.Next(4);
        var d = BaseDate.AddDays(random.Next(30));
        var skip = random.Next(5);
        var take = random.Next(1, 20);
        var p = (decimal)random.Next(100) / 4;
        var ages = new[] { 20, 25, 30, a };
        void Check(string name, Func<object?> expected, Func<object?> actual)
        {
            var e = Format(expected);
            var x = Format(actual);
            if (e != x) failures.Add($"{name}:\n  expected {e}\n  actual   {x}");
        }
        Check("chain 1", () => rows.Where(r => r.Tenant == t && r.Age > a).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t && r.Age > a).ToList());
        Check("chain 2", () => rows.Where(r => r.Name == n).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Name == n).ToList());
        Check("chain 3", () => rows.Where(r => r.Name != null && r.Name.StartsWith("n")).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Name != null && r.Name.StartsWith("n")).ToList());
        Check("chain 4", () => rows.Where(r => r.Kind == k || r.Bonus == null).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Kind == k || r.Bonus == null).ToList());
        Check("chain 5", () => rows.Where(r => r.Created >= d).OrderBy(r => r.Created).ThenByDescending(r => r.Code).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Created >= d).OrderBy(r => r.Created).ThenByDescending(r => r.Code).ToList());
        Check("chain 6", () => rows.Where(r => !r.Active).Skip(skip).Take(take).ToList(),
            () => table.Query<DiffRow>().Where(r => !r.Active).Skip(skip).Take(take).ToList());
        Check("chain 7", () => rows.Where(r => ages.Contains(r.Age)).Select(r => r.Code).ToList(),
            () => table.Query<DiffRow>().Where(r => ages.Contains(r.Age)).Select(r => r.Code).ToList());
        Check("chain 8", () => rows.Where(r => r.Price > p).Sum(r => r.Price),
            () => table.Query<DiffRow>().Where(r => r.Price > p).Sum(r => r.Price));
        Check("chain 9", () => rows.Count(r => r.Flag == true),
            () => table.Query<DiffRow>().Count(r => r.Flag == true));
        Check("chain 10", () => rows.Any(r => r.Age == a),
            () => table.Query<DiffRow>().Any(r => r.Age == a));
        Check("chain 11", () => rows.All(r => r.Age > 0),
            () => table.Query<DiffRow>().All(r => r.Age > 0));
        Check("chain 12", () => rows.Where(r => r.Tenant == t).Min(r => r.Score),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).Min(r => r.Score));
        Check("chain 13", () => rows.Where(r => r.Tenant == t).Max(r => r.Ratio),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).Max(r => r.Ratio));
        Check("chain 14", () => rows.Where(r => r.Tenant == t).Average(r => r.Age),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).Average(r => r.Age));
        Check("chain 15", () => rows.OrderByDescending(r => r.Score).ThenBy(r => r.Code).FirstOrDefault(),
            () => table.Query<DiffRow>().OrderByDescending(r => r.Score).ThenBy(r => r.Code).FirstOrDefault());
        Check("chain 16", () => rows.Where(r => r.Tenant == t).OrderBy(r => r.Name).Select(r => new { r.Name, r.Age }).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).OrderBy(r => r.Name).Select(r => new { r.Name, r.Age }).ToList());
        Check("chain 17", () => rows.Where(r => r.Note != null && r.Note.Contains("l")).Count(),
            () => table.Query<DiffRow>().Where(r => r.Note != null && r.Note.Contains("l")).Count());
        Check("chain 18", () => rows.Where(r => r.Bonus > a).Select(r => r.Bonus).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Bonus > a).Select(r => r.Bonus).ToList());
        Check("chain 19", () => rows.Where(r => r.Level >= 3 && r.Delta < 0).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Level >= 3 && r.Delta < 0).ToList());
        Check("chain 20", () => rows.Where(r => r.Rank % 3 == 1).Take(take).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Rank % 3 == 1).Take(take).ToList());
        Check("chain 21", () => rows.Where(r => r.Code.Length > 2).OrderBy(r => r.Code).Skip(skip).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Code.Length > 2).OrderBy(r => r.Code).Skip(skip).ToList());
        Check("chain 22", () => rows.Where(r => r.Duration > TimeSpan.FromHours(1)).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Duration > TimeSpan.FromHours(1)).ToList());
        Check("chain 23", () => rows.Where(r => r.Token == Guid.Empty).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Token == Guid.Empty).ToList());
        Check("chain 24", () => rows.Where(r => r.Tenant == t).Select(r => r.Age * 2 + 1).ToList(),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).Select(r => r.Age * 2 + 1).ToList());
        Check("chain 25", () => rows.Where(r => r.Name == null).Count(),
            () => table.Query<DiffRow>().Where(r => r.Name == null).Count());
        Check("chain 26", () => rows.Where(r => string.Compare(r.Code, "m", StringComparison.Ordinal) > 0).ToList(),
            () => table.Query<DiffRow>().Where(r => string.Compare(r.Code, "m", StringComparison.Ordinal) > 0).ToList());
        Check("chain 27", () => rows.Where(r => r.Tenant == t && r.Code == "x").SingleOrDefault(),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t && r.Code == "x").SingleOrDefault());
        Check("chain 28", () => rows.Where(r => r.Seen > new DateTimeOffset(BaseDate)).OrderBy(r => r.Seen).Select(r => r.Code).FirstOrDefault(),
            () => table.Query<DiffRow>().Where(r => r.Seen > new DateTimeOffset(BaseDate)).OrderBy(r => r.Seen).Select(r => r.Code).FirstOrDefault());
        Check("chain 29", () => rows.OrderBy(r => r.Age).ThenBy(r => r.Tenant).ThenBy(r => r.Code).Skip(skip).Take(take).Select(r => r.Code).ToList(),
            () => table.Query<DiffRow>().OrderBy(r => r.Age).ThenBy(r => r.Tenant).ThenBy(r => r.Code).Skip(skip).Take(take).Select(r => r.Code).ToList());
        Check("chain 30", () => rows.Where(r => r.Kind != DiffKind.A).Count(r => r.Active),
            () => table.Query<DiffRow>().Where(r => r.Kind != DiffKind.A).Count(r => r.Active));
        Check("chain 31", () => rows.Where(r => r.Tenant == t).LongCount(),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).LongCount());
        Check("chain 32", () => rows.Where(r => r.Tenant == t).OrderBy(r => r.Code).Last(),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).OrderBy(r => r.Code).Last());
        Check("chain 33", () => rows.Where(r => r.Tenant == t).Sum(r => (long)r.Rank),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).Sum(r => (long)r.Rank));
        Check("chain 34", () => rows.Where(r => r.Tenant == t).Max(r => r.Bonus),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).Max(r => r.Bonus));
        Check("chain 35", () => rows.Where(r => r.Tenant == t).Average(r => r.Bonus),
            () => table.Query<DiffRow>().Where(r => r.Tenant == t).Average(r => r.Bonus));
        Assert.True(failures.Count == 0, $"Seed {seed}:\n" + string.Join("\n", failures));
    }

    void FillRows(Random random)
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDiffRowTable>();
        for (var i = 0; i < RowCount; i++)
        {
            table.Insert(new()
            {
                Tenant = (ulong)random.Next(4),
                // Some keys are non-ASCII, so ordered string decoding sees multi-byte and surrogate chars; one key is
                // null, which string methods on it must throw for.
                Code = i == 7 ? null! : i % 9 == 0 ? $"ž{i:D3}😀" : $"c{i:D3}",
                Name = Names[random.Next(Names.Length)],
                Age = 20 + random.Next(10),
                Score = random.Next(-5, 6),
                Rank = (uint)random.Next(8),
                Active = random.Next(2) == 0,
                Created = BaseDate.AddDays(random.Next(5)),
                Ratio = RelationQueryPredicateGenerator.Ratios[random.Next(RelationQueryPredicateGenerator.Ratios.Length - 1)],
                Level = (byte)random.Next(250, 256),
                Delta = (short)random.Next(-3, 4),
                Kind = (DiffKind)random.Next(4),
                Bonus = random.Next(3) == 0 ? null : random.Next(-2, 3),
                Flag = random.Next(3) == 0 ? null : random.Next(2) == 0,
                Token = Tokens[random.Next(Tokens.Length)],
                Price = random.Next(-4, 5) * 1.25m,
                Duration = TimeSpan.FromMinutes(random.Next(-2, 3)),
                Seen = new(BaseDate.AddHours(random.Next(3)).Ticks, TimeSpan.FromHours(random.Next(-1, 2))),
                Note = Notes[random.Next(Notes.Length)]
            });
        }

        tr.Commit();
    }

    static string Describe(DiffRow row) =>
        $"{row.Tenant}/{row.Code}/{row.Name ?? "null"}/{row.Age}/{row.Score}/{row.Rank}/{row.Active}/" +
        $"{row.Created:yyyy-MM-dd}/{row.Ratio}/{row.Level}/{row.Delta}/{row.Kind}/{row.Bonus}/{row.Flag}/" +
        $"{row.Token}/{row.Price}/{row.Duration}/{row.Seen:O}/{row.Note ?? "null"}";

    /// Holds captured variables, as the C# compiler does: a field access on a constant closure object.
    sealed class Captured<T>
    {
        public T Value = default!;
    }

    sealed class RelationQueryPredicateGenerator
    {
        // The last ratio never appears in data, so it always produces a miss.
        public static readonly double[] Ratios = [-1.0, 0.5, 1.5, 2.25, 3.0];

        static readonly QueryOperator[] AllComparisons =
            [QueryOperator.Equal, QueryOperator.NotEqual, QueryOperator.Less, QueryOperator.LessOrEqual,
                QueryOperator.Greater, QueryOperator.GreaterOrEqual];

        static readonly QueryOperator[] Equalities = [QueryOperator.Equal, QueryOperator.NotEqual];

        enum QueryOperator
        {
            Equal,
            NotEqual,
            Less,
            LessOrEqual,
            Greater,
            GreaterOrEqual
        }

        readonly Random _random;
        readonly ParameterExpression _parameter = Expression.Parameter(typeof(DiffRow), "p");
        readonly List<Func<Expression>> _leaves = [];

        public RelationQueryPredicateGenerator(Random random)
        {
            _random = random;
            // Constant domains are a bit wider than the generated data, so predicates also miss.
            AddCompare(nameof(DiffRow.Tenant), () => (ulong)Pick(5), AllComparisons);
            AddCompare(nameof(DiffRow.Code), () => $"c{Pick(RowCount + 20):D3}", Equalities);
            AddCompare(nameof(DiffRow.Name), () => Names[Pick(Names.Length)], Equalities);
            AddCompare(nameof(DiffRow.Note), () => Notes[Pick(Notes.Length)], Equalities);
            AddCompare(nameof(DiffRow.Age), () => 19 + Pick(12), AllComparisons);
            AddCompare(nameof(DiffRow.Score), () => (long)Pick(13) - 6, AllComparisons);
            AddCompare(nameof(DiffRow.Rank), () => (uint)Pick(9), AllComparisons);
            AddCompare(nameof(DiffRow.Active), () => Pick(2) == 0, Equalities);
            AddCompare(nameof(DiffRow.Created), () => BaseDate.AddDays(Pick(6)), AllComparisons);
            AddCompare(nameof(DiffRow.Ratio), () => Ratios[Pick(Ratios.Length)], AllComparisons);
            AddCompare(nameof(DiffRow.Token), () => Tokens[Pick(Tokens.Length)], Equalities);
            AddCompare(nameof(DiffRow.Price), () => (Pick(11) - 5) * 1.25m, AllComparisons);
            AddCompare(nameof(DiffRow.Duration), () => TimeSpan.FromMinutes(Pick(5) - 2), AllComparisons);
            AddCompare(nameof(DiffRow.Seen), () => new DateTimeOffset(BaseDate.AddHours(Pick(4)).Ticks,
                TimeSpan.FromHours(Pick(3) - 1)), AllComparisons);
            AddCompare(nameof(DiffRow.Bonus), () => Pick(4) == 0 ? null : (int?)(Pick(5) - 2), AllComparisons);
            AddCompare(nameof(DiffRow.Flag), () => Pick(3) == 0 ? null : (bool?)(Pick(2) == 0), Equalities);
            // C# compares byte, short and enum fields after converting them to int.
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Level)), typeof(int)),
                Constant(250 + Pick(7)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Delta)), typeof(int)),
                Constant(Pick(9) - 4), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Kind)), typeof(int)),
                Expression.Convert(Constant((DiffKind)Pick(5)), typeof(int)), PickOf(AllComparisons)));
            // Comparing two fields.
            _leaves.Add(() => Compare(Property(nameof(DiffRow.Score)),
                Expression.Convert(Property(nameof(DiffRow.Delta)), typeof(long)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Property(nameof(DiffRow.Bonus)),
                Expression.Convert(Property(nameof(DiffRow.Delta)), typeof(int?)), PickOf(AllComparisons)));
            // Both operands nullable: null equals only null.
            _leaves.Add(() => Compare(Property(nameof(DiffRow.Bonus)),
                Expression.Add(Property(nameof(DiffRow.Bonus)), Constant((int?)(Pick(3) - 1), typeof(int?))),
                PickOf(AllComparisons)));
            // Arithmetic, including division by zero and checked overflow.
            _leaves.Add(() => Compare(Expression.Add(Property(nameof(DiffRow.Age)), Constant(Pick(5))),
                Constant(20 + Pick(15)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Multiply(Property(nameof(DiffRow.Price)), Constant(2m)),
                Constant((Pick(11) - 5) * 2.5m), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Divide(Constant(12u), Property(nameof(DiffRow.Rank))),
                Constant((uint)Pick(13)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Subtract(Property(nameof(DiffRow.Ratio)), Constant(0.5)),
                Constant(Ratios[Pick(Ratios.Length)]), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Add(Property(nameof(DiffRow.Bonus)), Constant((int?)1, typeof(int?))),
                Constant((int?)(Pick(5) - 2), typeof(int?)), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.AddChecked(Property(nameof(DiffRow.Score)),
                Constant(Pick(10) == 0 ? long.MaxValue : 1L)), Constant(0L), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Negate(Property(nameof(DiffRow.Score))), Constant((long)Pick(5) - 2),
                PickOf(AllComparisons)));
            // Bool members and nullable members.
            _leaves.Add(() => Property(nameof(DiffRow.Active)));
            _leaves.Add(() => Expression.Property(Property(nameof(DiffRow.Flag)), "HasValue"));
            _leaves.Add(() => Expression.Property(Property(nameof(DiffRow.Bonus)), "HasValue"));
            _leaves.Add(() => Expression.Property(Property(nameof(DiffRow.Flag)), "Value"));
            _leaves.Add(() => Compare(Expression.Property(Property(nameof(DiffRow.Bonus)), "Value"),
                Constant(Pick(5) - 2), PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Convert(Property(nameof(DiffRow.Bonus)), typeof(int)),
                Constant(Pick(5) - 2), PickOf(AllComparisons)));
            // Strings.
            _leaves.Add(() => StringCall(nameof(string.StartsWith), StringProperty(), PickPrefix(),
                StringComparison.Ordinal));
            _leaves.Add(() => StringCall(nameof(string.StartsWith), StringProperty(), PickPrefix(), null));
            _leaves.Add(() => StringCall(nameof(string.EndsWith), StringProperty(), PickSuffix(),
                StringComparison.OrdinalIgnoreCase));
            _leaves.Add(() => StringCall(nameof(string.Contains), StringProperty(), PickSuffix(), null));
            _leaves.Add(() => Expression.Call(StringProperty(), typeof(string).GetMethod(nameof(string.Contains), [typeof(char)])!,
                Constant("xaé😀"[Pick(3)])));
            _leaves.Add(() => StringCall(nameof(string.Equals), StringProperty(), Notes[Pick(Notes.Length - 1)],
                StringComparison.OrdinalIgnoreCase));
            _leaves.Add(() => Expression.Call(typeof(string).GetMethod(nameof(string.Equals),
                    [typeof(string), typeof(string), typeof(StringComparison)])!, StringProperty(),
                Constant(Notes[Pick(Notes.Length)]), Constant(StringComparison.OrdinalIgnoreCase)));
            _leaves.Add(() => Expression.Call(typeof(string).GetMethod(nameof(string.IsNullOrEmpty))!,
                StringProperty()));
            _leaves.Add(() => Compare(Expression.Property(StringProperty(), nameof(string.Length)), Constant(Pick(6)),
                PickOf(AllComparisons)));
            _leaves.Add(() => Compare(Expression.Call(typeof(string).GetMethod(nameof(string.CompareOrdinal),
                        [typeof(string), typeof(string)])!, Property(nameof(DiffRow.Code)),
                    Constant($"c{Pick(RowCount):D3}")), Constant(0), PickOf(AllComparisons)));
            // Contains on constant collections.
            _leaves.Add(() => Expression.Call(EnumerableContains<int>(), Constant(new[] { 19 + Pick(12), 19 + Pick(12) }),
                Property(nameof(DiffRow.Age))));
            _leaves.Add(() => Expression.Call(EnumerableContains<DiffKind>(),
                Constant(new[] { (DiffKind)Pick(4), (DiffKind)Pick(4) }), Property(nameof(DiffRow.Kind))));
            _leaves.Add(() => Expression.Call(EnumerableContains<string?>(),
                Constant(new List<string?> { Names[Pick(Names.Length)], Names[Pick(Names.Length)] }),
                Property(nameof(DiffRow.Name))));
            _leaves.Add(() => Expression.Call(Constant(new HashSet<long> { Pick(13) - 6, Pick(13) - 6 }),
                typeof(HashSet<long>).GetMethod(nameof(HashSet<long>.Contains))!, Property(nameof(DiffRow.Score))));
            _leaves.Add(() => Expression.Call(EnumerableContains<Guid>(), Constant(new[] { Tokens[Pick(Tokens.Length)] }),
                Property(nameof(DiffRow.Token))));
            // Contains on key fields becomes several seeks.
            _leaves.Add(() => Expression.Call(EnumerableContains<ulong>(),
                Constant(new[] { (ulong)Pick(5), (ulong)Pick(5) }), Property(nameof(DiffRow.Tenant))));
            _leaves.Add(() => Expression.Call(EnumerableContains<string>(),
                Constant(new[] { $"c{Pick(RowCount):D3}", $"ž{Pick(RowCount / 9) * 9:D3}😀", $"c{Pick(RowCount):D3}" }),
                Property(nameof(DiffRow.Code))));
        }

        int Pick(int count) => _random.Next(count);
        T PickOf<T>(T[] values) => values[_random.Next(values.Length)];

        string PickPrefix() => PickOf(["", "c0", "c01", "C0", "n", "Á", "😀", "a", "x😀", "hello", "ž0", "ž"]);
        string PickSuffix() => PickOf(["", "1", "x", "c", "😀", "ť", "BC", "world", "o w"]);

        Expression StringProperty() =>
            Property(PickOf([nameof(DiffRow.Code), nameof(DiffRow.Name), nameof(DiffRow.Note)]));

        MemberExpression Property(string name) => Expression.Property(_parameter, name);

        static MethodInfo EnumerableContains<T>() =>
            new Func<IEnumerable<T>, T, bool>(Enumerable.Contains).Method;

        void AddCompare<T>(string name, Func<T> value, QueryOperator[] operators)
        {
            _leaves.Add(() => Compare(Property(name), Constant(value()), PickOf(operators)));
        }

        Expression Constant<T>(T value) => Constant(value, typeof(T));

        Expression Constant(object? value, Type type)
        {
            if (Pick(2) == 0) return Expression.Constant(value, type);
            var capturedType = typeof(Captured<>).MakeGenericType(type);
            var captured = Activator.CreateInstance(capturedType)!;
            var field = capturedType.GetField(nameof(Captured<int>.Value))!;
            field.SetValue(captured, value);
            return Expression.Field(Expression.Constant(captured), field);
        }

        Expression StringCall(string method, Expression target, string argument, StringComparison? comparison)
        {
            return comparison == null
                ? Expression.Call(target, typeof(string).GetMethod(method, [typeof(string)])!, Constant(argument))
                : Expression.Call(target, typeof(string).GetMethod(method, [typeof(string), typeof(StringComparison)])!,
                    Constant(argument), Expression.Constant(comparison.Value));
        }

        Expression Compare(Expression left, Expression right, QueryOperator op)
        {
            if (Pick(4) == 0)
            {
                (left, right) = (right, left);
                op = op switch
                {
                    QueryOperator.Less => QueryOperator.Greater,
                    QueryOperator.LessOrEqual => QueryOperator.GreaterOrEqual,
                    QueryOperator.Greater => QueryOperator.Less,
                    QueryOperator.GreaterOrEqual => QueryOperator.LessOrEqual,
                    _ => op
                };
            }

            return op switch
            {
                QueryOperator.Equal => Expression.Equal(left, right),
                QueryOperator.NotEqual => Expression.NotEqual(left, right),
                QueryOperator.Less => Expression.LessThan(left, right),
                QueryOperator.LessOrEqual => Expression.LessThanOrEqual(left, right),
                QueryOperator.Greater => Expression.GreaterThan(left, right),
                _ => Expression.GreaterThanOrEqual(left, right)
            };
        }

        public Expression<Func<DiffRow, bool>> Generate()
        {
            return Expression.Lambda<Func<DiffRow, bool>>(GenerateNode(_random.Next(1, 5)), _parameter);
        }

        public IQueryable<DiffRow> ApplyAsWhereChain(IQueryable<DiffRow> source,
            Expression<Func<DiffRow, bool>> predicate)
        {
            if (_random.Next(2) == 0) return source.Where(predicate);
            var conjuncts = new List<Expression>();
            Flatten(predicate.Body, conjuncts);
            foreach (var conjunct in conjuncts)
                source = source.Where(Expression.Lambda<Func<DiffRow, bool>>(conjunct, _parameter));
            return source;
        }

        static void Flatten(Expression expression, List<Expression> conjuncts)
        {
            if (expression.NodeType == ExpressionType.AndAlso)
            {
                var binary = (BinaryExpression)expression;
                Flatten(binary.Left, conjuncts);
                Flatten(binary.Right, conjuncts);
                return;
            }

            conjuncts.Add(expression);
        }

        Expression GenerateNode(int depth)
        {
            if (depth <= 1 || _random.Next(3) == 0)
            {
                if (_random.Next(25) == 0) return Expression.Constant(_random.Next(2) == 0);
                var leaf = _leaves[_random.Next(_leaves.Count)]();
                return _random.Next(6) == 0 ? Expression.Not(leaf) : leaf;
            }

            var left = GenerateNode(depth - 1);
            var right = GenerateNode(depth - 1);
            var node = _random.Next(2) == 0 ? Expression.AndAlso(left, right) : Expression.OrElse(left, right);
            return _random.Next(8) == 0 ? Expression.Not(node) : node;
        }
    }
}
