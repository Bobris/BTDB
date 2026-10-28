using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Secondary keys change their field order, gain fields, appear on a new field and disappear or start with the primary key while their fields widen
/// from 32 to 64 bits across versions of one relation. After every upgrade and every batch of random changes, each
/// index of the current version lists exactly the rows a model predicts, in key order.
public class RelationIndexEvolutionTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class EvRowV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("AB", Order = 1)] public uint A { get; set; }
        [SecondaryKey("AB", Order = 2)] public string? B { get; set; }
        [SecondaryKey("C")] public int C { get; set; }
    }

    [BTDB.Generate]
    public class EvRowV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("AB", Order = 2)]
        [SecondaryKey("CA", Order = 2)]
        public ulong A { get; set; }

        [SecondaryKey("AB", Order = 1)] public string? B { get; set; }
        [SecondaryKey("CA", Order = 1)] public long C { get; set; }
        [SecondaryKey("D")] public string? D { get; set; }
    }

    [BTDB.Generate]
    public class EvRowV3
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("AB", Order = 1)] public ulong A { get; set; }
        [SecondaryKey("AB", Order = 2)] public string? B { get; set; }
        [SecondaryKey("C", IncludePrimaryKeyOrder = 1)] public long C { get; set; }
        public string? D { get; set; }
    }

    [BTDB.Generate]
    public interface IEvV1Table : IRelation<EvRowV1>
    {
        bool RemoveById(ulong id);
        IEnumerable<EvRowV1> FindByAB(uint a);
        IEnumerable<EvRowV1> FindByAB(uint a, string? b);
        IEnumerable<EvRowV1> FindByC(int c);
    }

    [BTDB.Generate]
    public interface IEvV2Table : IRelation<EvRowV2>
    {
        bool RemoveById(ulong id);
        IEnumerable<EvRowV2> FindByAB(string? b);
        IEnumerable<EvRowV2> FindByAB(string? b, ulong a);
        IEnumerable<EvRowV2> FindByCA(long c);
        IEnumerable<EvRowV2> FindByD(string? d);
    }

    [BTDB.Generate]
    public interface IEvV3Table : IRelation<EvRowV3>
    {
        bool RemoveById(ulong id);
        IEnumerable<EvRowV3> FindByAB(ulong a);
        IEnumerable<EvRowV3> FindByAB(ulong a, string? b);
        IEnumerable<EvRowV3> FindByC(ulong id);
        IEnumerable<EvRowV3> FindByC(ulong id, long c);
    }

    sealed record Model(ulong Id, ulong A, string? B, long C, string? D);

    static readonly string?[] Strings = [null, "", "a", "ab", "b"];
    static readonly uint[] As = [0, 1, 127, 128, uint.MaxValue];
    static readonly int[] Cs = [int.MinValue, -1, 0, 1, int.MaxValue];
    static readonly long[] WideCs = [long.MinValue, -1L << 40, 0, 1L << 40, long.MaxValue];

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    // Null strings sort last; others are ordinal.
    static int CompareString(string? x, string? y) =>
        x == null ? y == null ? 0 : 1 : y == null ? -1 : string.CompareOrdinal(x, y);

    static IEnumerable<Model> By<TKey>(IEnumerable<Model> rows, Func<Model, TKey> key, Comparison<TKey> compare) =>
        rows.OrderBy(key, Comparer<TKey>.Create(compare)).ThenBy(r => r.Id);

    static string D(Model m) => $"{m.Id} {m.A} {m.B ?? "null"} {m.C} {m.D ?? "null"}";

    [Theory]
    [MemberData(nameof(Seeds))]
    public void IndexesMatchRowsAcrossUpgrades(int seed)
    {
        var random = new Random(seed);
        var model = new SortedDictionary<ulong, Model>();
        var ids = Enumerable.Range(1, 15).Select(i => (ulong)i).ToArray();

        void Check(string stage, Func<IEnumerable<string>> all, IEnumerable<(string What, IEnumerable<string> Actual,
            IEnumerable<Model> Expected)> lookups)
        {
            Assert.Equal(model.Values.Select(D), all());
            foreach (var (what, actual, expected) in lookups)
                Assert.True(expected.Select(D).SequenceEqual(actual), $"Seed {seed} {stage}: {what} expected {string.Join(" | ", expected.Select(D))} actual {string.Join(" | ", actual)}");
        }

        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IEvV1Table>("Ev")(tr);
            for (var i = 0; i < 40; i++)
            {
                var id = ids[random.Next(ids.Length)];
                if (random.Next(5) == 0)
                {
                    Assert.Equal(model.Remove(id), table.RemoveById(id));
                    continue;
                }

                var row = new EvRowV1
                {
                    Id = id, A = As[random.Next(As.Length)], B = Strings[random.Next(Strings.Length)],
                    C = Cs[random.Next(Cs.Length)]
                };
                table.Upsert(row);
                model[id] = new(id, row.A, row.B, row.C, null);
            }

            static string V1(EvRowV1 r) => $"{r.Id} {r.A} {r.B ?? "null"} {r.C} null";
            Check("v1", () => table.Select(V1), As.SelectMany(a => Strings.Select(b =>
                    ($"AB {a} {b}", table.FindByAB(a, b).Select(V1),
                        model.Values.Where(m => m.A == a && m.B == b))))
                .Concat(As.Select(a => ($"AB {a}", table.FindByAB(a).Select(V1),
                    By(model.Values.Where(m => m.A == a), m => m.B, CompareString))))
                .Concat(Cs.Select(c => ($"C {c}", table.FindByC(c).Select(V1),
                    model.Values.Where(m => m.C == c)))));
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IEvV2Table>("Ev")(tr);
            static string V2(EvRowV2 r) => $"{r.Id} {r.A} {r.B ?? "null"} {r.C} {r.D ?? "null"}";

            void CheckV2(string stage)
            {
                var allA = model.Values.Select(m => m.A).Distinct().ToList();
                var allC = model.Values.Select(m => m.C).Concat(WideCs).Distinct().ToList();
                Check(stage, () => table.Select(V2), Strings.SelectMany(b => allA.Select(a =>
                        ($"AB {b} {a}", table.FindByAB(b, a).Select(V2),
                            model.Values.Where(m => m.A == a && m.B == b))))
                    .Concat(Strings.Select(b => ($"AB {b}", table.FindByAB(b).Select(V2),
                        By(model.Values.Where(m => m.B == b), m => m.A, (x, y) => x.CompareTo(y)))))
                    .Concat(allC.Select(c => ($"CA {c}", table.FindByCA(c).Select(V2),
                        By(model.Values.Where(m => m.C == c), m => m.A, (x, y) => x.CompareTo(y)))))
                    .Concat(Strings.Select(d => ($"D {d}", table.FindByD(d).Select(V2),
                        model.Values.Where(m => m.D == d)))));
            }

            CheckV2("v2 upgraded");
            for (var i = 0; i < 40; i++)
            {
                var id = ids[random.Next(ids.Length)];
                if (random.Next(5) == 0)
                {
                    Assert.Equal(model.Remove(id), table.RemoveById(id));
                    continue;
                }

                var row = new EvRowV2
                {
                    Id = id, A = random.Next(2) == 0 ? As[random.Next(As.Length)] : ulong.MaxValue - (ulong)random.Next(3),
                    B = Strings[random.Next(Strings.Length)], C = WideCs[random.Next(WideCs.Length)],
                    D = Strings[random.Next(Strings.Length)]
                };
                table.Upsert(row);
                model[id] = new(id, row.A, row.B, row.C, row.D);
            }

            CheckV2("v2 changed");
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IEvV3Table>("Ev")(tr);
            static string V3(EvRowV3 r) => $"{r.Id} {r.A} {r.B ?? "null"} {r.C} {r.D ?? "null"}";
            var allA = model.Values.Select(m => m.A).Distinct().ToList();
            var allC = model.Values.Select(m => m.C).Concat(WideCs).Distinct().ToList();
            Check("v3", () => table.Select(V3), allA.SelectMany(a => Strings.Select(b =>
                    ($"AB {a} {b}", table.FindByAB(a, b).Select(V3),
                        model.Values.Where(m => m.A == a && m.B == b))))
                .Concat(allA.Select(a => ($"AB {a}", table.FindByAB(a).Select(V3),
                    By(model.Values.Where(m => m.A == a), m => m.B, CompareString))))
                .Concat(ids.SelectMany(id => allC.Select(c => ($"C {id} {c}", table.FindByC(id, c).Select(V3),
                    model.Values.Where(m => m.Id == id && m.C == c)))))
                .Concat(ids.Select(id => ($"C {id}", table.FindByC(id).Select(V3),
                    model.Values.Where(m => m.Id == id)))));
        }
    }
}
