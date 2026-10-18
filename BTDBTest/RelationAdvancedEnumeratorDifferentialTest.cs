using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Compares ListBy/CountBy/AnyBy/RemoveBy methods taking AdvancedEnumeratorParam against a LINQ reference model for all
// combinations of order and start/end propositions (including bounds which are missing or outside of data).
// Every test runs with source generated relations and with IL emitted relations (RelationBuilder).
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class RelationAdvancedEnumeratorDifferentialTest : ObjectDbTestBase
{
    public RelationAdvancedEnumeratorDifferentialTest(ITestOutputHelper output) : base(output)
    {
    }

    public class ARow
    {
        [PrimaryKey(1)] public ulong T { get; set; }
        [PrimaryKey(2)] public long K { get; set; }
        [PrimaryKey(3)] public ulong V { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; } = "";

        public override string ToString() => $"{T}|{K}|{V}|{Name}";
    }

    public interface IARowTable : IRelation<ARow>
    {
        void Insert(ARow row);
        IEnumerable<ARow> ListById(ulong t, AdvancedEnumeratorParam<long> k);
        IOrderedDictionaryEnumerator<long, ARow> ListByIdDict(ulong t, AdvancedEnumeratorParam<long> k);
        ICollection<ARow> ListByIdCollection(ulong t, AdvancedEnumeratorParam<long> k);
        IReadOnlyCollection<ARow> ListByIdReadOnly(ulong t, AdvancedEnumeratorParam<long> k);
        IEnumerable<ARow> ListById(AdvancedEnumeratorParam<ulong> t);
        IEnumerable<ARow> ListById(ulong t, long k, AdvancedEnumeratorParam<ulong> v);
        long CountById(ulong t, AdvancedEnumeratorParam<long> k);
        int CountById(AdvancedEnumeratorParam<ulong> t);
        bool AnyById(ulong t, AdvancedEnumeratorParam<long> k);
        bool AnyById(AdvancedEnumeratorParam<ulong> t);
        long CountById(ulong t);
        bool AnyById(ulong t, long k);
        int RemoveById(ulong t, AdvancedEnumeratorParam<long> k);
        IEnumerable<ARow> ListByName(AdvancedEnumeratorParam<string> name);
        IOrderedDictionaryEnumerator<string, ARow> ListByNameDict(AdvancedEnumeratorParam<string> name);
        IEnumerable<ARow> ListByName(string name, AdvancedEnumeratorParam<ulong> t);
        long CountByName(AdvancedEnumeratorParam<string> name);
        uint CountByName(string name, AdvancedEnumeratorParam<ulong> t);
        bool AnyByName(AdvancedEnumeratorParam<string> name);
        bool AnyByName(string name, AdvancedEnumeratorParam<ulong> t);
        long CountByName(string name);
        bool AnyByName(string name);
    }

    static readonly string[] Names = ["", "a", "ab", "b", "ž"];

    List<ARow> Fill(int seed)
    {
        var rnd = new Random(seed);
        var rows = new Dictionary<(ulong, long, ulong), ARow>();
        for (var i = 0; i < 160; i++)
        {
            var row = new ARow
            {
                T = (ulong)rnd.Next(1, 4),
                K = rnd.Next(-6, 7) * 3,
                V = (ulong)rnd.Next(0, 3),
                Name = Names[rnd.Next(Names.Length)]
            };
            rows.TryAdd((row.T, row.K, row.V), row);
        }

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IARowTable>();
        foreach (var row in rows.Values) table.Insert(row);
        tr.Commit();
        return rows.Values.ToList();
    }

    static IEnumerable<AdvancedEnumeratorParam<TKey>> Params<TKey>(TKey[] bounds)
    {
        var propositions = new[] { KeyProposition.Ignored, KeyProposition.Included, KeyProposition.Excluded };
        foreach (var order in new[] { EnumerationOrder.Ascending, EnumerationOrder.Descending })
        foreach (var sp in propositions)
        foreach (var start in sp == KeyProposition.Ignored ? bounds.Take(1) : bounds)
        foreach (var ep in propositions)
        foreach (var end in ep == KeyProposition.Ignored ? bounds.Take(1) : bounds)
            yield return new(order, start, sp, end, ep);
    }

    static bool InRange<TKey>(TKey value, AdvancedEnumeratorParam<TKey> param, IComparer<TKey> comparer)
    {
        if (param.StartProposition == KeyProposition.Included && comparer.Compare(value, param.Start) < 0)
            return false;
        if (param.StartProposition == KeyProposition.Excluded && comparer.Compare(value, param.Start) <= 0)
            return false;
        if (param.EndProposition == KeyProposition.Included && comparer.Compare(value, param.End) > 0) return false;
        if (param.EndProposition == KeyProposition.Excluded && comparer.Compare(value, param.End) >= 0) return false;
        return true;
    }

    static string Describe<TKey>(AdvancedEnumeratorParam<TKey> p) =>
        $"{p.Order} {p.StartProposition}:{p.Start} {p.EndProposition}:{p.End}";

    static string Format(IEnumerable<ARow> rows) => string.Join(",", rows.Select(r => r.ToString()));

    static List<ARow> Ordered(IEnumerable<ARow> rows, EnumerationOrder order)
    {
        var res = rows.OrderBy(r => r.T).ThenBy(r => r.K).ThenBy(r => r.V).ToList();
        if (order == EnumerationOrder.Descending) res.Reverse();
        return res;
    }

    static List<ARow> OrderedByName(IEnumerable<ARow> rows, EnumerationOrder order)
    {
        var res = rows.OrderBy(r => r.Name, StringComparer.Ordinal).ThenBy(r => r.T).ThenBy(r => r.K)
            .ThenBy(r => r.V).ToList();
        if (order == EnumerationOrder.Descending) res.Reverse();
        return res;
    }

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
    [InlineData(true)]
    [InlineData(false)]
    public void PrimaryKeyAdvancedEnumerationMatchesLinq(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rows = Fill(1);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IARowTable>();
            long[] bounds = [long.MinValue, -18, -17, -3, 0, 1, 6, 18, 19, long.MaxValue];
            foreach (var t in new ulong[] { 0, 1, 2, 3, 4 })
            {
                Assert.Equal(rows.Count(r => r.T == t), table.CountById(t));
                foreach (var param in Params(bounds))
                {
                    var name = $"t={t} " + Describe(param);
                    var expected = Ordered(rows.Where(r => r.T == t && InRange(r.K, param, Comparer<long>.Default)),
                        param.Order);
                    Assert.True(Format(expected) == Format(table.ListById(t, param)), "ListById " + name);
                    Assert.True(expected.Count == table.CountById(t, param), "CountById " + name);
                    Assert.True(expected.Count > 0 == table.AnyById(t, param), "AnyById " + name);

                    var collection = table.ListByIdCollection(t, param);
                    Assert.Equal(expected.Count, collection.Count);
                    Assert.True(collection.IsReadOnly);
                    var array = new ARow[expected.Count + 1];
                    collection.CopyTo(array, 1);
                    Assert.True(Format(expected) == Format(array.Skip(1)), "CopyTo " + name);
                    Assert.Equal(expected.Count, table.ListByIdReadOnly(t, param).Count);
                    Assert.True(Format(expected) == Format(table.ListByIdReadOnly(t, param)), "ReadOnly " + name);

                    using var dict = table.ListByIdDict(t, param);
                    Assert.Equal((uint)expected.Count, dict.Count);
                    var keys = new List<long>();
                    var values = new List<ARow>();
                    while (dict.NextKey(out var key))
                    {
                        keys.Add(key);
                        values.Add(dict.CurrentValue);
                    }

                    Assert.False(dict.NextKey(out _));
                    Assert.Equal(expected.Select(r => r.K), keys);
                    Assert.True(Format(expected) == Format(values), "Dict " + name);
                    if (expected.Count > 1)
                    {
                        dict.Position = 1;
                        Assert.Equal(1u, dict.Position);
                        Assert.True(dict.NextKey(out var keyAt1));
                        Assert.Equal(expected[1].K, keyAt1);
                        Assert.Equal(expected[1].ToString(), dict.CurrentValue.ToString());
                    }
                }
            }

            ulong[] tBounds = [0, 1, 2, 3, 4];
            foreach (var param in Params(tBounds))
            {
                var expected = Ordered(rows.Where(r => InRange(r.T, param, Comparer<ulong>.Default)), param.Order);
                Assert.True(Format(expected) == Format(table.ListById(param)), "ListById(t) " + Describe(param));
                Assert.Equal(expected.Count, table.CountById(param));
                Assert.Equal(expected.Count > 0, table.AnyById(param));
            }

            foreach (var row in rows.Where((_, i) => i % 7 == 0))
            {
                Assert.True(table.AnyById(row.T, row.K));
                foreach (var param in Params<ulong>([0, 1, 2, 3]))
                {
                    var expected = Ordered(rows.Where(r =>
                        r.T == row.T && r.K == row.K && InRange(r.V, param, Comparer<ulong>.Default)), param.Order);
                    Assert.True(Format(expected) == Format(table.ListById(row.T, row.K, param)),
                        $"ListById({row.T},{row.K}) " + Describe(param));
                }
            }

            Assert.False(table.AnyById(1, 1));
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RemoveByIdWithAdvancedParamMatchesLinq(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rows = Fill(2);
            long[] bounds = [long.MinValue, -18, -9, 0, 7, 18, long.MaxValue];
            foreach (var param in Params(bounds).Where((_, i) => i % 3 == 0))
            {
                const ulong t = 2;
                var expectedRemoved = rows.Count(r => r.T == t && InRange(r.K, param, Comparer<long>.Default));
                using var tr = _db.StartTransaction();
                var table = tr.GetRelation<IARowTable>();
                var removed = table.RemoveById(t, param);
                Assert.True(expectedRemoved == removed,
                    $"RemoveById {Describe(param)} expected {expectedRemoved} removed {removed}");
                Assert.Equal(rows.Count - expectedRemoved, table.Count);
                var expectedRest = Ordered(rows.Where(r => !(r.T == t && InRange(r.K, param,
                    Comparer<long>.Default))), EnumerationOrder.Ascending);
                Assert.True(Format(expectedRest) == Format(table), "Remove " + Describe(param));
                // secondary index must be cleaned too
                var expectedByName = OrderedByName(expectedRest, EnumerationOrder.Ascending);
                Assert.True(Format(expectedByName) ==
                            Format(table.ListByName(new AdvancedEnumeratorParam<string>())),
                    "Remove SK " + Describe(param));
                // transaction is not committed, so data are restored for next iteration
            }
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SecondaryKeyAdvancedEnumerationMatchesLinq(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rows = Fill(3);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IARowTable>();
            string[] bounds = ["", "a", "aa", "ab", "b", "c", "ž", "ſ"];
            foreach (var param in Params(bounds))
            {
                var name = Describe(param);
                var expected = OrderedByName(rows.Where(r => InRange(r.Name, param, StringComparer.Ordinal)),
                    param.Order);
                Assert.True(Format(expected) == Format(table.ListByName(param)), "ListByName " + name);
                Assert.True(expected.Count == table.CountByName(param), "CountByName " + name);
                Assert.True(expected.Count > 0 == table.AnyByName(param), "AnyByName " + name);
                using var dict = table.ListByNameDict(param);
                var keys = new List<string>();
                while (dict.NextKey(out var key)) keys.Add(key);
                Assert.Equal(expected.Select(r => r.Name), keys);
            }

            foreach (var n in Names.Append("missing"))
            {
                Assert.Equal(rows.Count(r => r.Name == n), table.CountByName(n));
                Assert.Equal(rows.Any(r => r.Name == n), table.AnyByName(n));
                foreach (var param in Params<ulong>([0, 1, 2, 3, 4]))
                {
                    var expected = OrderedByName(
                        rows.Where(r => r.Name == n && InRange(r.T, param, Comparer<ulong>.Default)), param.Order);
                    var desc = $"{n} " + Describe(param);
                    Assert.True(Format(expected) == Format(table.ListByName(n, param)), "ListByName " + desc);
                    Assert.True((uint)expected.Count == table.CountByName(n, param), "CountByName " + desc);
                    Assert.True(expected.Count > 0 == table.AnyByName(n, param), "AnyByName " + desc);
                }
            }
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AdvancedEnumeratorCollectionContract(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var rows = Fill(4);
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IARowTable>();
            var collection = table.ListByIdCollection(1, new AdvancedEnumeratorParam<long>());
            var expected = Ordered(rows.Where(r => r.T == 1), EnumerationOrder.Ascending);
            Assert.Throws<NotSupportedException>(() => collection.Add(new ARow()));
            Assert.Throws<NotSupportedException>(() => collection.Clear());
            Assert.Throws<NotSupportedException>(() => collection.Contains(expected[0]));
            Assert.Throws<NotSupportedException>(() => collection.Remove(expected[0]));
            Assert.Throws<ArgumentException>(() => collection.CopyTo(new ARow[expected.Count - 1], 0));
            using var enumerator = collection.GetEnumerator();
            Assert.Throws<BTDBException>(() => enumerator.Current);
            Assert.True(enumerator.MoveNext());
            Assert.Equal(expected[0].ToString(), enumerator.Current.ToString());
            Assert.True(enumerator.MoveNext());
            enumerator.Reset();
            Assert.True(enumerator.MoveNext());
            Assert.Equal(expected[0].ToString(), enumerator.Current.ToString());

            var empty = table.ListByIdCollection(42, new AdvancedEnumeratorParam<long>());
            Assert.Empty(empty);
            Assert.Equal(0, empty.Count);
            empty.CopyTo([], 0);
        });
    }
}
