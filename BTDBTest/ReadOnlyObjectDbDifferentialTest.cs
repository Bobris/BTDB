using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.ReadOnly;
using BTDB.ODBLayer;
using Xunit;
using static BTDBTest.RelationMutationDifferentialTest;

namespace BTDBTest;

/// ObjectDB over a ReadOnlyKeyValueDB built with small nodes from random relation content answers lookups,
/// secondary key ranges and relation queries like ObjectDB over the BTreeKeyValueDB it was built from.
public class ReadOnlyObjectDbDifferentialTest
{
    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 80).Select(seed => new object[] { seed });

    static readonly string?[] Names = [null, "", "a", "b", "ab"];

    [Theory]
    [MemberData(nameof(Seeds))]
    public void ReadOnlyCopyAnswersLikeTheSource(int seed)
    {
        var random = new Random(seed);
        using var source = new BTreeKeyValueDB(new InMemoryFileCollection());
        using (var odb = new ObjectDB())
        {
            odb.Open(source, false);
            using var tr = odb.StartTransaction();
            var table = tr.GetRelation<IRowTable>();
            for (var i = random.Next(0, 300); i > 0; i--)
            {
                var row = new Row
                {
                    Tenant = (ulong)random.Next(1, 4), Id = (ulong)random.Next(1, 60), Age = (uint)random.Next(20, 26),
                    Name = Names[random.Next(Names.Length)], Payload = random.Next(100)
                };
                if (random.Next(5) == 0) table.RemoveById(row.Tenant, row.Id);
                else table.Upsert(row);
            }

            tr.Commit();
        }

        byte[] bytes;
        using (var tr = source.StartReadOnlyTransaction())
        {
            var creator = new ReadOnlyCreator(tr);
            creator.SetMaxChildren((uint)random.Next(2, 9));
            bytes = creator.WriteToArray();
        }

        using var copy = ReadOnlyKeyValueDB.Create(bytes);
        using var expectedOdb = new ObjectDB();
        expectedOdb.Open(source, false);
        using var actualOdb = new ObjectDB();
        actualOdb.Open(copy, false);
        using var e = expectedOdb.StartReadOnlyTransaction();
        using var a = actualOdb.StartReadOnlyTransaction();
        var expected = e.GetRelation<IRowTable>();
        var actual = a.GetRelation<IRowTable>();

        void Same(string what, Func<IRowTable, IEnumerable<string>> query) =>
            Assert.True(query(expected).SequenceEqual(query(actual)), $"Seed {seed}: {what}");

        static string D(Row r) => $"{r.Tenant}/{r.Id} {r.Age} {r.Name ?? "null"} {r.Payload}";
        Assert.Equal(expected.Count, actual.Count);
        Same("all", t => t.Select(D));
        Same("by id", t => Enumerable.Range(0, 62).SelectMany(i => new[] { 1ul, 2ul, 3ul }
            .Select(tenant => t.FindByIdOrDefault(tenant, (ulong)i) is { } r ? D(r) : "-")));
        foreach (var name in Names) Same($"name {name}", t => t.FindByName(name).Select(D));
        foreach (var tenant in new ulong[] { 0, 1, 2, 3 })
        {
            for (var age = 19u; age < 27; age++) Same($"age {tenant} {age}", t => t.FindByAge(tenant, age).Select(D));
            foreach (var order in new[] { EnumerationOrder.Ascending, EnumerationOrder.Descending })
            foreach (var start in new[] { KeyProposition.Ignored, KeyProposition.Included, KeyProposition.Excluded })
            foreach (var end in new[] { KeyProposition.Ignored, KeyProposition.Included, KeyProposition.Excluded })
            {
                var range = new AdvancedEnumeratorParam<uint>(order, (uint)random.Next(19, 27), start,
                    (uint)random.Next(19, 27), end);
                Same($"range {tenant} {order} {start} {end}", t => t.ListByAge(tenant, range).Select(D));
                Same($"count {tenant} {order} {start} {end}", t => [t.CountByAge(tenant, range).ToString()]);
            }
        }

        var bound = random.Next(20, 26);
        Same("query age", t => t.Query<Row>().Where(r => r.Age > bound && r.Tenant == 2).Select(D));
        Same("query order", t => t.Query<Row>().Where(r => r.Name != null).OrderByDescending(r => r.Payload)
            .ThenBy(r => r.Id).Select(D));
        Same("query paged", t => t.Query<Row>().OrderBy(r => r.Tenant).ThenBy(r => r.Id).Skip(5).Take(7).Select(D));
        Same("query starts", t => t.Query<Row>().Where(r => r.Name != null && r.Name.StartsWith("a")).Select(D));
    }
}
