using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Random upserts, updates and removes of rows owning nested dictionaries: after every commit the rows hold what a
/// model holds, and no dictionary or object is left in storage that no row references.
public class RelationNestedContentLeakTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class Row
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Version")] public int Version { get; set; }
        public IDictionary<int, int> Map { get; set; } = null!;
        public IDictionary<int, IDictionary<int, int>> Nested { get; set; } = null!;
        public IList<IDictionary<int, int>>? Maps { get; set; }
    }

    [BTDB.Generate]
    public interface IRowTable : IRelation<Row>
    {
        void Update(Row row);
        bool RemoveById(ulong tenant, ulong id);
        int RemoveById(ulong tenant);
        int RemoveById(ulong tenant, AdvancedEnumeratorParam<ulong> id);
        Row? FindByIdOrDefault(ulong tenant, ulong id);
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void NestedContentIsFreedWithItsRow(int seed)
    {
        var random = new Random(seed);
        var model = new Dictionary<(ulong, ulong), string>();
        var tr = _db.StartTransaction();
        var log = new List<string>();
        try
        {
            for (var step = 0; step < 80; step++)
            {
                var table = tr.GetRelation<IRowTable>();
                var tenant = (ulong)random.Next(1, 3);
                var id = (ulong)random.Next(1, 6);
                var op = random.Next(7);
                log.Add($"{step}: op {op} {tenant}/{id}");
                switch (op)
                {
                    case 0:
                    case 1:
                    {
                        var row = NewRow(random, tenant, id);
                        table.Upsert(row);
                        model[(tenant, id)] = Describe(row);
                        break;
                    }
                    case 2:
                    {
                        // Change loaded content in place, then update the row.
                        var row = table.FindByIdOrDefault(tenant, id);
                        if (row == null) break;
                        var key = random.Next(6);
                        if (!row.Map.Remove(key)) row.Map[key] = random.Next(100);
                        if (random.Next(2) == 0) row.Nested.Remove(random.Next(3));
                        else row.Nested[random.Next(3)] = new Dictionary<int, int> { { key, key } };
                        if (random.Next(3) == 0) row.Maps = null;
                        row.Version++;
                        table.Update(row);
                        model[(tenant, id)] = Describe(row);
                        break;
                    }
                    case 3:
                        Assert.Equal(model.Remove((tenant, id)), table.RemoveById(tenant, id));
                        break;
                    case 4:
                        if (random.Next(3) == 0)
                        {
                            var removed = model.Keys.Where(k => k.Item1 == tenant).ToList();
                            Assert.Equal(removed.Count, table.RemoveById(tenant));
                            foreach (var k in removed) model.Remove(k);
                        }

                        break;
                    case 5:
                    {
                        var order = random.Next(2) == 0 ? EnumerationOrder.Ascending : EnumerationOrder.Descending;
                        var removed = model.Keys.Where(k => k.Item1 == tenant && k.Item2 >= id && k.Item2 <= id + 1)
                            .ToList();
                        Assert.Equal(removed.Count, table.RemoveById(tenant, new AdvancedEnumeratorParam<ulong>(order,
                            id, KeyProposition.Included, id + 1, KeyProposition.Included)));
                        foreach (var k in removed) model.Remove(k);
                        break;
                    }
                    default:
                        tr.Commit();
                        tr.Dispose();
                        Verify(model);
                        tr = _db.StartTransaction();
                        break;
                }
            }

            tr.Commit();
            tr.Dispose();
            Verify(model);
            tr = _db.StartTransaction();
        }
        catch (Exception ex)
        {
            throw new Exception($"Seed {seed}:{Environment.NewLine}{string.Join(Environment.NewLine, log.TakeLast(8))}",
                ex);
        }
        finally
        {
            tr.Dispose();
        }
    }

    static Row NewRow(Random random, ulong tenant, ulong id)
    {
        var row = new Row
        {
            Tenant = tenant, Id = id, Version = random.Next(3), Map = new Dictionary<int, int>(),
            Nested = new Dictionary<int, IDictionary<int, int>>()
        };
        for (var i = random.Next(4); i > 0; i--) row.Map[random.Next(6)] = random.Next(100);
        for (var i = random.Next(3); i > 0; i--)
            row.Nested[random.Next(3)] = new Dictionary<int, int> { { random.Next(5), random.Next(5) } };
        if (random.Next(2) == 0)
            row.Maps = Enumerable.Range(0, random.Next(3)).Select(i => (IDictionary<int, int>)new Dictionary<int, int>
                { { i, i } }).ToList();
        return row;
    }

    static string Describe(Row row) =>
        $"{row.Tenant}/{row.Id} v{row.Version} map {string.Join(",", row.Map.OrderBy(p => p.Key).Select(p => $"{p.Key}={p.Value}"))} " +
        $"nested {string.Join(";", row.Nested.OrderBy(p => p.Key).Select(p => $"{p.Key}:{string.Join(",", p.Value.OrderBy(q => q.Key).Select(q => $"{q.Key}={q.Value}"))}"))} " +
        $"maps {(row.Maps == null ? "null" : string.Join(";", row.Maps.Select(m => string.Join(",", m.OrderBy(q => q.Key).Select(q => $"{q.Key}={q.Value}")))))}";

    void Verify(Dictionary<(ulong, ulong), string> model)
    {
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var table = tr.GetRelation<IRowTable>();
            Assert.Equal(model.OrderBy(p => p.Key).Select(p => p.Value), table.Select(Describe));
        }

        using var visitor = new FindUnusedKeysVisitor();
        using var read = _db.StartReadOnlyTransaction();
        visitor.ImportAllKeys(read);
        visitor.Iterate(read);
        Assert.Empty(visitor.UnseenKeys());
    }

    /// A dictionary loaded with one row and stored in another should belong to both, so removing either keeps the
    /// other's. Today the second row stores a reference to the same dictionary, and removing the first row frees it.
    [Fact(Skip = "Known bug: saving a live ObjectDB dictionary into a second row shares it instead of copying it")]
    public void DictionaryOfAnotherRowIsCopied()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IRowTable>();
            table.Upsert(new Row
            {
                Tenant = 1, Id = 1, Map = new Dictionary<int, int> { { 1, 10 } },
                Nested = new Dictionary<int, IDictionary<int, int>> { { 1, new Dictionary<int, int> { { 2, 20 } } } }
            });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IRowTable>();
            var first = table.FindByIdOrDefault(1, 1)!;
            table.Upsert(new Row { Tenant = 1, Id = 2, Map = first.Map, Nested = first.Nested });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IRowTable>();
            Assert.True(table.RemoveById(1, 1));
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            var second = tr.GetRelation<IRowTable>().FindByIdOrDefault(1, 2)!;
            Assert.Equal(10, second.Map[1]);
            Assert.Equal(20, second.Nested[1][2]);
        }

        using var visitor = new FindUnusedKeysVisitor();
        using var read = _db.StartReadOnlyTransaction();
        visitor.ImportAllKeys(read);
        visitor.Iterate(read);
        Assert.Empty(visitor.UnseenKeys());
    }
}
