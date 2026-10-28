using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Random replaces, removes, range removes and clears of dictionary values owning nested dictionaries and lists:
/// after every commit the dictionary holds what a model holds, and nothing is left in storage that it does not
/// reference.
public class ODBDictionaryNestedContentTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class Inner
    {
        public int Version { get; set; }
        public IDictionary<int, int> Map { get; set; } = null!;
        public IList<IDictionary<int, int>>? Maps { get; set; }
    }

    [BTDB.Generate]
    public class Root
    {
        public IOrderedDictionary<int, Inner> Items { get; set; } = null!;
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 80).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void NestedContentIsFreedWithItsValue(int seed)
    {
        var random = new Random(seed);
        var model = new SortedDictionary<int, string>();
        var tr = _db.StartTransaction();
        var log = new List<string>();
        try
        {
            for (var step = 0; step < 80; step++)
            {
                var items = tr.Singleton<Root>().Items;
                var key = random.Next(8);
                var op = random.Next(7);
                log.Add($"{step}: op {op} key {key}");
                switch (op)
                {
                    case 0:
                    case 1:
                    {
                        var inner = NewInner(random);
                        items[key] = inner;
                        model[key] = Describe(inner);
                        break;
                    }
                    case 2:
                    {
                        // Change the loaded value's own dictionary in place, then store the value again.
                        if (!items.TryGetValue(key, out var inner)) break;
                        var k = random.Next(5);
                        if (!inner.Map.Remove(k)) inner.Map[k] = random.Next(100);
                        if (random.Next(3) == 0) inner.Maps = null;
                        inner.Version++;
                        items[key] = inner;
                        model[key] = Describe(inner);
                        break;
                    }
                    case 3:
                        Assert.Equal(model.Remove(key), items.Remove(key));
                        break;
                    case 4:
                    {
                        var removed = model.Keys.Where(k => k >= key && k <= key + 2).ToList();
                        Assert.Equal(removed.Count, items.RemoveRange(key, true, key + 2, true));
                        foreach (var k in removed) model.Remove(k);
                        break;
                    }
                    case 5:
                        if (random.Next(4) == 0)
                        {
                            items.Clear();
                            model.Clear();
                        }

                        break;
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

    static Inner NewInner(Random random)
    {
        var inner = new Inner { Version = random.Next(3), Map = new Dictionary<int, int>() };
        for (var i = random.Next(4); i > 0; i--) inner.Map[random.Next(5)] = random.Next(100);
        if (random.Next(2) == 0)
            inner.Maps = Enumerable.Range(0, random.Next(3))
                .Select(i => (IDictionary<int, int>)new Dictionary<int, int> { { i, i } }).ToList();
        return inner;
    }

    static string Describe(Inner inner) =>
        $"v{inner.Version} map {string.Join(",", inner.Map.OrderBy(p => p.Key).Select(p => $"{p.Key}={p.Value}"))} " +
        $"maps {(inner.Maps == null ? "null" : string.Join(";", inner.Maps.Select(m => string.Join(",", m.OrderBy(q => q.Key).Select(q => $"{q.Key}={q.Value}")))))}";

    void Verify(SortedDictionary<int, string> model)
    {
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var items = tr.Singleton<Root>().Items;
            Assert.Equal(model.Select(p => $"{p.Key}: {p.Value}"), items.Select(p => $"{p.Key}: {Describe(p.Value)}"));
        }

        using var visitor = new FindUnusedKeysVisitor();
        using var read = _db.StartReadOnlyTransaction();
        visitor.ImportAllKeys(read);
        visitor.Iterate(read);
        Assert.Empty(visitor.UnseenKeys());
    }
}
