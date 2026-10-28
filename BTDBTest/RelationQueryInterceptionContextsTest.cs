using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest.RelationQueryInterceptionContexts;

[Generate]
public class ContextItem
{
    [PrimaryKey(1)] public ulong Id { get; set; }
    public int Age { get; set; }
    public string? Name { get; set; }
}

public interface IContextItemTable : IRelation<ContextItem>
{
    void Insert(ContextItem item);
}

public struct ContextBounds
{
    public int Min;
    public int Max { get; set; }
}

/// Queries the generated interceptors replace at compile time, written where call sites are unusual: in a generic method
/// called with several type arguments, an iterator, an async method after await, a local function, a lambda of another
/// query, and capturing instance and static fields, struct members and loop variables; they return what LINQ returns.
public class RelationQueryInterceptionContextsTest : ObjectDbTestBase
{
    int _field = 30;
    static int _staticField = 20;
    public RelationQueryInterceptionContextsTest(ITestOutputHelper output) : base(output) { }

    void Fill()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IContextItemTable>();
        for (var i = 0; i < 60; i++) t.Insert(new() { Id = (ulong)i, Age = i % 50, Name = i % 3 == 0 ? null : "n" + i % 7 });
        tr.Commit();
    }

    static List<ulong> Generic<T>(IContextItemTable table, T marker, int age) where T : notnull =>
        table.Query<ContextItem>().Where(x => x.Age > age && marker.ToString() != "").Select(x => x.Id).ToList();

    static IEnumerable<ulong> Iterator(IContextItemTable table, int age)
    {
        foreach (var id in table.Query<ContextItem>().Where(x => x.Age < age).Select(x => x.Id).ToList()) yield return id;
    }

    async Task<int> Async(IContextItemTable table, int age)
    {
        await Task.Yield();
        return table.Query<ContextItem>().Count(x => x.Age == age || x.Age == _field);
    }

    [Fact]
    public async Task InterceptedQueriesInUnusualContextsMatchLinq()
    {
        _staticField = 20;
        Fill();
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IContextItemTable>();
        var rows = table.ToList();
        var b = new ContextBounds { Min = 10, Max = 25 };

        Assert.Equal(rows.Where(x => x.Age > 40).Select(x => x.Id), Generic(table, 1, 40));
        Assert.Equal(rows.Where(x => x.Age > 30).Select(x => x.Id), Generic(table, "s", 30));
        Assert.Equal(rows.Where(x => x.Age > 20).Select(x => x.Id), Generic(table, 2L, 20));
        Assert.Equal(rows.Where(x => x.Age > 45).Select(x => x.Id), Generic(table, "t", 45));
        Assert.Equal(rows.Where(x => x.Age > 10).Select(x => x.Id), Generic(table, 3, 10));
        Assert.Equal(rows.Where(x => x.Age < 5).Select(x => x.Id), Iterator(table, 5).ToList());
        Assert.Equal(rows.Count(x => x.Age == 7 || x.Age == _field), await Async(table, 7));

        int Local(int age) => table.Query<ContextItem>().Count(x => x.Age >= age && x.Age < _staticField);
        Assert.Equal(rows.Count(x => x.Age >= 3 && x.Age < _staticField), Local(3));

        Assert.Equal(rows.Count(x => x.Age >= b.Min && x.Age <= b.Max),
            table.Query<ContextItem>().Count(x => x.Age >= b.Min && x.Age <= b.Max));

        var ages = new[] { 1, 2, 3 };
        Assert.Equal(ages.Select(a => rows.Count(x => x.Age == a)).ToList(),
            ages.Select(a => table.Query<ContextItem>().Count(x => x.Age == a)).ToList());

        _field = 11;
        Assert.Equal(rows.Count(x => x.Age == _field), table.Query<ContextItem>().Count(x => x.Age == _field));
        _staticField = 9;
        Assert.Equal(rows.Count(x => x.Age < _staticField), table.Query<ContextItem>().Count(x => x.Age < _staticField));

        string? name = null;
        Assert.Equal(rows.Count(x => x.Name == name), table.Query<ContextItem>().Count(x => x.Name == name));
        name = "n1";
        Assert.Equal(rows.Count(x => x.Name == name), table.Query<ContextItem>().Count(x => x.Name == name));

        for (var i = 0; i < 3; i++)
        {
            var captured = i * 10;
            Assert.Equal(rows.Where(x => x.Age > captured).OrderBy(x => x.Name).ThenBy(x => x.Id).Select(x => x.Id).ToList(),
                table.Query<ContextItem>().Where(x => x.Age > captured).OrderBy(x => x.Name).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        }
    }
}
