using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Secondary keys removed in one version and added back in a later one: every version's lookups match the rows, so
/// no entry of an index's earlier life is left over and no change made while it was absent is missing.
public class RelationSecondaryKeyUpgradeTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class RowV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string? Name { get; set; }
        public uint Age { get; set; }
    }

    [BTDB.Generate]
    public class RowV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public string? Name { get; set; }
        [SecondaryKey("Age")] public uint Age { get; set; }
    }

    [BTDB.Generate]
    public class RowV3
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string? Name { get; set; }
        public uint Age { get; set; }
    }

    [BTDB.Generate]
    public interface IRowV1Table : IRelation<RowV1>
    {
        bool RemoveById(ulong id);
        IEnumerable<RowV1> FindByName(string? name);
    }

    [BTDB.Generate]
    public interface IRowV2Table : IRelation<RowV2>
    {
        bool RemoveById(ulong id);
        IEnumerable<RowV2> FindByAge(uint age);
    }

    [BTDB.Generate]
    public interface IRowV3Table : IRelation<RowV3>
    {
        bool RemoveById(ulong id);
        IEnumerable<RowV3> FindByName(string? name);
    }

    static readonly string?[] Names = [null, "a", "b", "c"];

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void RemovedAndRestoredSecondaryKeysMatchRows(int seed)
    {
        var random = new Random(seed);
        var model = new SortedDictionary<ulong, (string? Name, uint Age)>();

        (ulong Id, string? Name, uint Age, bool Remove) Change()
        {
            var id = (ulong)random.Next(1, 12);
            var remove = random.Next(4) == 0;
            var row = (id, Names[random.Next(Names.Length)], (uint)random.Next(20, 24), remove);
            if (remove) model.Remove(id);
            else model[id] = (row.Item2, row.Item3);
            return row;
        }

        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IRowV1Table>("Rows")(tr);
            for (var i = 0; i < 30; i++)
            {
                var (id, name, age, remove) = Change();
                if (remove) table.RemoveById(id);
                else table.Upsert(new RowV1 { Id = id, Name = name, Age = age });
            }

            foreach (var name in Names)
                Assert.Equal(model.Where(p => p.Value.Name == name).Select(p => p.Key), table.FindByName(name).Select(r => r.Id));
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IRowV2Table>("Rows")(tr);
            for (var age = 20u; age < 24; age++)
                Assert.Equal(model.Where(p => p.Value.Age == age).Select(p => p.Key), table.FindByAge(age).Select(r => r.Id));
            // Changes while the Name index is absent.
            for (var i = 0; i < 30; i++)
            {
                var (id, name, age, remove) = Change();
                if (remove) table.RemoveById(id);
                else table.Upsert(new RowV2 { Id = id, Name = name, Age = age });
            }

            for (var age = 20u; age < 24; age++)
                Assert.Equal(model.Where(p => p.Value.Age == age).Select(p => p.Key), table.FindByAge(age).Select(r => r.Id));
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var table = tr.InitRelation<IRowV3Table>("Rows")(tr);
            foreach (var name in Names)
                Assert.Equal(model.Where(p => p.Value.Name == name).Select(p => p.Key), table.FindByName(name).Select(r => r.Id));
            Assert.Equal(model.Select(p => $"{p.Key} {p.Value.Name} {p.Value.Age}"),
                table.Select(r => $"{r.Id} {r.Name} {r.Age}"));
        }
    }
}
