using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Read-only transactions opened between random relation commits keep seeing the rows and the secondary index of
/// the moment they started, however many commits follow.
public class RelationSnapshotIsolationTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class Row
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Group")] public uint Group { get; set; }
        public string? Text { get; set; }
    }

    [BTDB.Generate]
    public interface IRowTable : IRelation<Row>
    {
        bool RemoveById(ulong id);
        IEnumerable<Row> FindByGroup(uint group);
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void SnapshotsKeepTheirState(int seed)
    {
        var random = new Random(seed);
        var model = new SortedDictionary<ulong, (uint Group, string Text)>();
        var readers = new List<(IObjectDBTransaction Transaction, SortedDictionary<ulong, (uint, string)> State)>();
        try
        {
            for (var commit = 0; commit < 25; commit++)
            {
                using (var tr = _db.StartTransaction())
                {
                    var table = tr.GetRelation<IRowTable>();
                    for (var i = random.Next(1, 6); i > 0; i--)
                    {
                        var id = (ulong)random.Next(1, 15);
                        if (random.Next(4) == 0)
                        {
                            Assert.Equal(model.Remove(id), table.RemoveById(id));
                        }
                        else
                        {
                            var row = new Row { Id = id, Group = (uint)random.Next(3), Text = "t" + random.Next(100) };
                            table.Upsert(row);
                            model[id] = (row.Group, row.Text!);
                        }
                    }

                    tr.Commit();
                }

                if (random.Next(3) == 0) readers.Add((_db.StartReadOnlyTransaction(), new(model)));
                foreach (var (transaction, state) in readers) Check(transaction, state);
            }
        }
        finally
        {
            foreach (var (transaction, _) in readers) transaction.Dispose();
        }
    }

    static void Check(IObjectDBTransaction transaction, SortedDictionary<ulong, (uint Group, string Text)> state)
    {
        var table = transaction.GetRelation<IRowTable>();
        Assert.Equal(state.Select(p => $"{p.Key} {p.Value.Group} {p.Value.Text}"),
            table.Select(r => $"{r.Id} {r.Group} {r.Text}"));
        for (var group = 0u; group < 3; group++)
            Assert.Equal(state.Where(p => p.Value.Group == group).Select(p => p.Key),
                table.FindByGroup(group).Select(r => r.Id));
    }
}

public class InMemoryRelationSnapshotIsolationTest(ITestOutputHelper output) : RelationSnapshotIsolationTest(output)
{
    protected override BTDB.KVDBLayer.IKeyValueDB CreateKeyValueDB() => new BTDB.KVDBLayer.InMemoryKeyValueDB();
}
