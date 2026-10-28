using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
/// Random upserts, inserts, removes, partial updates of secondary key fields and prefix removals in transactions that
/// commit or roll back, checked against a model by lookups, both secondary keys, counts, and no leaked keys.
public class RelationPartialUpdateDifferentialTest : ObjectDbTestBase
{
    public RelationPartialUpdateDifferentialTest(ITestOutputHelper output) : base(output) { }

    public class MRow
    {
        [PrimaryKey(1)] public uint Tenant { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Name", IncludePrimaryKeyOrder = 1)] public string Name { get; set; } = "";
        [SecondaryKey("Age")] [SecondaryKey("Name", Order = 2)] public int Age { get; set; }
        public string? Note { get; set; }
        public IDictionary<int, int>? Map { get; set; }
    }

    public interface IMRowTable : IRelation<MRow>
    {
        MRow? FindByIdOrDefault(uint tenant, ulong id);
        bool RemoveById(uint tenant, ulong id);
        int RemoveById(uint tenant);
        bool UpdateById(uint tenant, ulong id, int age);
        bool UpdateById(uint tenant, ulong id, string name, string? note);
        IEnumerable<MRow> ListByName(uint tenant, string name);
        IEnumerable<MRow> ListByAge(int age);
        int CountById(uint tenant);
        bool Insert(MRow row);
    }

    record M(uint Tenant, ulong Id, string Name, int Age, string? Note, int MapCount);

    [Theory]
    [InlineData(true, 1)] [InlineData(true, 2)] [InlineData(false, 3)] [InlineData(false, 4)]
    public void RandomMutationsMatchModel(bool noEmit, int seed)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(noEmit, () =>
        {
            var r = new Random(seed);
            var model = new Dictionary<(uint, ulong), M>();
            for (var round = 0; round < 40; round++)
            {
                var tr = _db.StartTransaction();
                var t = tr.GetRelation<IMRowTable>();
                var local = new Dictionary<(uint, ulong), M>(model);
                for (var op = 0; op < 30; op++)
                {
                    var tenant = (uint)r.Next(1, 4);
                    var id = (ulong)r.Next(0, 12);
                    var name = "n" + r.Next(4);
                    var age = r.Next(5);
                    var ctx = $"seed {seed} round {round} op {op}";
                    switch (r.Next(7))
                    {
                        case 0:
                        {
                            var mapCount = r.Next(3);
                            t.Upsert(new MRow { Tenant = tenant, Id = id, Name = name, Age = age, Note = "u", Map = Enumerable.Range(0, mapCount).ToDictionary(i => i, i => i) });
                            local[(tenant, id)] = new(tenant, id, name, age, "u", mapCount);
                            break;
                        }
                        case 1:
                            Assert.True(local.Remove((tenant, id)) == t.RemoveById(tenant, id), ctx + " remove");
                            break;
                        case 2:
                        {
                            var had = local.TryGetValue((tenant, id), out var m);
                            Assert.True(had == t.UpdateById(tenant, id, age), ctx + " update age");
                            if (had) local[(tenant, id)] = m! with { Age = age };
                            break;
                        }
                        case 3:
                        {
                            var had = local.TryGetValue((tenant, id), out var m);
                            Assert.True(had == t.UpdateById(tenant, id, name, "upd"), ctx + " update name");
                            if (had) local[(tenant, id)] = m! with { Name = name, Note = "upd" };
                            break;
                        }
                        case 4:
                        {
                            var exists = local.ContainsKey((tenant, id));
                            var inserted = t.Insert(new MRow { Tenant = tenant, Id = id, Name = name, Age = age, Note = "i" });
                            Assert.True(inserted == !exists, ctx + " insert");
                            if (!exists) local[(tenant, id)] = new(tenant, id, name, age, "i", 0);
                            break;
                        }
                        case 5:
                            if (r.Next(4) == 0)
                            {
                                var expected = local.Keys.Count(k => k.Item1 == tenant);
                                Assert.True(expected == t.RemoveById(tenant), ctx + " remove prefix");
                                foreach (var k in local.Keys.Where(k => k.Item1 == tenant).ToList()) local.Remove(k);
                            }
                            break;
                        case 6:
                        {
                            var got = t.ListByName(tenant, name).Select(x => (x.Tenant, x.Id, x.Name, x.Age)).ToList();
                            var exp = local.Values.Where(m => m.Tenant == tenant && m.Name == name).OrderBy(m => m.Age).ThenBy(m => m.Id).Select(m => (m.Tenant, m.Id, m.Name, m.Age)).ToList();
                            Assert.True(exp.SequenceEqual(got), ctx + $" listByName exp {string.Join(";", exp)} got {string.Join(";", got)}");
                            var gotAge = t.ListByAge(age).Select(x => (x.Tenant, x.Id)).OrderBy(x => x).ToList();
                            var expAge = local.Values.Where(m => m.Age == age).Select(m => (m.Tenant, m.Id)).OrderBy(x => x).ToList();
                            Assert.True(expAge.SequenceEqual(gotAge), ctx + " listByAge");
                            Assert.True(local.Keys.Count(k => k.Item1 == tenant) == t.CountById(tenant), ctx + " count");
                            break;
                        }
                    }
                }
                if (r.Next(4) == 0) { tr.Dispose(); }
                else { tr.Commit(); tr.Dispose(); model = local; }
                if (r.Next(5) == 0) ReopenDb();
                using var rt = _db.StartReadOnlyTransaction();
                var rtab = rt.GetRelation<IMRowTable>();
                Assert.Equal(model.Count, (int)rtab.Count);
                foreach (var m in model.Values)
                {
                    var row = rtab.FindByIdOrDefault(m.Tenant, m.Id)!;
                    Assert.True(row.Name == m.Name && row.Age == m.Age && row.Note == m.Note && (row.Map?.Count ?? 0) == m.MapCount, $"seed {seed} round {round} row {m}");
                }
            }
            using var visitor = new FindUnusedKeysVisitor();
            using var vt = _db.StartReadOnlyTransaction();
            visitor.ImportAllKeys(vt);
            visitor.Iterate(vt);
            Assert.Empty(visitor.UnseenKeys());
        });
    }
}
