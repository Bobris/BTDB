using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Random inserts, upserts, updates and removes that change secondary key fields, compared after every step with a
/// model through the primary and both secondary indexes, so every index stays in step with the rows.
public class RelationMutationDifferentialTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class Row
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)] public uint Age { get; set; }
        [SecondaryKey("Name")] public string? Name { get; set; }
        public int Payload { get; set; }
    }

    [BTDB.Generate]
    public interface IRowTable : IRelation<Row>
    {
        void Insert(Row row);
        void Update(Row row);
        void ShallowUpdate(Row row);
        bool ShallowUpsert(Row row);
        bool RemoveById(ulong tenant, ulong id);
        int RemoveById(ulong tenant);
        int RemoveById(ulong tenant, AdvancedEnumeratorParam<ulong> id);
        Row? FindByIdOrDefault(ulong tenant, ulong id);
        IEnumerable<Row> FindByAge(ulong tenant, uint age);
        IEnumerable<Row> FindByName(string? name);
        IEnumerable<Row> ListByAge(ulong tenant, AdvancedEnumeratorParam<uint> age);
        int CountByAge(ulong tenant, AdvancedEnumeratorParam<uint> age);
    }

    static readonly string?[] Names = [null, "", "a", "b", "ab"];

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 150).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void MutationsKeepIndexesInStep(int seed)
    {
        var random = new Random(seed);
        var model = new Dictionary<(ulong, ulong), (uint Age, string? Name, int Payload)>();
        var tr = _db.StartTransaction();
        var table = tr.GetRelation<IRowTable>();
        var log = new List<string>();
        try
        {
            for (var step = 0; step < 150; step++)
            {
                if (random.Next(15) == 0)
                {
                    // Committed rows are read back from storage, on LsmKeyValueDB from flushed tables too.
                    tr.Commit();
                    tr.Dispose();
                    tr = _db.StartTransaction();
                    table = tr.GetRelation<IRowTable>();
                }

                var tenant = (ulong)random.Next(1, 3);
                var id = (ulong)random.Next(1, 8);
                var row = new Row
                {
                    Tenant = tenant, Id = id, Age = (uint)random.Next(20, 24), Name = Names[random.Next(Names.Length)],
                    Payload = random.Next(100)
                };
                var exists = model.ContainsKey((tenant, id));
                var op = random.Next(10);
                log.Add($"{step}: op {op} {tenant}/{id} age {row.Age} name {row.Name ?? "null"} exists {exists}");
                switch (op)
                {
                    case 0:
                        if (exists) Assert.Throws<BTDB.KVDBLayer.BTDBException>(() => table.Insert(row));
                        else table.Insert(row);
                        break;
                    case 1:
                        Assert.Equal(!exists, table.Upsert(row));
                        break;
                    case 2:
                        if (exists) table.Update(row);
                        else Assert.Throws<BTDB.KVDBLayer.BTDBException>(() => table.Update(row));
                        break;
                    case 3:
                        if (exists) table.ShallowUpdate(row);
                        else Assert.Throws<BTDB.KVDBLayer.BTDBException>(() => table.ShallowUpdate(row));
                        break;
                    case 4:
                        Assert.Equal(!exists, table.ShallowUpsert(row));
                        break;
                    case 5:
                        Assert.Equal(exists, table.RemoveById(tenant, id));
                        model.Remove((tenant, id));
                        break;
                    case 6:
                        if (random.Next(4) == 0)
                        {
                            var removed = model.Keys.Where(k => k.Item1 == tenant).ToList();
                            Assert.Equal(removed.Count, table.RemoveById(tenant));
                            foreach (var k in removed) model.Remove(k);
                        }

                        break;
                    case 8:
                    {
                        var batch = Enumerable.Range(0, random.Next(1, 4)).Select(_ => new Row
                        {
                            Tenant = (ulong)random.Next(1, 3), Id = (ulong)random.Next(1, 8),
                            Age = (uint)random.Next(20, 24), Name = Names[random.Next(Names.Length)],
                            Payload = random.Next(100)
                        }).ToList();
                        table.UpsertRange(batch);
                        foreach (var r in batch) model[(r.Tenant, r.Id)] = (r.Age, r.Name, r.Payload);
                        break;
                    }
                    case 9:
                        if (random.Next(6) == 0)
                        {
                            table.RemoveAll();
                            model.Clear();
                        }

                        break;
                    default:
                    {
                        var order = random.Next(2) == 0 ? EnumerationOrder.Ascending : EnumerationOrder.Descending;
                        var from = (ulong)random.Next(1, 8);
                        var to = (ulong)random.Next(1, 8);
                        var fromProposition = (KeyProposition)random.Next(3);
                        var toProposition = (KeyProposition)random.Next(3);
                        log[^1] += $" remove {order} {fromProposition} {from} {toProposition} {to}";
                        var removed = model.Keys.Where(k => k.Item1 == tenant &&
                            fromProposition switch
                            {
                                KeyProposition.Included => k.Item2 >= from,
                                KeyProposition.Excluded => k.Item2 > from,
                                _ => true
                            } && toProposition switch
                            {
                                KeyProposition.Included => k.Item2 <= to,
                                KeyProposition.Excluded => k.Item2 < to,
                                _ => true
                            }).ToList();
                        Assert.Equal(removed.Count, table.RemoveById(tenant,
                            new AdvancedEnumeratorParam<ulong>(order, from, fromProposition, to, toProposition)));
                        foreach (var k in removed) model.Remove(k);
                        break;
                    }
                }

                // Insert of an existing row and updates of a missing one throw and change nothing.
                if (op is 1 or 4 || op == 0 && !exists || op is 2 or 3 && exists)
                    model[(tenant, id)] = (row.Age, row.Name, row.Payload);
                Check(table, model);
            }
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

    static readonly KeyProposition[] Propositions =
        [KeyProposition.Ignored, KeyProposition.Included, KeyProposition.Excluded];

    static bool InRange(uint age, AdvancedEnumeratorParam<uint> range) =>
        range.StartProposition switch
        {
            KeyProposition.Included => age >= range.Start,
            KeyProposition.Excluded => age > range.Start,
            _ => true
        } && range.EndProposition switch
        {
            KeyProposition.Included => age <= range.End,
            KeyProposition.Excluded => age < range.End,
            _ => true
        };

    static string Describe(Row r) => $"{r.Tenant}/{r.Id} {r.Age} {r.Name ?? "null"} {r.Payload}";

    static void Check(IRowTable table, Dictionary<(ulong, ulong), (uint Age, string? Name, int Payload)> model)
    {
        var rows = model.Select(p => new Row
            { Tenant = p.Key.Item1, Id = p.Key.Item2, Age = p.Value.Age, Name = p.Value.Name, Payload = p.Value.Payload })
            .ToList();
        Assert.Equal(rows.Count, table.Count);
        Assert.Equal(rows.OrderBy(r => r.Tenant).ThenBy(r => r.Id).Select(Describe), table.Select(Describe));
        foreach (var tenant in new ulong[] { 1, 2 })
        {
            for (var age = 20u; age < 24; age++)
                Assert.Equal(rows.Where(r => r.Tenant == tenant && r.Age == age).OrderBy(r => r.Id).Select(Describe),
                    table.FindByAge(tenant, age).Select(Describe));
            foreach (var order in new[] { EnumerationOrder.Ascending, EnumerationOrder.Descending })
            foreach (var startProposition in Propositions)
            foreach (var endProposition in Propositions)
            {
                var start = (uint)(20 + (int)startProposition);
                var end = (uint)(23 - (int)endProposition);
                var range = new AdvancedEnumeratorParam<uint>(order, start, startProposition, end, endProposition);
                var expected = rows.Where(r => r.Tenant == tenant && InRange(r.Age, range))
                    .OrderBy(r => r.Age).ThenBy(r => r.Id).ToList();
                if (order == EnumerationOrder.Descending) expected.Reverse();
                Assert.Equal(expected.Count, table.CountByAge(tenant, range));
                Assert.Equal(expected.Select(Describe), table.ListByAge(tenant, range).Select(Describe));
            }
        }

        foreach (var name in Names)
            Assert.Equal(rows.Where(r => r.Name == name).OrderBy(r => r.Tenant).ThenBy(r => r.Id).Select(Describe),
                table.FindByName(name).Select(Describe));
    }
}
