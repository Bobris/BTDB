using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Random upserts, UpdateById of an in-key value (alone and with a secondary key field) and removes, compared after
/// every step with a model through the primary key, lookups by id and the secondary index.
public class RelationInKeyValueMutationTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class Row
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [InKeyValue(3)] public int Version { get; set; }
        [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)] public uint Age { get; set; }
        public int Payload { get; set; }
    }

    [BTDB.Generate]
    public interface IRowTable : IRelation<Row>
    {
        bool RemoveById(ulong tenant, ulong id);
        bool UpdateById(ulong tenant, ulong id, int version);
        bool UpdateByIdWithAge(ulong tenant, ulong id, int version, uint age);
        Row? FindByIdOrDefault(ulong tenant, ulong id);
        IEnumerable<Row> FindByAge(ulong tenant, uint age);
        IEnumerable<Row> ListById(ulong tenant, AdvancedEnumeratorParam<ulong> id);
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 120).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void InKeyValueUpdatesKeepIndexesInStep(int seed)
    {
        var random = new Random(seed);
        var model = new Dictionary<(ulong, ulong), (int Version, uint Age, int Payload)>();
        var tr = _db.StartTransaction();
        var log = new List<string>();
        try
        {
            for (var step = 0; step < 120; step++)
            {
                var table = tr.GetRelation<IRowTable>();
                var tenant = (ulong)random.Next(1, 3);
                var id = (ulong)random.Next(1, 6);
                var version = random.Next(10);
                var age = (uint)random.Next(20, 23);
                var exists = model.ContainsKey((tenant, id));
                var op = random.Next(6);
                log.Add($"{step}: op {op} {tenant}/{id} version {version} age {age} exists {exists}");
                switch (op)
                {
                    case 0:
                    case 1:
                    {
                        var payload = random.Next(100);
                        Assert.Equal(!exists,
                            table.Upsert(new Row { Tenant = tenant, Id = id, Version = version, Age = age, Payload = payload }));
                        model[(tenant, id)] = (version, age, payload);
                        break;
                    }
                    case 2:
                        Assert.Equal(exists, table.UpdateById(tenant, id, version));
                        if (exists) model[(tenant, id)] = model[(tenant, id)] with { Version = version };
                        break;
                    case 3:
                        Assert.Equal(exists, table.UpdateByIdWithAge(tenant, id, version, age));
                        if (exists) model[(tenant, id)] = model[(tenant, id)] with { Version = version, Age = age };
                        break;
                    case 4:
                        Assert.Equal(model.Remove((tenant, id)), table.RemoveById(tenant, id));
                        break;
                    default:
                        tr.Commit();
                        tr.Dispose();
                        tr = _db.StartTransaction();
                        break;
                }

                Check(tr.GetRelation<IRowTable>(), model);
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

    static string Describe(Row r) => $"{r.Tenant}/{r.Id} v{r.Version} {r.Age} {r.Payload}";

    static void Check(IRowTable table, Dictionary<(ulong, ulong), (int Version, uint Age, int Payload)> model)
    {
        var rows = model.OrderBy(p => p.Key)
            .Select(p => new Row
            {
                Tenant = p.Key.Item1, Id = p.Key.Item2, Version = p.Value.Version, Age = p.Value.Age,
                Payload = p.Value.Payload
            }).ToList();
        Assert.Equal(rows.Count, table.Count);
        Assert.Equal(rows.Select(Describe), table.Select(Describe));
        foreach (var row in rows)
            Assert.Equal(Describe(row), Describe(table.FindByIdOrDefault(row.Tenant, row.Id)!));
        foreach (var tenant in new ulong[] { 1, 2 })
        {
            for (var age = 20u; age < 23; age++)
                Assert.Equal(rows.Where(r => r.Tenant == tenant && r.Age == age).Select(Describe),
                    table.FindByAge(tenant, age).Select(Describe));
            var range = new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Descending, 2, KeyProposition.Included, 4,
                KeyProposition.Excluded);
            Assert.Equal(rows.Where(r => r.Tenant == tenant && r.Id is >= 2 and < 4).Reverse().Select(Describe),
                table.ListById(tenant, range).Select(Describe));
        }
    }

    /// Relation queries filter and order by the in-key value and the key fields like LINQ to objects.
    [Theory]
    [MemberData(nameof(Seeds))]
    public void QueriesOverInKeyValueMatchLinq(int seed)
    {
        var random = new Random(seed);
        using (var write = _db.StartTransaction())
        {
            var table = write.GetRelation<IRowTable>();
            for (var i = 0; i < 40; i++)
                table.Upsert(new Row
                {
                    Tenant = (ulong)random.Next(1, 4), Id = (ulong)random.Next(1, 9), Version = random.Next(-3, 6),
                    Age = (uint)random.Next(20, 24), Payload = random.Next(10)
                });
            write.Commit();
        }

        using var tr = _db.StartReadOnlyTransaction();
        var rows = tr.GetRelation<IRowTable>();
        var list = rows.ToList();
        var tenant = (ulong)random.Next(0, 4);
        var id = (ulong)random.Next(0, 9);
        var version = random.Next(-4, 7);
        var age = (uint)random.Next(19, 25);
        var cases = new (string, Func<IQueryable<Row>, IQueryable<Row>>)[]
        {
            ("tenant and version", q => q.Where(r => r.Tenant == tenant && r.Version > version)),
            ("tenant, id and version", q => q.Where(r => r.Tenant == tenant && r.Id == id && r.Version <= version)),
            ("version only", q => q.Where(r => r.Version == version)),
            ("tenant and id range", q => q.Where(r => r.Tenant == tenant && r.Id >= id && r.Id < id + 3)),
            ("tenant and age", q => q.Where(r => r.Tenant == tenant && r.Age == age)),
            ("order by version", q => q.Where(r => r.Tenant == tenant).OrderBy(r => r.Version)),
            ("order by id descending", q => q.Where(r => r.Tenant == tenant).OrderByDescending(r => r.Id)),
            ("order by tenant, id, version", q => q.OrderBy(r => r.Tenant).ThenBy(r => r.Id).ThenBy(r => r.Version)),
            ("paged by key", q => q.Where(r => r.Version != version).OrderBy(r => r.Tenant).ThenBy(r => r.Id).Skip(3).Take(5)),
            ("age range", q => q.Where(r => r.Tenant == tenant && r.Age > age && r.Payload < 7)),
        };
        foreach (var (name, query) in cases)
        {
            var expected = query(list.AsQueryable()).Select(Describe).ToList();
            var actual = query(rows.Query<Row>()).AsEnumerable().Select(Describe).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"{name}: expected {string.Join(" | ", expected)}, actual {string.Join(" | ", actual)}");
        }
    }
}
