using System;
using System.Linq;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace AotQuerySmoke;

[Generate]
public class SmokePerson
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public string Email { get; set; } = "";
    public string? Name { get; set; }
    public int Age { get; set; }
    public bool Active { get; set; }
    public DateTime Created { get; set; }
}

public interface ISmokePersonTable : IRelation<SmokePerson>
{
    void Insert(SmokePerson person);
}

static class Program
{
    static int _failures;

    static int Main()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = new ObjectDB();
        db.Open(new BTreeKeyValueDB(fileCollection), false);

        var created = new DateTime(2025, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        using (var tr = db.StartTransaction())
        {
            var table = tr.GetRelation<ISmokePersonTable>();
            for (var i = 0; i < 100; i++)
            {
                table.Insert(new()
                {
                    TenantId = (ulong)(i % 4),
                    Email = $"user{i:D3}@example.com",
                    Name = i % 10 == 0 ? null : $"Name{i % 7}",
                    Age = 20 + i % 30,
                    Active = i % 3 == 0,
                    Created = created.AddDays(i % 5)
                });
            }

            tr.Commit();
        }

        using (var tr = db.StartReadOnlyTransaction())
        {
            var table = tr.GetRelation<ISmokePersonTable>();
            var all = table.ToList();
            var tenant = 2ul;
            string? missingName = null;
            var day = created.AddDays(3);

            Check("all", all.Count, table.Query<SmokePerson>().AsEnumerable().Count());
            Check("pk prefix", all.Count(p => p.TenantId == tenant),
                table.Query<SmokePerson>().Where(p => p.TenantId == tenant).AsEnumerable().Count());
            Check("value field", all.Count(p => p.Age == 25),
                table.Query<SmokePerson>().Where(p => p.Age == 25).AsEnumerable().Count());
            Check("string and bool", all.Count(p => p.Name == "Name3" && p.Active == true),
                table.Query<SmokePerson>().Where(p => p.Name == "Name3" && p.Active == true).AsEnumerable().Count());
            Check("null or date", all.Count(p => p.Name == missingName || p.Created == day),
                table.Query<SmokePerson>().Where(p => p.Name == missingName || p.Created == day).AsEnumerable()
                    .Count());
            Check("chained where", all.Count(p => p.TenantId == 1 && p.Age == 21),
                table.Query<SmokePerson>().Where(p => p.TenantId == 1).Where(p => p.Age == 21).AsEnumerable()
                    .Count());
            Check("materialized row", "user025@example.com",
                table.Query<SmokePerson>().Where(p => p.TenantId == 1 && p.Email == "user025@example.com")
                    .AsEnumerable().Single().Email);
        }

        Console.WriteLine(_failures == 0 ? "AOT query smoke passed." : $"AOT query smoke failed: {_failures}.");
        return _failures == 0 ? 0 : 1;
    }

    static void Check<TValue>(string name, TValue expected, TValue actual)
    {
        if (Equals(expected, actual))
        {
            Console.WriteLine($"ok   {name}: {actual}");
            return;
        }

        _failures++;
        Console.WriteLine($"FAIL {name}: expected {expected}, got {actual}");
    }
}
