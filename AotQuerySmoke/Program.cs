using System;
using System.Diagnostics.CodeAnalysis;
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
    [SecondaryKey("Age")] public int Age { get; set; }
    public bool Active { get; set; }
    public DateTime Created { get; set; }
    public SmokeLevel Level { get; set; }
    public int? Bonus { get; set; }
    public decimal Price { get; set; }
}

[Generate]
public class SmokeDto
{
    public string? Email { get; set; }
    public int Age { get; set; }
    public SmokeLevel Level { get; set; }
    public int? Bonus { get; set; }
}

public enum SmokeLevel
{
    Low,
    Middle,
    High
}

public interface ISmokePersonTable : IRelation<SmokePerson>
{
    void Insert(SmokePerson person);
}

static class Program
{
    static int _failures;

    // The C# compiler builds decimal comparisons in expression trees without their MethodInfo, so
    // System.Linq.Expressions looks the operators up by reflection, which trimming must keep.
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(decimal))]
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
                    Created = created.AddDays(i % 5),
                    Level = (SmokeLevel)(i % 3),
                    Bonus = i % 4 == 0 ? null : i % 4,
                    Price = i * 0.5m
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
            var levels = new[] { SmokeLevel.Low, SmokeLevel.High };
            var minimum = 30;
            Check("range", all.Count(p => p.Age >= 25 && p.Age < 31),
                table.Query<SmokePerson>().Where(p => p.Age >= 25 && p.Age < 31).AsEnumerable().Count());
            Check("captured arithmetic", all.Count(p => p.Age + 5 > minimum + 1),
                table.Query<SmokePerson>().Where(p => p.Age + 5 > minimum + 1).AsEnumerable().Count());
            Check("enum", all.Count(p => p.Level == SmokeLevel.Middle),
                table.Query<SmokePerson>().Where(p => p.Level == SmokeLevel.Middle).AsEnumerable().Count());
            Check("enum contains", all.Count(p => levels.Contains(p.Level)),
                table.Query<SmokePerson>().Where(p => levels.Contains(p.Level)).AsEnumerable().Count());
            Check("nullable", all.Count(p => p.Bonus > 1 || p.Bonus == null),
                table.Query<SmokePerson>().Where(p => p.Bonus > 1 || p.Bonus == null).AsEnumerable().Count());
            Check("decimal and not", all.Count(p => !(p.Price < 10m)),
                table.Query<SmokePerson>().Where(p => !(p.Price < 10m)).AsEnumerable().Count());
            Check("string methods", all.Count(p => p.Email.StartsWith("user01", StringComparison.Ordinal) ||
                                                    p.Name != null && p.Name.Contains('3')),
                table.Query<SmokePerson>().Where(p => p.Email.StartsWith("user01", StringComparison.Ordinal) ||
                                                      p.Name != null && p.Name.Contains('3')).AsEnumerable().Count());
            Check("string set", all.Count(p => new[] { "Name1", "Name2", null }.Contains(p.Name)),
                table.Query<SmokePerson>().Where(p => new[] { "Name1", "Name2", null }.Contains(p.Name)).AsEnumerable()
                    .Count());
            var query = table.Query<SmokePerson>();
            Check("count", all.Count(p => p.TenantId == tenant), query.Count(p => p.TenantId == tenant));
            Check("count all", all.Count, query.Count());
            Check("any and all", "True/False", $"{query.Any(p => p.Age > 40)}/{query.All(p => p.Active)}");
            Check("first and last", $"{all.First(p => p.Age == 30).Email}/{all.Last().Email}",
                $"{query.First(p => p.Age == 30).Email}/{query.Last().Email}");
            Check("sum and average", $"{all.Sum(p => p.Price)}/{all.Average(p => p.Bonus)}",
                $"{query.Sum(p => p.Price)}/{query.Average(p => p.Bonus)}");
            Check("min and max", $"{all.Min(p => p.Created)}/{all.Max(p => p.Level)}",
                $"{query.Min(p => p.Created)}/{query.Max(p => p.Level)}");
            var tenants = new[] { 3ul, 0ul };
            Check("key range", all.Count(p => p.TenantId >= 1 && p.TenantId < 3),
                query.Count(p => p.TenantId >= 1 && p.TenantId < 3));
            Check("key in list", all.Count(p => tenants.Contains(p.TenantId) && p.Active),
                query.Count(p => tenants.Contains(p.TenantId) && p.Active));
            Check("key starts with", all.Count(p => p.TenantId == 1 && p.Email.StartsWith("user0", StringComparison.Ordinal)),
                query.Count(p => p.TenantId == 1 && p.Email.StartsWith("user0", StringComparison.Ordinal)));
            Check("index", string.Join(",", all.Where(p => p.Age == 32 && p.Active).Select(p => p.Email)),
                string.Join(",", query.Where(p => p.Age == 32 && p.Active).AsEnumerable().Select(p => p.Email)));
            Check("covering index count", all.Count(p => p.Age > 40), query.Count(p => p.Age > 40));
            Check("order and page", string.Join(",", all.OrderByDescending(p => p.Price).ThenBy(p => p.Name).Skip(3).Take(4).Select(p => p.Email)),
                string.Join(",", query.OrderByDescending(p => p.Price).ThenBy(p => p.Name).Skip(3).Take(4).AsEnumerable().Select(p => p.Email)));
            Check("key order page", string.Join(",", all.Where(p => p.TenantId == 2).OrderBy(p => p.Email).Skip(5).Take(3).Select(p => p.Email)),
                string.Join(",", query.Where(p => p.TenantId == 2).OrderBy(p => p.Email).Skip(5).Take(3).AsEnumerable().Select(p => p.Email)));
            Check("deep skip", all.Skip(90).First().Email, query.Skip(90).First().Email);
            Check("select value", string.Join(",", all.Where(p => p.Age > 45).Select(p => p.Age)),
                string.Join(",", query.Where(p => p.Age > 45).Select(p => p.Age)));
            Check("select string", string.Join(",", all.Where(p => p.Age == 22).Select(p => p.Name)),
                string.Join(",", query.Where(p => p.Age == 22).Select(p => p.Name)));
            Check("select anonymous", string.Join(",", all.Where(p => p.Age == 30).Select(p => new { p.Email, p.Level })),
                string.Join(",", query.Where(p => p.Age == 30).Select(p => new { p.Email, p.Level })));
            Check("select tuple", string.Join(",", all.Where(p => p.Age == 31).Select(p => (p.TenantId, p.Bonus))),
                string.Join(",", query.Where(p => p.Age == 31).Select(p => ValueTuple.Create(p.TenantId, p.Bonus))));
            var dto = query.Where(p => p.Age == 33).Select(p => new SmokeDto { Email = p.Email, Age = p.Age, Level = p.Level, Bonus = p.Bonus }).First();
            var dtoExpected = all.First(p => p.Age == 33);
            Check("select dto", $"{dtoExpected.Email}/{dtoExpected.Age}/{dtoExpected.Level}/{dtoExpected.Bonus}",
                $"{dto.Email}/{dto.Age}/{dto.Level}/{dto.Bonus}");
            Check("select sum and page", $"{all.Select(p => p.Age).Sum()}/{string.Join(",", all.OrderByDescending(p => p.Price).Select(p => p.Email).Take(2))}",
                $"{query.Select(p => p.Age).Sum()}/{string.Join(",", query.OrderByDescending(p => p.Price).Select(p => p.Email).Take(2))}");
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
