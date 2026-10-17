using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace DBBenchmark;

[Generate]
public class QueryBenchRow
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public ulong Id { get; set; }
    public string? Name { get; set; }
    public string? City { get; set; }
    public int Age { get; set; }
    public long Score { get; set; }
    public bool Active { get; set; }
    public DateTime Created { get; set; }
}

public interface IQueryBenchTable : IRelation<QueryBenchRow>
{
    void Insert(QueryBenchRow row);
    IEnumerable<QueryBenchRow> FindById(ulong tenantId);
    IEnumerable<QueryBenchRow> ScanById(Constraint<ulong> tenantId);
}

static class QueryBenchData
{
    public const int TenantCount = 10;
    public const int RowsPerTenant = 10_000;
    public const int RowCount = TenantCount * RowsPerTenant;
    public const ulong SelectedTenant = 3;
    public const int SelectiveAge = 35;
    public const string SelectedCity = "Prague";

    static readonly string[] Cities =
    [
        "Prague", "Brno", "Ostrava", "Plzen", "Liberec", "Olomouc", "Usti", "Hradec", "Pardubice", "Zlin",
        "Havirov", "Kladno", "Most", "Opava", "Frydek", "Karvina", "Jihlava", "Teplice", "Decin", "Chomutov"
    ];

    public static ObjectDB Create(InMemoryFileCollection fileCollection)
    {
        var db = new ObjectDB();
        db.Open(new BTreeKeyValueDB(fileCollection), false);
        var created = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        using var tr = db.StartTransaction();
        var table = tr.GetRelation<IQueryBenchTable>();
        for (var i = 0; i < RowCount; i++)
        {
            table.Insert(new()
            {
                TenantId = (ulong)(i / RowsPerTenant),
                Id = (ulong)i,
                Name = "Name" + i,
                City = Cities[i % Cities.Length],
                // 80 distinct ages make Age == SelectiveAge match 1.25 % of rows.
                Age = 18 + i * 7 % 80,
                Score = i * 31L,
                Active = i % 2 == 0,
                Created = created.AddMinutes(i)
            });
        }

        tr.Commit();
        return db;
    }
}

/// End-to-end relation query scans. Times and allocations are reported per scanned row.
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[Orderer(SummaryOrderPolicy.Declared)]
[ShortRunJob]
public class RelationQueryBenchmark
{
    InMemoryFileCollection _fileCollection = null!;
    ObjectDB _db = null!;
    IObjectDBTransaction _transaction = null!;
    IQueryBenchTable _table = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _fileCollection = new();
        _db = QueryBenchData.Create(_fileCollection);
        _transaction = _db.StartReadOnlyTransaction();
        _table = _transaction.GetRelation<IQueryBenchTable>();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _transaction.Dispose();
        _db.Dispose();
        _fileCollection.Dispose();
    }

    [BenchmarkCategory("ValueSelective"), Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowCount)]
    public int ValueSelectiveQuery() =>
        Count(_table.Query<QueryBenchRow>().Where(p => p.Age == QueryBenchData.SelectiveAge));

    [BenchmarkCategory("ValueSelective"), Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public int ValueSelectiveLinq() => _table.Count(p => p.Age == QueryBenchData.SelectiveAge);

    [BenchmarkCategory("ValueNonSelective"), Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowCount)]
    public int ValueNonSelectiveQuery() => Count(_table.Query<QueryBenchRow>().Where(p => p.Active == true));

    [BenchmarkCategory("ValueNonSelective"), Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public int ValueNonSelectiveLinq() => _table.Count(p => p.Active);

    [BenchmarkCategory("String"), Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowCount)]
    public int StringEqualityQuery() =>
        Count(_table.Query<QueryBenchRow>().Where(p => p.City == QueryBenchData.SelectedCity));

    [BenchmarkCategory("String"), Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public int StringEqualityLinq() => _table.Count(p => p.City == QueryBenchData.SelectedCity);

    [BenchmarkCategory("PkPrefix"), Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowsPerTenant)]
    public int PkPrefixQuery() =>
        Count(_table.Query<QueryBenchRow>().Where(p => p.TenantId == QueryBenchData.SelectedTenant));

    [BenchmarkCategory("PkPrefix"), Benchmark(OperationsPerInvoke = QueryBenchData.RowsPerTenant)]
    public int PkPrefixFindById() => Count(_table.FindById(QueryBenchData.SelectedTenant));

    [BenchmarkCategory("PkPrefix"), Benchmark(OperationsPerInvoke = QueryBenchData.RowsPerTenant)]
    public int PkPrefixScanById() =>
        Count(_table.ScanById(Constraint.Unsigned.Exact(QueryBenchData.SelectedTenant)));

    [BenchmarkCategory("PkPrefixAndValue"), Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowsPerTenant)]
    public int PkPrefixAndValueQuery() =>
        Count(_table.Query<QueryBenchRow>()
            .Where(p => p.TenantId == QueryBenchData.SelectedTenant && p.Age == QueryBenchData.SelectiveAge));

    [BenchmarkCategory("PkPrefixAndValue"), Benchmark(OperationsPerInvoke = QueryBenchData.RowsPerTenant)]
    public int PkPrefixAndValueFindById() =>
        _table.FindById(QueryBenchData.SelectedTenant).Count(p => p.Age == QueryBenchData.SelectiveAge);

    // Reads Current for every row so all variants pay for row materialization.
    static int Count(IEnumerable<QueryBenchRow> rows)
    {
        var count = 0;
        using var enumerator = rows.GetEnumerator();
        while (enumerator.MoveNext())
        {
            _ = enumerator.Current;
            count++;
        }

        return count;
    }
}
