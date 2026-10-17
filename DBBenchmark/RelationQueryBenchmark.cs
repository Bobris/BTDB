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
    [SecondaryKey("City")] public string? City { get; set; }
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

    /// Access paths, measured per call: a primary key range, an IN list of tenants and the City index.
    [BenchmarkCategory("PkRange"), Benchmark(Baseline = true)]
    public int PkRangeQuery() =>
        Count(_table.Query<QueryBenchRow>().Where(p => p.TenantId >= 3 && p.TenantId < 5 && p.Age == 35));

    [BenchmarkCategory("PkRange"), Benchmark]
    public int PkRangeLinq() => _table.Count(p => p.TenantId >= 3 && p.TenantId < 5 && p.Age == 35);

    static readonly ulong[] SelectedTenants = [2, 7];

    [BenchmarkCategory("InList"), Benchmark(Baseline = true)]
    public int InListQuery() =>
        _table.Query<QueryBenchRow>().Count(p => SelectedTenants.Contains(p.TenantId) && p.Active);

    [BenchmarkCategory("InList"), Benchmark]
    public int InListLinq() => _table.Count(p => SelectedTenants.Contains(p.TenantId) && p.Active);

    [BenchmarkCategory("IndexEquality"), Benchmark(Baseline = true)]
    public int IndexEqualityQuery() =>
        Count(_table.Query<QueryBenchRow>().Where(p => p.City == QueryBenchData.SelectedCity && p.Age == 35));

    [BenchmarkCategory("IndexEquality"), Benchmark]
    public int IndexEqualityLinq() => _table.Count(p => p.City == QueryBenchData.SelectedCity && p.Age == 35);

    [BenchmarkCategory("IndexCount"), Benchmark(Baseline = true)]
    public int IndexCountQuery() => _table.Query<QueryBenchRow>().Count(p => p.City == QueryBenchData.SelectedCity);

    [BenchmarkCategory("IndexCount"), Benchmark]
    public int IndexCountLinq() => _table.Count(p => p.City == QueryBenchData.SelectedCity);

    /// Without a predicate Count only computes key indexes, so it is measured per call.
    [BenchmarkCategory("CountAll"), Benchmark(Baseline = true)]
    public int CountAllQuery() => _table.Query<QueryBenchRow>().Count();

    [BenchmarkCategory("CountAll"), Benchmark]
    public int CountAllRelation() => _table.Count;

    [BenchmarkCategory("CountTenant"), Benchmark(Baseline = true)]
    public int CountTenantQuery() =>
        _table.Query<QueryBenchRow>().Count(p => p.TenantId == QueryBenchData.SelectedTenant);

    [BenchmarkCategory("CountTenant"), Benchmark]
    public int CountTenantFindById() => _table.FindById(QueryBenchData.SelectedTenant).Count();

    [BenchmarkCategory("CountPredicate"), Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowCount)]
    public int CountPredicateQuery() => _table.Query<QueryBenchRow>().Count(p => p.Active);

    [BenchmarkCategory("CountPredicate"), Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public int CountPredicateLinq() => _table.Count(p => p.Active);

    [BenchmarkCategory("Sum"), Benchmark(Baseline = true, OperationsPerInvoke = QueryBenchData.RowCount)]
    public long SumQuery() => _table.Query<QueryBenchRow>().Sum(p => p.Score);

    [BenchmarkCategory("Sum"), Benchmark(OperationsPerInvoke = QueryBenchData.RowCount)]
    public long SumLinq() => _table.Sum(p => p.Score);

    /// Fixed cost of one query: expression parsing, planning, program building and the first seek.
    [BenchmarkCategory("EmptyResult"), Benchmark(Baseline = true)]
    public int EmptyResultQuery() => Count(_table.Query<QueryBenchRow>().Where(p => p.TenantId == MissingTenant));

    [BenchmarkCategory("EmptyResult"), Benchmark]
    public int EmptyResultFindById() => Count(_table.FindById(MissingTenant));

    const ulong MissingTenant = QueryBenchData.TenantCount + 1;

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
