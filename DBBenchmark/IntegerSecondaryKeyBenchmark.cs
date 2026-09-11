using System;
using BenchmarkDotNet.Attributes;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace DBBenchmark;

[Generate]
public class IntegerSecondaryRecord
{
    [PrimaryKey(1)] public ulong BatchId { get; set; }
    [PrimaryKey(2)] public ulong Id { get; set; }
    [SecondaryKey("Signed")] public long Signed { get; set; }
    [SecondaryKey("Unsigned")] public ulong Unsigned { get; set; }
}

public interface IIntegerSecondaryTable : IRelation<IntegerSecondaryRecord>
{
    void Insert(IntegerSecondaryRecord value);
    int RemoveById(ulong batchId);
}

[MemoryDiagnoser]
[ShortRunJob]
[IterationCount(10)]
[InvocationCount(16)]
public class IntegerSecondaryKeyBenchmark
{
    [Params(false, true)] public bool WideValues { get; set; }
    InMemoryFileCollection _files = null!;
    ObjectDB _db = null!;

    [IterationSetup]
    public void Setup()
    {
        _files = new InMemoryFileCollection();
        _db = new ObjectDB();
        _db.Open(new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = _files, CompactorScheduler = null, Compression = new NoCompressionStrategy()
        }) { DurableTransactions = false }, true);
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IIntegerSecondaryTable>();
        for (var i = 0; i < 1000; i++)
            table.Insert(new IntegerSecondaryRecord
            {
                BatchId = 1, Id = (ulong)i,
                Signed = WideValues ? long.MinValue + i : i % 127 - 63,
                Unsigned = WideValues ? ulong.MaxValue - (ulong)i : (ulong)i % 128
            });
        tr.Commit();
    }

    [Benchmark]
    public int RemoveBatch()
    {
        using var tr = _db.StartTransaction();
        var count = tr.GetRelation<IIntegerSecondaryTable>().RemoveById(1);
        if (count != 1000) throw new InvalidOperationException("Expected a populated batch");
        return count;
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _db.Dispose();
        _files.Dispose();
    }
}
