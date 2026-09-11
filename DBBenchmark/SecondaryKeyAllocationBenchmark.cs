using System;
using BenchmarkDotNet.Attributes;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace DBBenchmark;

[Generate]
public class SecondaryKeyRecord
{
    [PrimaryKey(1)] public ulong CompanyId { get; set; }
    [PrimaryKey(2)] public ulong BatchId { get; set; }
    [PrimaryKey(3)] public ulong MessageId { get; set; }
    [InKeyValue(4)] public string Recipient { get; set; } = "recipient@example.com";
    [SecondaryKey("CustomField", IncludePrimaryKeyOrder = 1)]
    public string CustomField { get; set; } = "";
}

public interface ISecondaryKeyRecordTable : IRelation<SecondaryKeyRecord>
{
    void Insert(SecondaryKeyRecord record);
    void Update(SecondaryKeyRecord record);
    int RemoveById(ulong companyId, ulong batchId);
}

// One operation is an entire batch. Each invocation rolls back to the same committed data.
// Recreate the database between iterations to bound transaction-log growth.
[MemoryDiagnoser]
[ShortRunJob]
[IterationCount(10)]
[InvocationCount(16)]
public class SecondaryKeyAllocationBenchmark
{
    [Params(100, 1000)] public int RowCount { get; set; }
    [Params(24, 8192)] public int CustomFieldLength { get; set; }
    InMemoryFileCollection _files = null!;
    ObjectDB _db = null!;
    SecondaryKeyRecord[] _updates = null!;

    [IterationSetup]
    public void Setup()
    {
        _files = new InMemoryFileCollection();
        _db = new ObjectDB();
        _db.Open(new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = _files, CompactorScheduler = null, Compression = new NoCompressionStrategy()
        }) { DurableTransactions = false }, true);
        _updates = new SecondaryKeyRecord[RowCount];
        using var transaction = _db.StartTransaction();
        var table = transaction.GetRelation<ISecondaryKeyRecordTable>();
        for (var i = 0; i < RowCount; i++)
        {
            var record = new SecondaryKeyRecord
            {
                CompanyId = 3045805, BatchId = 4625492789, MessageId = (ulong)i,
                CustomField = new string('a', CustomFieldLength - 10) + i.ToString("D10")
            };
            table.Insert(record);
            _updates[i] = new SecondaryKeyRecord
            {
                CompanyId = record.CompanyId, BatchId = record.BatchId, MessageId = record.MessageId,
                CustomField = new string('b', CustomFieldLength - 10) + i.ToString("D10")
            };
        }
        transaction.Commit();
    }

    [Benchmark]
    public int RemoveBatch()
    {
        using var transaction = _db.StartTransaction();
        var removed = transaction.GetRelation<ISecondaryKeyRecordTable>().RemoveById(3045805, 4625492789);
        if (removed != RowCount) throw new InvalidOperationException("Benchmark must delete a populated batch.");
        return removed;
    }

    [Benchmark]
    public void UpdateBatch()
    {
        using var transaction = _db.StartTransaction();
        var table = transaction.GetRelation<ISecondaryKeyRecordTable>();
        foreach (var record in _updates) table.Update(record);
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _db.Dispose();
        _files.Dispose();
    }
}
