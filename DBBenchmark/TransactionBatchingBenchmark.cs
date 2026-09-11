using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace DBBenchmark;

/// <summary>Identical updates, grouped in application transactions, BTree batches, or individual transactions.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class TransactionBatchingBenchmark
{
    const int UpdateCount = 500_000;
    const int KeyCount = 100_000;
    InMemoryFileCollection _files = null!;
    BTreeKeyValueDB _db = null!;
    byte[][] _keys = null!;
    readonly byte[] _value = new byte[32];

    [Params(100, 1000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void PrepareKeys()
    {
        _keys = new byte[KeyCount][];
        for (var i = 0; i < KeyCount; i++)
        {
            _keys[i] = new byte[4];
            PackUnpack.PackInt32BE(_keys[i], 0, i);
        }
    }

    [IterationSetup]
    public void Setup()
    {
        _files = new InMemoryFileCollection();
        _db = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = _files, CompactorScheduler = null, Compression = new NoCompressionStrategy()
        }) { DurableTransactions = false };
        using var transaction = _db.StartWritingTransaction().GetAwaiter().GetResult();
        using var cursor = transaction.CreateCursor();
        foreach (var key in _keys) cursor.CreateOrUpdateKeyValue(key, _value);
        transaction.Commit();
    }

    [IterationCleanup]
    public void Cleanup()
    {
        using (var reader = _db.StartReadOnlyTransaction())
        {
            if (reader.GetKeyValueCount() != KeyCount || reader.GetCommitUlong() != UpdateCount)
                throw new InvalidOperationException("Benchmark produced an unexpected database state");
        }
        _db.Dispose();
        _files.Dispose();
    }

    void Update(IKeyValueDBTransaction transaction, int index)
    {
        using var cursor = transaction.CreateCursor();
        // A deterministic permutation touches different BTree paths in all three variants.
        cursor.CreateOrUpdateKeyValue(_keys[(index % KeyCount) * 7919 % KeyCount], _value);
        transaction.SetCommitUlong((ulong)(index + 1));
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = UpdateCount)]
    public void WithoutBatching()
    {
        for (var i = 0; i < UpdateCount; i++)
        {
            using var transaction = _db.StartWritingTransaction().GetAwaiter().GetResult();
            Update(transaction, i);
            transaction.Commit();
        }
    }

    [Benchmark(OperationsPerInvoke = UpdateCount)]
    public void BTreeBatching()
    {
        for (var start = 0; start < UpdateCount; start += BatchSize)
        {
            for (var i = start; i < start + BatchSize; i++)
            {
                using var transaction = _db.StartWritingTransaction(inBatch: true).GetAwaiter().GetResult();
                Update(transaction, i);
                transaction.Commit();
            }
            _db.FinishTransactionBatchAfterCurrentTransaction();
        }
    }

    [Benchmark(OperationsPerInvoke = UpdateCount)]
    public void ApplicationBatching()
    {
        for (var start = 0; start < UpdateCount; start += BatchSize)
        {
            using var transaction = _db.StartWritingTransaction().GetAwaiter().GetResult();
            for (var i = start; i < start + BatchSize; i++) Update(transaction, i);
            transaction.Commit();
        }
    }
}
