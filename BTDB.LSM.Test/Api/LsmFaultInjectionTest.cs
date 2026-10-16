using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Storage wrapper that crashes at a chosen write and makes writes ambiguous: applied with a lost response, or not
/// applied with a timeout (ImplementationPlan.md, section 19, item 4).
sealed class FaultyLsmStorage(InMemoryLsmStorage inner, int seed) : ILsmStorage
{
    readonly Random _random = new(seed);
    readonly Lock _lock = new();
    bool _crashed;

    public int Writes { get; private set; }
    public int CrashAtWrite { get; set; } = int.MaxValue;
    public double AmbiguousAfterApply { get; set; }
    public double AmbiguousWithoutApply { get; set; }
    public bool Crashed => _crashed;

    void ThrowIfCrashed()
    {
        if (_crashed) throw new IOException("Storage is unreachable after the simulated crash.");
    }

    /// Counts a write; at the crash point the write applies or not at random and everything fails from then on.
    bool CrashHere()
    {
        lock (_lock)
        {
            ThrowIfCrashed();
            if (++Writes < CrashAtWrite) return false;
            _crashed = true;
            return true;
        }
    }

    double Next()
    {
        lock (_lock) return _random.NextDouble();
    }

    public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        if (CrashHere())
        {
            if (Next() < 0.5) await inner.CreateAsync(key, content, cancellation);
            throw new IOException("Simulated crash during a write.");
        }

        var roll = Next();
        if (roll < AmbiguousWithoutApply) return LsmWriteOutcome.Ambiguous;
        var outcome = await inner.CreateAsync(key, content, cancellation);
        return roll < AmbiguousWithoutApply + AmbiguousAfterApply ? LsmWriteOutcome.Ambiguous : outcome;
    }

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation)
    {
        ThrowIfCrashed();
        return inner.TryReadAsync(key, cancellation);
    }

    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation)
    {
        ThrowIfCrashed();
        return inner.ReadRangeAsync(key, offset, destination, cancellation);
    }

    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        ThrowIfCrashed();
        inner.ReadRange(key, offset, destination);
    }

    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation)
    {
        ThrowIfCrashed();
        return inner.ListAsync(prefix, cancellation);
    }

    public async ValueTask DeleteAsync(string key, CancellationToken cancellation)
    {
        if (CrashHere())
        {
            if (Next() < 0.5) await inner.DeleteAsync(key, cancellation);
            throw new IOException("Simulated crash during a delete.");
        }

        await inner.DeleteAsync(key, cancellation);
    }

    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation)
    {
        ThrowIfCrashed();
        return inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
    }

    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation)
    {
        ThrowIfCrashed();
        return inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
    }

    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation)
    {
        ThrowIfCrashed();
        return inner.ReleaseLeaseAsync(key, leaseId, cancellation);
    }
}

public class LsmFaultInjectionTest
{
    static Exception? LastError;

    static LsmKeyValueDBOptions Options(ILsmStorage storage) => new()
    {
        Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false, MemtableSize = 12 * 1024,
        BlockSize = 512, Level0CompactionTrigger = 2, LevelBaseBytes = 24 * 1024, LevelMultiplier = 3, LevelCount = 4,
        TargetFileSize = 8 * 1024, ValueSeparationThreshold = 200, WalChunkTargetSize = 1500
    };

    static byte[] Key(int i) => [(byte)(i >> 8), (byte)i];

    static SortedDictionary<string, string> Content(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (cursor.FindFirstKey([]))
            do result.Add(Convert.ToHexString(cursor.SlowGetKey()), Convert.ToHexString(cursor.SlowGetValue()));
            while (cursor.FindNextKey([]));
        return result;
    }

    /// Runs random commits until the storage crashes or the commit budget is used; returns the history of states by
    /// CommitUlong and the highest commit acknowledged as durable.
    static (List<SortedDictionary<string, string>> History, int Durable) Workload(LsmKeyValueDB db,
        SortedDictionary<string, string> start, ulong firstCommit, Random random, int commits)
    {
        var history = new List<SortedDictionary<string, string>>();
        var model = new SortedDictionary<string, string>(start, StringComparer.Ordinal);
        var durable = -1;
        try
        {
            for (var c = 0; c < commits; c++)
            {
                using var tr = db.StartTransaction();
                using var cursor = tr.CreateCursor();
                for (var op = random.Next(1, 12); op > 0; op--)
                {
                    var key = Key(random.Next(120));
                    var hex = Convert.ToHexString(key);
                    if (random.Next(5) == 0)
                    {
                        if (!cursor.FindExactKey(key)) continue;
                        cursor.EraseCurrent();
                        model.Remove(hex);
                    }
                    else
                    {
                        var value = new byte[random.Next(3) == 0 ? random.Next(600) : 3];
                        random.NextBytes(value);
                        cursor.CreateOrUpdateKeyValue(key, value);
                        model[hex] = Convert.ToHexString(value);
                    }
                }

                tr.SetCommitUlong(firstCommit + (ulong)history.Count);
                history.Add(new(model, StringComparer.Ordinal));
                tr.Commit();
                // Synchronous mode: Commit returned after the WAL upload, so the commit is durable.
                durable = history.Count - 1;
            }
        }
        catch (Exception ex) when (ex is IOException or BTDBException)
        {
            LastError = ex;
        }

        return (history, durable);
    }

    [Fact]
    public async Task AmbiguousWritesAreReconciled()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new FaultyLsmStorage(inner, 1) { AmbiguousAfterApply = 0.15, AmbiguousWithoutApply = 0.15 };
        var db = await LsmKeyValueDB.OpenAsync(Options(storage));
        var (history, durable) = Workload(db, new(), 1, new Random(1), 400);
        Assert.Equal(399, durable);
        Assert.Equal(history[^1], Content(db));
        db.Dispose();
        using var reopened = await LsmKeyValueDB.OpenAsync(Options(inner));
        Assert.Equal(history[^1], Content(reopened));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task RecoversFromCrashAtEveryWrite(int seed)
    {
        for (var crashAt = 1; crashAt < 220; crashAt += seed == 1 ? 1 : 7)
        {
            var inner = new InMemoryLsmStorage();
            var start = new SortedDictionary<string, string>(StringComparer.Ordinal);
            // A first writer builds some history, including flushes and compactions, then crashes.
            var storage = new FaultyLsmStorage(inner, crashAt) { CrashAtWrite = crashAt };
            LsmKeyValueDB? db = null;
            var history = new List<SortedDictionary<string, string>> { start };
            var durable = 0;
            try
            {
                db = await LsmKeyValueDB.OpenAsync(Options(storage));
                var (h, d) = Workload(db, start, 1, new Random(crashAt), 60);
                history.AddRange(h);
                durable = d + 1;
            }
            catch (Exception ex) when (ex is IOException or BTDBException)
            {
            }

            db?.SimulateCrash();
            inner.ExpireAllLeases();
            using var recovered = await LsmKeyValueDB.OpenAsync(Options(inner));
            ulong commit;
            using (var tr = recovered.StartReadOnlyTransaction()) commit = tr.GetCommitUlong();
            Assert.True((int)commit >= durable, $"crash at {crashAt}: recovered {commit} < durable {durable}");
            Assert.Equal(history[(int)commit], Content(recovered));
            // The recovered writer keeps working on top of what it found.
            var (more, moreDurable) = Workload(recovered, history[(int)commit], commit + 1, new Random(-crashAt), 15);
            Assert.True(moreDurable == 14, $"crash at {crashAt}: {LastError}");
            Assert.Equal(more[^1], Content(recovered));
        }
    }
}
