using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class KeyValueDBCompactorSchedulerTest
{
    sealed class TestKeyValueDB : IKeyValueDB
    {
        readonly Func<CancellationToken, ValueTask<bool>> _compactAction;

        public TestKeyValueDB(Func<CancellationToken, ValueTask<bool>> compactAction)
        {
            _compactAction = compactAction;
        }

        public void Dispose()
        {
        }

        public bool DurableTransactions { get; set; }
        public IKeyValueDBTransaction StartTransaction() => throw new NotSupportedException();
        public IKeyValueDBTransaction StartReadOnlyTransaction() => throw new NotSupportedException();
        public ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false) => throw new NotSupportedException();
        public string CalcStats() => string.Empty;
        public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats() =>
            default;
        public ValueTask<bool> Compact(CancellationToken cancellation) => _compactAction(cancellation);
        public void CreateKvi(CancellationToken cancellation) => throw new NotSupportedException();
        public ulong? PreserveHistoryUpToCommitUlong { get; set; }
        public IKeyValueDBLogger? Logger { get; set; }
        public uint CompactorRamLimitInMb { get; set; }
        public long MaxTrLogFileSize { get; set; }
        public IEnumerable<IKeyValueDBTransaction> Transactions()
        {
            yield break;
        }

        public ulong CompactorReadBytesPerSecondLimit { get; set; }
        public ulong CompactorWriteBytesPerSecondLimit { get; set; }
    }

    [Fact]
    public void ItWillNotStartImmediately()
    {
        var run = false;
        using var db = new TestKeyValueDB(token =>
        {
            run = true;
            return ValueTask.FromResult(false);
        });
        using (var sch = new CompactorScheduler())
        {
            sch.AddCompactAction(db);
        }

        Assert.False(run, "Should not Start immediately");
    }

    [Fact]
    public void ItWillRunFirstTimeAfterWaitTime()
    {
        var e = new AutoResetEvent(false);
        using var db = new TestKeyValueDB(token =>
        {
            e.Set();
            return ValueTask.FromResult(false);
        });
        using var s = new CompactorScheduler();
        s.AddCompactAction(db);
        s.WaitTime = TimeSpan.FromMilliseconds(1);
        s.AdviceRunning(db, true);
        Assert.True(e.WaitOne(1000));
    }

    [Fact]
    public void ItShouldRunAgainSoonIfCompactionSuccessful()
    {
        var e = new AutoResetEvent(false);
        var first = true;
        using var db = new TestKeyValueDB(token =>
        {
            if (first)
            {
                first = false;
                return ValueTask.FromResult(true);
            }

            e.Set();
            return ValueTask.FromResult(false);
        });
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(db);
            s.WaitTime = TimeSpan.FromMilliseconds(1);
            s.AdviceRunning(db, true);
            Assert.True(e.WaitOne(1000));
            Assert.False(first);
        }
    }

    [Fact]
    public void ItShouldCancelRunningCompaction()
    {
        var e = new AutoResetEvent(false);
        using var db = new TestKeyValueDB(token =>
        {
            e.Set();
            while (true)
            {
                if (token.IsCancellationRequested)
                {
                    e.Set();
                    token.ThrowIfCancellationRequested();
                }
            }
        });
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(db);
            s.WaitTime = TimeSpan.FromMilliseconds(1);
            s.AdviceRunning(db, true);
            Assert.True(e.WaitOne(1000));
        }

        Assert.True(e.WaitOne(1000));
    }

    // Before the fix removing one database cancelled the compaction running for another, and neither it nor the
    // databases due after it were scheduled again, until their transaction log moved to a new file.
    [Fact]
    public void RemovingOtherDbKeepsInterruptedCompactionsScheduled()
    {
        var started = new ManualResetEventSlim();
        var rerunA = new ManualResetEventSlim();
        var ranB = new ManualResetEventSlim();
        var runsOfA = 0;
        using var dbA = new TestKeyValueDB(token =>
        {
            if (Interlocked.Increment(ref runsOfA) > 1)
            {
                rerunA.Set();
                return ValueTask.FromResult(false);
            }

            started.Set();
            while (!token.IsCancellationRequested) Thread.Sleep(1);
            token.ThrowIfCancellationRequested();
            return ValueTask.FromResult(false);
        });
        using var dbB = new TestKeyValueDB(_ =>
        {
            ranB.Set();
            return ValueTask.FromResult(false);
        });
        using var dbC = new TestKeyValueDB(_ => ValueTask.FromResult(false));
        using var s = new CompactorScheduler();
        s.WaitTime = TimeSpan.FromMilliseconds(1);
        s.AddCompactAction(dbA);
        s.AddCompactAction(dbB);
        s.AddCompactAction(dbC);
        s.AdviceRunning(dbA, true);
        s.AdviceRunning(dbB, true);
        Assert.True(started.Wait(1000));
        s.RemoveCompactAction(dbC);
        Assert.True(rerunA.Wait(2000), "The interrupted compaction did not run again");
        Assert.True(ranB.Wait(2000), "The compaction due after it did not run");
    }

    [Fact]
    public void AdviceRunningWorksWellForSharedInstance()
    {
        var e = new AutoResetEvent(false);
        using var db = new TestKeyValueDB(token =>
        {
            e.Set();
            return ValueTask.FromResult(false);
        });
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(db);
            s.WaitTime = TimeSpan.FromMilliseconds(1500);
            s.AdviceRunning(db, true);
            s.AdviceRunning(db, true);
            s.AdviceRunning(db, false);
            Assert.False(e.WaitOne(1000));
            Assert.True(e.WaitOne(800));
            s.AdviceRunning(db, true);
            Assert.False(e.WaitOne(200));
            s.AdviceRunning(db, false);
            Assert.True(e.WaitOne(500));
        }
    }

    [Fact]
    public void DbOpenedAfterFirstCompactingIsCorrectlyPlanned()
    {
        var e = new AutoResetEvent(false);
        using var db = new TestKeyValueDB(token =>
        {
            e.Set();
            return ValueTask.FromResult(false);
        });
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(db);
            s.WaitTime = TimeSpan.FromMilliseconds(50);
            s.AdviceRunning(db, true);
            Assert.True(e.WaitOne(100));
            s.AdviceRunning(db, true);
            Assert.True(e.WaitOne(100));
        }
    }

    [Fact]
    public void MissingOpeningAdviceDoesNotBlockScheduling()
    {
        var e = new AutoResetEvent(false);
        using var db = new TestKeyValueDB(token =>
        {
            e.Set();
            return ValueTask.FromResult(false);
        });
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(db);
            s.WaitTime = TimeSpan.FromMilliseconds(50);
            s.AdviceRunning(db, false);
            Assert.True(e.WaitOne(100));
        }
    }

    [Fact]
    public void ItCompactsOnlyAdvisedDb()
    {
        var firstDbCompacted = new AutoResetEvent(false);
        var secondDbCompacted = new AutoResetEvent(false);
        using var firstDb = new TestKeyValueDB(token =>
        {
            firstDbCompacted.Set();
            return ValueTask.FromResult(false);
        });
        using var secondDb = new TestKeyValueDB(token =>
        {
            secondDbCompacted.Set();
            return ValueTask.FromResult(false);
        });
        using var scheduler = new CompactorScheduler();
        scheduler.AddCompactAction(firstDb);
        scheduler.AddCompactAction(secondDb);
        scheduler.WaitTime = TimeSpan.FromMilliseconds(50);

        scheduler.AdviceRunning(firstDb, false);

        Assert.True(firstDbCompacted.WaitOne(1000));
        Assert.False(secondDbCompacted.WaitOne(200));
    }
}
