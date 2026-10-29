using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Locks;
using Xunit;

namespace BTDBTest;

public class SeqLockTests
{
    [Fact]
    public void SimpleWriteLockWorks()
    {
        var data = 0;
        var seqLock = new SeqLock();
        seqLock.StartWrite();
        data++;
        seqLock.EndWrite();

        Assert.Equal(1, data);
    }

    [Fact]
    public void SimpleReadLockWorks()
    {
        var data = 0;
        var seqLock = new SeqLock();
        seqLock.StartWrite();
        data++;
        seqLock.EndWrite();

        var seqCounter = seqLock.StartRead();
        Assert.Equal(1, data);
        Assert.False(seqLock.RetryRead(ref seqCounter));
    }

    [Fact]
    public void ComplexReadLockWorks()
    {
        var data = 0;
        var seqLock = new SeqLock();

        Task.Run(() =>
        {
            seqLock.StartWrite();
            data++;
            seqLock.EndWrite();
        });

        int readData;
        do
        {
            var seqCounter = seqLock.StartRead();
            retry:
            readData = data;
            if (seqLock.RetryRead(ref seqCounter)) goto retry;
        } while (readData != 1);
    }

    sealed class Pair
    {
        public SeqLock Lock;
        public long A;
        public long Padding1, Padding2, Padding3, Padding4, Padding5, Padding6, Padding7;
        public long B;
        public volatile bool Stop;
    }

    // Before the fix RetryRead read the counter with only acquire semantics, so on arm64 the data reads before it could
    // complete after it: under contention some reads passed as consistent while they saw a half-done write.
    [Fact]
    public void ReadsThatPassRetryNeverSeeAHalfDoneWrite()
    {
        var pair = new Pair();
        var writer = new Thread(() =>
        {
            while (!pair.Stop)
            {
                pair.Lock.StartWrite();
                var next = pair.A + 1;
                pair.A = next;
                pair.B = next;
                pair.Lock.EndWrite();
            }
        });
        writer.Start();
        var torn = 0L;
        var readers = new Thread[Math.Clamp(Environment.ProcessorCount - 2, 2, 8)];
        for (var r = 0; r < readers.Length; r++)
        {
            readers[r] = new(() =>
            {
                var localTorn = 0L;
                var stopwatch = Stopwatch.StartNew();
                while (stopwatch.ElapsedMilliseconds < 2000)
                {
                    var seqCounter = pair.Lock.StartRead();
                    long a, b;
                    retry:
                    b = pair.B;
                    a = pair.A;
                    if (pair.Lock.RetryRead(ref seqCounter)) goto retry;
                    if (a != b) localTorn++;
                }

                Interlocked.Add(ref torn, localTorn);
            });
            readers[r].Start();
        }

        foreach (var reader in readers) reader.Join();
        pair.Stop = true;
        writer.Join();
        Assert.Equal(0, torn);
    }
}
