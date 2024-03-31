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
}
