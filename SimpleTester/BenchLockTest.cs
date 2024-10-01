using System;
using System.Threading;
using BenchmarkDotNet.Attributes;
using BTDB.KVDBLayer;
using BTDB.Locks;

namespace SimpleTester;

// | Method                     | Mean       | Error     | StdDev    |
// |--------------------------- |-----------:|----------:|----------:|
// | SeqLock_Read               |  0.8039 ns | 0.0120 ns | 0.0100 ns |
// | ReaderWriterLockSlim_Read  | 12.5249 ns | 0.0526 ns | 0.0466 ns |
// | MonitorLock_Read           |  8.6376 ns | 0.0472 ns | 0.0441 ns |
// | SeqLock_Write              |  3.7167 ns | 0.0157 ns | 0.0139 ns |
// | ReaderWriterLockSlim_Write | 13.5983 ns | 0.0503 ns | 0.0420 ns |
// | MonitorLock_Write          |  8.1922 ns | 0.0323 ns | 0.0287 ns |

[InProcess]
public class BenchLockTest
{
    ulong _global;
    int _lockCounter;
    ReaderWriterLockSlim _lock = new();
    SeqLock _seqLock;
    object _lockObj = new();

    [GlobalSetup]
    public void GlobalSetup()
    {
        Interlocked.Increment(ref _lockCounter);
        _global = (ulong)Random.Shared.Next();
        Interlocked.Increment(ref _lockCounter);
    }

    [Benchmark]
    public ulong SeqLock_Read()
    {
        var seqCounter = _seqLock.StartRead();
        retry:
        var res = _global;
        if (_seqLock.RetryRead(ref seqCounter)) goto retry;
        return res;
    }

    [Benchmark]
    public ulong ReaderWriterLockSlim_Read()
    {
        using (_lock.ReadLock())
        {
            return _global;
        }
    }

    [Benchmark]
    public ulong MonitorLock_Read()
    {
        lock (_lockObj)
        {
            return _global;
        }
    }

    [Benchmark]
    public void SeqLock_Write()
    {
        _seqLock.StartWrite();
        try
        {
            _global++;
        }
        finally
        {
            _seqLock.EndWrite();
        }
    }

    [Benchmark]
    public void ReaderWriterLockSlim_Write()
    {
        using (_lock.WriteLock())
        {
            _global++;
        }
    }

    [Benchmark]
    public void MonitorLock_Write()
    {
        lock (_lockObj)
        {
            _global++;
        }
    }
}
