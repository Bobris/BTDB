using System;
using System.Threading;
using BenchmarkDotNet.Attributes;
using BTDB.KVDBLayer;
using BTDB.Locks;

namespace SimpleTester;

// | Method                     | Mean      | Error     | StdDev    |
// |--------------------------- |----------:|----------:|----------:|
// | SeqLock_Read               |  1.239 ns | 0.0044 ns | 0.0039 ns |
// | ReaderWriterLockSlim_Read  | 12.508 ns | 0.0304 ns | 0.0284 ns |
// | MonitorLock_Read           |  8.579 ns | 0.0291 ns | 0.0272 ns |
// | SeqLock_Write              |  4.148 ns | 0.0252 ns | 0.0236 ns |
// | ReaderWriterLockSlim_Write | 13.532 ns | 0.0323 ns | 0.0302 ns |
// | MonitorLock_Write          |  8.145 ns | 0.0272 ns | 0.0254 ns |

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
