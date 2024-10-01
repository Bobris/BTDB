using System.Threading;

namespace BTDB.Locks;

/// <summary>
/// Super optimistic reader writer lock.
/// It is suitable for rare writes and many parallel reads.
/// Read cannot hang or crash or corrupt write when write is in progress.
/// Readers never block writer. Writer blocks other writers. Writer lock is not reentrant.
/// It is struct and its size is just 4 bytes.
/// Pattern for read is this:
/// var seqCounter = seqLock.StartRead();
/// retry:
/// try
/// {
///     // read data
///     if (seqLock.RetryRead(ref seqCounter)) goto retry;
/// }
/// catch
/// {
///     if (seqLock.RetryRead(ref seqCounter)) goto retry;
///     throw;
/// }
///
/// Pattern for write is this:
/// seqLock.StartWrite();
/// try
/// {
///     // write data
/// }
/// finally
/// {
///     seqLock.EndWrite();
/// }
/// </summary>
public struct SeqLock
{
    uint _counter;

    public uint StartRead()
    {
        SpinWait spin = default;
        var res = Volatile.Read(ref _counter);
        while ((res & 1u) != 0)
        {
            spin.SpinOnce();
            res = Volatile.Read(ref _counter);
        }

        return res;
    }

    public bool RetryRead(ref uint seqCounter)
    {
        var current = Volatile.Read(ref _counter);
        if (seqCounter != current)
        {
            SpinWait spin = default;
            while ((current & 1u) != 0)
            {
                spin.SpinOnce();
                current = Volatile.Read(ref _counter);
            }

            seqCounter = current;
            return true;
        }

        return false;
    }

    public void StartWrite()
    {
        SpinWait spin = default;
        while (true)
        {
            var counter = _counter;
            while ((counter & 1u) != 0)
            {
                spin.SpinOnce();
                counter = Volatile.Read(ref _counter);
            }

            if (Interlocked.CompareExchange(ref _counter, counter + 1u, counter) == counter)
                break;
            spin.SpinOnce();
        }
    }

    public void EndWrite()
    {
        Volatile.Write(ref _counter, _counter + 1);
    }
}
