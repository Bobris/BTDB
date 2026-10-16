using System;
using System.Collections.Concurrent;
using System.Threading;

namespace BTDB.LSM.Format;

/// Dedicated threads for readahead and prefetch reads (ImplementationPlan.md, section 12.2 and risk R2). They use
/// the storage's synchronous ranged read, so a cursor waiting for a block being read ahead never waits for a
/// thread-pool thread, and many blocked cursors cannot starve the reads they wait for. Requests beyond the thread
/// count queue; a cursor whose block is still queued waits for it like for any read in flight.
static class LsmReadThreads
{
    const int ThreadCount = 16;
    static readonly BlockingCollection<Action> Queue = new();
    static int _started;

    public static void Run(Action read)
    {
        if (Interlocked.Exchange(ref _started, 1) == 0)
        {
            for (var i = 0; i < ThreadCount; i++)
                new Thread(Loop) { IsBackground = true, Name = "BTDB.LSM read " + i }.Start();
        }

        Queue.Add(read);
    }

    static void Loop()
    {
        foreach (var read in Queue.GetConsumingEnumerable())
        {
            try
            {
                read();
            }
            catch
            {
                // Each read completes its own waiters; nothing may stop the thread.
            }
        }
    }
}
