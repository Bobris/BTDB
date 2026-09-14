using System;
using System.Threading;

namespace BTDB.KVDBLayer;

internal static class FileIdAllocator
{
    internal static uint Allocate(ref int lastId, FileIdParity parity)
    {
        if (parity == FileIdParity.Any) return (uint)Interlocked.Increment(ref lastId);
        if (parity is not (FileIdParity.Odd or FileIdParity.Even))
            throw new ArgumentOutOfRangeException(nameof(parity));
        while (true)
        {
            var previous = Volatile.Read(ref lastId);
            var next = (ulong)(uint)previous + 1;
            if ((next & 1) != (parity == FileIdParity.Odd ? 1ul : 0ul)) next++;
            if (next > uint.MaxValue) throw new InvalidOperationException("File IDs exhausted.");
            if (Interlocked.CompareExchange(ref lastId, unchecked((int)next), previous) == previous)
                return (uint)next;
        }
    }
}
