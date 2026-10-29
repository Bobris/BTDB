using System;
using System.Threading;

namespace BTDB.KVDBLayer;

// Accessed during initial loading or under the collection's creation lock.
internal struct FileIdAllocator
{
    uint _lastOddId;
    uint _lastEvenId;

    internal void Observe(uint id)
    {
        if ((id & 1) != 0) _lastOddId = Math.Max(_lastOddId, id);
        else _lastEvenId = Math.Max(_lastEvenId, id);
    }

    internal uint Allocate(FileIdParity parity)
    {
        var lastId = parity switch
        {
            FileIdParity.Odd => _lastOddId,
            FileIdParity.Even => _lastEvenId,
            FileIdParity.Any => Math.Max(_lastOddId, _lastEvenId),
            _ => throw new ArgumentOutOfRangeException(nameof(parity))
        };
        var next = (ulong)lastId + 1;
        if (parity != FileIdParity.Any && (next & 1) != (parity == FileIdParity.Odd ? 1ul : 0ul)) next++;
        if (next > uint.MaxValue) throw new InvalidOperationException("File IDs exhausted.");
        Observe((uint)next);
        return (uint)next;
    }

    /// Advances maxFileId to the next ID and returns it, safely from several threads. It never wraps around to 0, which
    /// means no file, so a file found with the largest uint ID leaves none to allocate.
    internal static uint Next(ref uint maxFileId)
    {
        while (true)
        {
            var current = Volatile.Read(ref maxFileId);
            if (current == uint.MaxValue) throw new InvalidOperationException("File IDs exhausted.");
            if (Interlocked.CompareExchange(ref maxFileId, current + 1, current) == current) return current + 1;
        }
    }
}
