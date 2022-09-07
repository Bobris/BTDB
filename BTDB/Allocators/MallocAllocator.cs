using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace BTDB.Allocators;

public class MallocAllocator : IOffHeapAllocator
{
    ulong _allocSize;
    ulong _allocCount;
    ulong _deallocSize;
    ulong _deallocCount;

    public unsafe IntPtr Allocate(IntPtr size)
    {
        Interlocked.Add(ref _allocSize, (ulong)size.ToInt64());
        Interlocked.Increment(ref _allocCount);
        return (IntPtr)NativeMemory.Alloc((nuint)size.ToPointer());
    }

    public unsafe void Deallocate(IntPtr ptr, IntPtr size)
    {
        Interlocked.Add(ref _deallocSize, (ulong)size.ToInt64());
        Interlocked.Increment(ref _deallocCount);
        NativeMemory.Free(ptr.ToPointer());
    }

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetStats()
    {
        return (Interlocked.Read(ref _allocSize), Interlocked.Read(ref _allocCount), Interlocked.Read(ref _deallocSize),
            Interlocked.Read(ref _deallocCount));
    }
}
