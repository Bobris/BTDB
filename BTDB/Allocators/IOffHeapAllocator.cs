using System;

namespace BTDB.Allocators;

public interface IOffHeapAllocator
{
    IntPtr Allocate(IntPtr size);
    void Deallocate(IntPtr ptr, IntPtr size);

    (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetStats();
}
