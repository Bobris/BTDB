using System;
using System.Runtime.InteropServices;

namespace BTDB.Allocators;

public class MallocAllocator : IOffHeapAllocator
{
    public unsafe IntPtr Allocate(IntPtr size)
    {
        return (IntPtr)NativeMemory.Alloc((nuint)size.ToPointer());
    }

    public unsafe void Deallocate(IntPtr ptr)
    {
        NativeMemory.Free(ptr.ToPointer());
    }
}
