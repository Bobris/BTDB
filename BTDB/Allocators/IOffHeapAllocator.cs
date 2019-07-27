using System;

namespace BTDB.Allocators
{
    public interface IOffHeapAllocator
    {
        IntPtr Allocate(IntPtr size);
        void Deallocate(IntPtr ptr);
    }
}
