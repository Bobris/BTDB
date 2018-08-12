using System;

namespace BTDB.ARTLib
{
    public interface IOffHeapAllocator
    {
        IntPtr Allocate(IntPtr size);
        void Deallocate(IntPtr ptr);
    }
}
