using System;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
{
    public class HGlobalAllocator : IOffHeapAllocator
    {
        public IntPtr Allocate(IntPtr size)
        {
            return Marshal.AllocHGlobal(size);
        }

        public void Deallocate(IntPtr ptr)
        {
            Marshal.FreeHGlobal(ptr);
        }
    }
}
