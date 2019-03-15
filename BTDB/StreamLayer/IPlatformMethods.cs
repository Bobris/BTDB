using System;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer
{
    public interface IPlatformMethods
    {
        uint PRead(SafeFileHandle handle, Span<byte> data, ulong position);
        void PWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, ulong position);
    }
}
