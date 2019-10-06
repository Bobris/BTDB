using System;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer
{
    public interface IPlatformMethods
    {
        uint PRead(SafeFileHandle handle, Span<byte> data, ulong position);
        void PWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, ulong position);

        /// returns original name of any existing file or directory (follows symlinks), if anything fails it returns null. On windows it also fixes casing
        string? RealPath(string path);
    }
}
