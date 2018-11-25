using System;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.KVDBLayer;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer
{
    public class WindowsPlatformMethods : IPlatformMethods
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern unsafe int ReadFile(SafeFileHandle handle, byte* bytes, int numBytesToRead,
            IntPtr numBytesRead_mustBeZero, NativeOverlapped* overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern unsafe int WriteFile(SafeFileHandle handle, byte* bytes, int numBytesToWrite,
            IntPtr numBytesWritten_mustBeZero, NativeOverlapped* lpOverlapped);

        public unsafe uint PRead(SafeFileHandle handle, Span<byte> data, ulong position)
        {
            fixed (byte* dataptr = data)
            {
                NativeOverlapped overlapped;
                overlapped.OffsetLow = (int) (uint) (position & 0xffffffff);
                overlapped.OffsetHigh = (int) (position >> 32);
                uint bread = 0;
                var result = ReadFile(handle, dataptr, data.Length, (IntPtr) (&bread), &overlapped);
                if (result != 0)
                    return bread;
                throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
            }
        }

        public unsafe void PWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, ulong position)
        {
            fixed (byte* dataptr = data)
            {
                NativeOverlapped overlapped;
                overlapped.OffsetLow = (int) (uint) (position & 0xffffffff);
                overlapped.OffsetHigh = (int) (position >> 32);
                uint bwrite = 0;
                var result = WriteFile(handle, dataptr, data.Length, (IntPtr) (&bwrite), &overlapped);
                if (result == 0)
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error());
                if (bwrite != data.Length)
                    throw new BTDBException($"Out of disk space written {bwrite} out of {data.Length} at {position}");
            }
        }
    }
}
