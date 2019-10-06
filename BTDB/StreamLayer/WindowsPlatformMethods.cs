using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
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
                overlapped.OffsetLow = (int)(uint)(position & 0xffffffff);
                overlapped.OffsetHigh = (int)(position >> 32);
                uint bread = 0;
                var result = ReadFile(handle, dataptr, data.Length, (IntPtr)(&bread), &overlapped);
                if (result != 0)
                    return bread;
                var lastError = Marshal.GetLastWin32Error();
                if (lastError == 38) //ERROR_HANDLE_EOF
                    return 0;
                throw Marshal.GetExceptionForHR(lastError & ushort.MaxValue | -2147024896) ?? new IOException(); //GetHRForLastWin32Error
            }
        }

        public unsafe void PWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, ulong position)
        {
            fixed (byte* dataptr = data)
            {
                NativeOverlapped overlapped;
                overlapped.OffsetLow = (int)(uint)(position & 0xffffffff);
                overlapped.OffsetHigh = (int)(position >> 32);
                uint bwrite = 0;
                var result = WriteFile(handle, dataptr, data.Length, (IntPtr)(&bwrite), &overlapped);
                if (result == 0)
                    throw Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error()) ?? new IOException();
                if (bwrite != data.Length)
                    throw new BTDBException($"Out of disk space written {bwrite} out of {data.Length} at {position}");
            }
        }

        const int CREATION_DISPOSITION_OPEN_EXISTING = 3;

        const int FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;

        [DllImport("kernel32.dll", EntryPoint = "GetFinalPathNameByHandleW", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern int GetFinalPathNameByHandle(IntPtr handle, [In, Out] StringBuilder path, int bufLen, int flags);

        [DllImport("kernel32.dll", EntryPoint = "CreateFileW", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern SafeFileHandle CreateFile(string lpFileName, int dwDesiredAccess, int dwShareMode,
        IntPtr SecurityAttributes, int dwCreationDisposition, int dwFlagsAndAttributes, IntPtr hTemplateFile);

        public string? RealPath(string opath)
        {
            using (var directoryHandle = CreateFile(opath, 0, 2, IntPtr.Zero, CREATION_DISPOSITION_OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, IntPtr.Zero))
            {
                if (directoryHandle.IsInvalid)
                    return null;

                var result = new StringBuilder(512);
                int size = GetFinalPathNameByHandle(directoryHandle.DangerousGetHandle(), result, result.Capacity, 0);

                if (size < 0)
                    return null;
                if (result[0] == '\\' && result[1] == '\\' && result[2] == '?' && result[3] == '\\')
                {
                    return result.ToString(4, result.Length - 4);
                }
                return result.ToString();
            }
        }
    }
}
