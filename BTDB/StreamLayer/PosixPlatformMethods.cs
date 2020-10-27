#if NET5_0
using System;
using System.Runtime.InteropServices;
using BTDB.KVDBLayer;
using Microsoft.Win32.SafeHandles;
using Mono.Unix;

namespace BTDB.StreamLayer
{
    public class PosixPlatformMethods : IPlatformMethods
    {
        public unsafe uint PRead(SafeFileHandle handle, Span<byte> data, ulong position)
        {
            fixed (void* dataptr = data)
            {
                long result;
                do
                {
                    result = Mono.Unix.Native.Syscall.pread((int)handle.DangerousGetHandle(), dataptr,
                        (ulong)data.Length, (long)position);
                } while (UnixMarshal.ShouldRetrySyscall((int)result));

                if (result == -1)
                    UnixMarshal.ThrowExceptionForLastError();
                return (uint)result;
            }
        }

        public unsafe void PWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, ulong position)
        {
            fixed (void* dataptr = data)
            {
                long result;
                do
                {
                    result = Mono.Unix.Native.Syscall.pwrite((int)handle.DangerousGetHandle(), dataptr,
                        (ulong)data.Length, (long)position);
                } while (UnixMarshal.ShouldRetrySyscall((int)result));

                if (result == -1)
                    UnixMarshal.ThrowExceptionForLastError();
                if (result != data.Length)
                    throw new BTDBException($"Out of disk space written {result} out of {data.Length} at {position}");
            }
        }

        [DllImport("libc", EntryPoint = "realpath", CharSet = CharSet.Ansi, ExactSpelling = true, CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr UnixRealPath(string path, IntPtr buffer);

        [DllImport("libc", EntryPoint = "free", ExactSpelling = true, CallingConvention = CallingConvention.Cdecl)]
        static extern void UnixFree(IntPtr ptr);

        public string? RealPath(string path)
        {
            var ptr = UnixRealPath(path, IntPtr.Zero);
            if (ptr == IntPtr.Zero)
                return null;
            string result;
            try
            {
                result = Marshal.PtrToStringAnsi(ptr); // uses UTF8 on Unix
            }
            finally
            {
                UnixFree(ptr);
            }
            return result;
        }
    }
}
#endif
