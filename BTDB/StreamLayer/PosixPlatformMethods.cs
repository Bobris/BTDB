#if NETCOREAPP
using System;
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
                    result = Mono.Unix.Native.Syscall.pread((int) handle.DangerousGetHandle(), dataptr,
                        (ulong) data.Length, (long) position);
                } while (UnixMarshal.ShouldRetrySyscall((int) result));

                if (result == -1)
                    UnixMarshal.ThrowExceptionForLastError();
                return (uint) result;
            }
        }

        public unsafe void PWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, ulong position)
        {
            fixed (void* dataptr = data)
            {
                long result;
                do
                {
                    result = Mono.Unix.Native.Syscall.pwrite((int) handle.DangerousGetHandle(), dataptr,
                        (ulong) data.Length, (long) position);
                } while (UnixMarshal.ShouldRetrySyscall((int) result));

                if (result == -1)
                    UnixMarshal.ThrowExceptionForLastError();
                if (result != data.Length)
                    throw new BTDBException($"Out of disk space written {result} out of {data.Length} at {position}");
            }
        }
    }
}
#endif
