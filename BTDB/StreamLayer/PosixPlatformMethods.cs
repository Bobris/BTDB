using System;
using System.Runtime.InteropServices;
using BTDB.KVDBLayer;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer;

public class PosixPlatformMethods : IPlatformMethods
{
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
