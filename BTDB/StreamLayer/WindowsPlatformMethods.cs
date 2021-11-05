using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using System.Threading;
using BTDB.KVDBLayer;
using Microsoft.Win32.SafeHandles;

namespace BTDB.StreamLayer;

[SupportedOSPlatform("windows")]
public class WindowsPlatformMethods : IPlatformMethods
{
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
