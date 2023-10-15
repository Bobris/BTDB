using System.Runtime.InteropServices;

namespace BTDB.StreamLayer;

public static class PlatformMethods
{
    static PlatformMethods()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            Instance = new WindowsPlatformMethods();
        }
        else
        {
            Instance = new PosixPlatformMethods();
        }
    }

    public static readonly IPlatformMethods Instance;
}
