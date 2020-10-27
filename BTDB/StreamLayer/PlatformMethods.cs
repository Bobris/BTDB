using System.Runtime.InteropServices;

namespace BTDB.StreamLayer
{
    public class PlatformMethods
    {
        static PlatformMethods()
        {
#if NET5_0
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Instance = new WindowsPlatformMethods();
            }
            else
            {
                Instance = new PosixPlatformMethods();
            }
#else
            Instance = new WindowsPlatformMethods();
#endif
        }

        public static readonly IPlatformMethods Instance;
    }
}
