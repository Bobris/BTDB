using System.Diagnostics;
using BTDB.IL.Caching;

namespace BTDB.IL
{
    public static class ILBuilder
    {
        static ILBuilder()
        {
            Instance = Debugger.IsAttached ? new CachingILBuilder(new ILBuilderDebug()) : new CachingILBuilder(new ILBuilderRelease());
        }

        public static IILBuilder Instance { get; private set; }
    }
}