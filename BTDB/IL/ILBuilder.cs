using System.Diagnostics;
using BTDB.IL.Caching;

namespace BTDB.IL
{
    public static class ILBuilderConfig
    {
        public static bool PreventDebugOutput { get; set; }
    }

    public static class ILBuilder
    {
        static ILBuilder()
        {
            NoCachingInstance = Debugger.IsAttached && !ILBuilderConfig.PreventDebugOutput
                ? (IILBuilder)new ILBuilderDebug()
                : new ILBuilderRelease();
            Instance = new CachingILBuilder(NoCachingInstance);
        }

        public static IILBuilder Instance { get; private set; }
        public static IILBuilder NoCachingInstance { get; private set; }
    }
}