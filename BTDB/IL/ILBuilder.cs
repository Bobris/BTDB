using BTDB.IL.Caching;

namespace BTDB.IL;

public static class ILBuilderConfig
{
    public static bool PreventDebugOutput { get; set; }
}

public static class ILBuilder
{
    static ILBuilder()
    {
        NoCachingInstance = new ILBuilderRelease();
        Instance = new CachingILBuilder(NoCachingInstance);
    }

    public static IILBuilder Instance { get; private set; }
    public static IILBuilder NoCachingInstance { get; private set; }
}
