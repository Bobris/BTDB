using System.Diagnostics;

namespace BTDB.IL
{
    public static class ILBuilder
    {
        static ILBuilder()
        {
            Instance = new ILBuilderImpl { Debuggable = Debugger.IsAttached };
        }

        public static IILBuilder Instance { get; set; }
    }
}