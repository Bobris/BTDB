using System.Runtime.CompilerServices;
using BTDB.IOC;

namespace BTDBTest.IOCDomain;

public class Logger : ILogger
{
    public bool Verbose { get; set; }

    [ModuleInitializer]
    public static void Init()
    {
        IContainer.RegisterFactory(typeof(Logger).TypeHandle.Value, (_, _) => (_, _) => new Logger());
    }
}
