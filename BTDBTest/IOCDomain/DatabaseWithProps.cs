// ReSharper disable NotNullMemberIsNotInitialized

using BTDB.IOC;

namespace BTDBTest.IOCDomain;

// ReSharper disable once ClassNeverInstantiated.Global
public partial class DatabaseWithProps : IDatabase
{
    [Dependency]
    public ILogger Logger { get; private set; }
    [Dependency]
    public IErrorHandler ErrorHandler { get; private set; }
}
