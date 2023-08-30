// ReSharper disable NotNullMemberIsNotInitialized

using BTDB.IOC;

namespace BTDBTest.IOCDomain;

// ReSharper disable once ClassNeverInstantiated.Global
public class DatabaseWithProps : IDatabase
{
    [Dependency]
    public ILogger Logger { get; init; }
    [Dependency]
    public IErrorHandler ErrorHandler { get; init; }
}
