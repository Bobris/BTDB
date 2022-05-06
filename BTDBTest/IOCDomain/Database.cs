namespace BTDBTest.IOCDomain;

public class Database : IDatabase
{
    public Database(IErrorHandler handler, ILogger logger)
    {
        ErrorHandler = handler;
        Logger = logger;
    }

    public ILogger Logger { get; }

    public IErrorHandler ErrorHandler { get; }
}
