namespace BTDBTest.IOCDomain
{
    public interface IDatabase
    {
        ILogger Logger { get; }
        IErrorHandler ErrorHandler { get; }
    }
}
