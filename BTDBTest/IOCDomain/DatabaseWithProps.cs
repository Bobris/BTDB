// ReSharper disable NotNullMemberIsNotInitialized
namespace BTDBTest.IOCDomain
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class DatabaseWithProps : IDatabase
    {
        public ILogger Logger { get; private set; }
        public IErrorHandler ErrorHandler { get; private set; }
    }
}
