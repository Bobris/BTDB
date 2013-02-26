namespace BTDBTest.IOCDomain
{
    public class LoggerWithErrorHandler : ILogger, IErrorHandler
    {
        public ILogger Logger { get { return this; } }
    }
}