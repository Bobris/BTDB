namespace BTDBTest.IOCDomain
{
    public class ErrorHandler : IErrorHandler
    {
        readonly ILogger _logger;

        public ErrorHandler(ILogger logger)
        {
            _logger = logger;
        }

        public ILogger Logger => _logger;
    }
}
