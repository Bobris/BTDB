namespace BTDBTest.IOCDomain
{
	public class Database : IDatabase
	{
	    readonly ILogger _logger;
	    readonly IErrorHandler _handler;

		public Database(IErrorHandler handler, ILogger logger)
		{
			_handler = handler;
            _logger = logger;
        }

		public ILogger Logger => _logger;
	    public IErrorHandler ErrorHandler => _handler;
	}
}
