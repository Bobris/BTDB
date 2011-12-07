namespace BTDBTest.IOCDomain
{
	public class Database : IDatabase
	{
	    readonly ILogger _logger;
	    readonly IErrorHandler _handler;

		public Database(ILogger logger, IErrorHandler handler)
		{
			_logger = logger;
			_handler = handler;
		}

		public ILogger Logger { get { return _logger; } }
		public IErrorHandler ErrorHandler { get { return _handler; } }
	}
}
