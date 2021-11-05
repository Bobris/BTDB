namespace BTDBTest.IOCDomain;

public class StockQuote : IStockQuote
{
    readonly ILogger _logger;
    readonly IErrorHandler _handler;
    readonly IDatabase _database;

    public StockQuote(ILogger logger, IErrorHandler handler, IDatabase database)
    {
        _logger = logger;
        _handler = handler;
        _database = database;
    }

    public ILogger Logger => _logger;
    public IErrorHandler ErrorHandler => _handler;
    public IDatabase Database => _database;
}
