using BTDB;

namespace BTDBTest.IOCDomain;

[Generate]
public interface IStockQuote
{
    ILogger Logger { get; }
    IErrorHandler ErrorHandler { get; }
    IDatabase Database { get; }
}
