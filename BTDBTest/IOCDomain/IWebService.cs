using BTDB;

namespace BTDBTest.IOCDomain;

[Generate]
public interface IWebService
{
    IAuthenticator Authenticator { get; }
    IStockQuote StockQuote { get; }
    void Execute();
}
