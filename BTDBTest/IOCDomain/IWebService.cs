namespace BTDBTest.IOCDomain
{
    public interface IWebService
    {
        IAuthenticator Authenticator { get; }
        IStockQuote StockQuote { get; }
        void Execute();
    }
}