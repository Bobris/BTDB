namespace BTDBTest.IOCDomain
{
    public class WebService : IWebService
    {
        readonly IAuthenticator _authenticator;
        readonly IStockQuote _quotes;

        public WebService(IAuthenticator authenticator, IStockQuote quotes)
        {
            _authenticator = authenticator;
            _quotes = quotes;
        }

        public IAuthenticator Authenticator => _authenticator;
        public IStockQuote StockQuote => _quotes;

        public void Execute()
        {
        }
    }
}