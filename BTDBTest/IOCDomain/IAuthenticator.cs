using BTDB;

namespace BTDBTest.IOCDomain;

[Generate]
public interface IAuthenticator
{
    ILogger Logger { get; }
    IErrorHandler ErrorHandler { get; }
    IDatabase Database { get; }
}
