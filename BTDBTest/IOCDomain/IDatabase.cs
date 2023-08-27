using BTDB;

namespace BTDBTest.IOCDomain;

[Generate]
public interface IDatabase
{
    ILogger Logger { get; }
    IErrorHandler ErrorHandler { get; }
}
