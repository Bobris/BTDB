using BTDB;

namespace BTDBTest.IOCDomain;

[Generate]
public interface IErrorHandler
{
    ILogger Logger { get; }
}
