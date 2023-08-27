using BTDB;

namespace BTDBTest.IOCDomain;

[Generate]
public class Logger : ILogger
{
    public bool Verbose { get; set; }
}
