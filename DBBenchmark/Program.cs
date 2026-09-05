using System;
using BenchmarkDotNet.Running;

namespace DBBenchmark;

class Program
{
    static void Main(string[] args)
    {
        if (args.Length > 0 && string.Equals(args[0], "company-user-role", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(CompanyUserRoleScanBenchmark)]).Run(args[1..]);
            return;
        }

        new KeyValueSpeedTest().Run();
    }
}
