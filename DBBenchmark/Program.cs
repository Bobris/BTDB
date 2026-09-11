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

        if (args.Length > 0 && string.Equals(args[0], "transaction-batching", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(TransactionBatchingBenchmark)]).Run(args[1..]);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "secondary-key", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(SecondaryKeyAllocationBenchmark)]).Run(args[1..]);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "string-conversion", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(OrderedStringConversionBenchmark)]).Run(args[1..]);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "integer-secondary", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(IntegerSecondaryKeyBenchmark)]).Run(args[1..]);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "integer-copy", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(VarIntCopyBenchmark)]).Run(args[1..]);
            return;
        }

        new KeyValueSpeedTest().Run();
    }
}
