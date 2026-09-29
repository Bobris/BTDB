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

        if (args.Length > 0 && string.Equals(args[0], "replication-commit", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(Replication.ReplicationCommitBenchmark)]).Run(args[1..]);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "eventlog-storage", StringComparison.OrdinalIgnoreCase))
        {
            EventLog.EventLogStorageScreening.RunAsync().GetAwaiter().GetResult();
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "eventlog-e2e", StringComparison.OrdinalIgnoreCase))
        {
            EventLog.EventLogEndToEnd.RunAsync(args[1..]).GetAwaiter().GetResult();
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "replication", StringComparison.OrdinalIgnoreCase))
        {
            Replication.ReplicationMeasurements.RunAsync(args[1..]).GetAwaiter().GetResult();
            return;
        }

        new KeyValueSpeedTest().Run();
    }
}
