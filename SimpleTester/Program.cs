//using JetBrains.Profiler.Windows.Api;

using System;
using System.Threading;
using BenchmarkDotNet.Running;

namespace SimpleTester;

static class Program
{
    static void Main(string[] args)
    {
        if (args.Length > 0 && string.Equals(args[0], "ioc-benchmark", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(IocResolveBenchmark)]).Run(args[1..]);
            return;
        }
        if (args.Length > 0 && string.Equals(args[0], "roaring-benchmark", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkSwitcher.FromTypes([typeof(RoaringBitmapsBenchmark)]).Run(args[1..]);
            return;
        }

        //var b = new EventSerializationBenchmark();
        //b.Complexity = "Complex";
        //if (MemoryProfiler.IsActive && MemoryProfiler.CanControlAllocations)
        //    MemoryProfiler.EnableAllocations();
        //if (MemoryProfiler.IsActive)
        //    MemoryProfiler.Dump();
        //b.Setup();
        //b.BtdbSerialization();
        //if (MemoryProfiler.IsActive)
        //    MemoryProfiler.Dump();
        //new SpeedTest1().Test();
        //new ChannelSpeedTest().Run(args);
        //new RxSpeedTest().Run();
        //new ComplexServiceTest().Run();
        //new KeyValueSpeedTest(KVType.Managed, false, true).Run();
        new KeyValueSpeedTest(KVType.BTree, false, true).Run();
        //new EventStorageSpeedTestAwaitable().Run();
        //new EventStorageSpeedTestDisruptor().Run();
        //new EventStorageSpeedTest().Run();
        //new RelationSpeedTest().Run();
        //BenchmarkRunner.Run<BenchmarkRelationPartialView>();
        //BenchmarkRunner.Run<EventSerializationBenchmark>();
        //new TestCompactor().Run(new CancellationToken());
        //new CompactorLatencyMeasurement().Run();
        //new ClassGenerator().Run();
        //new EventLayer2TestWithALotOfClasses().Run();
        //new BenchTest4().Verify();
        //BenchmarkRunner.Run<BenchTest4>();
        //new KeyValueDBRollbackTest().CanOpenDbAfterDeletingAndCompacting();
        //
        //var cts = new CancellationTokenSource();
        //Console.CancelKeyPress += delegate { cts.Cancel(); };
        //new CompactionStressTest().Run("e:/testdb", cts.Token);
        //new NativeVsManagedBugFinder().Torture();
        //new BenchTestSpanReaderWriter().GlobalSetup();
        //BenchmarkRunner.Run<BenchTest>();
        //new InKeyValueStressTest().Run();
        //BigBonTest.Run();
        //var cts = new CancellationTokenSource();
        //Console.CancelKeyPress += delegate { cts.Cancel(); };
        //new AutoSplitSizeTest().Run("/tmp/db", cts.Token);
        //BenchmarkRunner.Run<BenchLockTest>();
        //new GatherSpeedTest().Run();
    }
}
