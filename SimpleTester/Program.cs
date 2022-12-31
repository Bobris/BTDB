
//using JetBrains.Profiler.Windows.Api;

using System;
using System.Threading;
using BenchmarkDotNet.Running;

namespace SimpleTester;

static class Program
{
    static void Main(string[] args)
    {
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
        //new KeyValueSpeedTest(KVType.BTree, false, true).Run();
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
        BenchmarkRunner.Run<BenchTestWriteString>();
    }
}
