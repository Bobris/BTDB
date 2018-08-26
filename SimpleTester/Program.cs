using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using BenchmarkDotNet.Running;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
//using JetBrains.Profiler.Windows.Api;

namespace SimpleTester
{
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
            //new KeyValueDBReplayer("bug.log").Replay();
            //new SpeedTest1().Test();
            //new ChannelSpeedTest().Run(args);
            //new RxSpeedTest().Run();
            //new ComplexServiceTest().Run();
            new KeyValueSpeedTest(true).Run();
            new KeyValueSpeedTest(false).Run();
            //new EventStorageSpeedTestAwaitable().Run();
            //new EventStorageSpeedTestDisruptor().Run();
            //new EventStorageSpeedTest().Run();
            //new RelationSpeedTest().Run();
            //BenchmarkRunner.Run<EventSerializationBenchmark>();
            //new TestCompactor().Run(new CancellationToken());
            //new CompactorLatencyMeasurement().Run();
            //new ClassGenerator().Run();
            //new EventLayer2TestWithALotOfClasses().Run();
        }
    }
}
