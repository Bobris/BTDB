using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace SimpleTester
{
    [SimpleJob(RuntimeMoniker.NetCoreApp31, warmupCount:1, targetCount:1, launchCount:1)]
    public class BenchTest
    {

            [Params(42,420,42000,420000000)]
            public uint N;

            [Benchmark(Baseline = true)]
            public int Branchy()
            {
                var n = N;
                if (n < 0x80) return 1;
                if (n < 0x4000) return 2;
                if (n < 0x200000) return 3;
                if (n < 0x10000000) return 4;
                return 5;
            }

            [Benchmark]
            public int Branchless()
            {
                var n = N;
                if (n < 0x80) return 1;
                return (int)((32 + 6 - System.Runtime.Intrinsics.X86.Lzcnt.LeadingZeroCount(n)) / 7);
            }

            public void Verify()
            {
                N = 0;
                do
                {
                    if (Branchy()!=Branchless())
                        throw new Exception("Bad N="+N+" "+Branchy()+"!="+Branchless());
                    N++;
                } while (N!=0);
            }
    }
}
