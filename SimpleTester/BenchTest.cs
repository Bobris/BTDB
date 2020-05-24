using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace SimpleTester
{
    [DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
    [SimpleJob(RuntimeMoniker.NetCoreApp31, warmupCount: 1, targetCount: 1, launchCount: 1)]
    public class BenchTest
    {
        [Params(42, 420, 42000, 420000000)] public uint N;

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

        static readonly IntPtr LzcToVUintLen;

        static BenchTest()
        {
            LzcToVUintLen = Marshal.AllocHGlobal(33);
            for (var i = 0; i < 33; i++)
            {
                Marshal.WriteByte(LzcToVUintLen, i, (byte) Math.Max(1, (32 + 6 - i) / 7));
            }
        }

        [Benchmark]
        public unsafe int Branchless()
        {
            var n = N;
            return ((byte *)LzcToVUintLen)[(int) System.Runtime.Intrinsics.X86.Lzcnt.LeadingZeroCount(n)];
        }

        public void Verify()
        {
            N = 0;
            do
            {
                if (Branchy() != Branchless())
                    throw new Exception("Bad N=" + N + " " + Branchy() + "!=" + Branchless());
                N++;
            } while (N != 0);
        }
    }
}
