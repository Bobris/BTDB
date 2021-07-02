using System;
using System.Numerics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace SimpleTester
{
    [DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
    [SimpleJob(RuntimeMoniker.Net50, warmupCount: 1, targetCount: 1, launchCount: 1)]
    public class BenchTest
    {
        [Params(0x80, 0xf0, 0)] public int N;

        [Benchmark(Baseline = true)]
        public int Branchy()
        {
            var first = (byte)N;
            if (0x40 <= first && first < 0xC0) return 1;
            if (0x20 <= first && first < 0xE0) return 2;
            if (0x10 <= first && first < 0xF0) return 3;
            if (0x08 <= first && first < 0xF8) return 4;
            if (0x04 <= first && first < 0xFC) return 5;
            if (0x02 <= first && first < 0xFE) return 6;
            if (0x01 <= first && first < 0xFF) return 7;
            return 9;
        }

        [Benchmark]
        public int Branchless()
        {
            var first = (uint)N;
            first ^= (uint)((sbyte)first >> 7) & 0xff;
            var res = BitOperations.LeadingZeroCount(first) + 8 - 32;
            return (int)(0x976543211UL >> (res * 4)) & 0xf;
        }

        public void Verify()
        {
            N = 0;
            do
            {
                if (Branchy() != Branchless())
                    throw new Exception("Bad N=" + N + " " + Branchy() + "!=" + Branchless());
                N++;
            } while (N <= 0xff);
        }
    }
}
