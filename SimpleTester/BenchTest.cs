using System;
using System.Numerics;
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
        [Params(42,0xf0,0xff)]
        public uint N;

        [Benchmark(Baseline = true)]
        public int Branchy()
        {
            var first = N;
            if (first < 0x80) return 1;
            if (first < 0xC0) return 2;
            if (first < 0xE0) return 3;
            if (first < 0xF0) return 4;
            if (first < 0xF8) return 5;
            if (first < 0xFC) return 6;
            if (first < 0xFE) return 7;
            return first == 0xFE ? 8 : 9;
        }

        static ReadOnlySpan<byte> LzcToVUintLen => new byte[65]
        {
            9, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5,
            4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1
        };

        [Benchmark]
        public int Branchless()
        {
            var n = N;
            n = n ^ 0xff;
            return BitOperations.LeadingZeroCount(n)+9-32;
        }

        public void Verify()
        {
            N = 0;
            do
            {
                if (Branchy() != Branchless())
                    throw new Exception("Bad N=" + N + " " + Branchy() + "!=" + Branchless());
                N++;
            } while (N!=256);
        }
    }
}
