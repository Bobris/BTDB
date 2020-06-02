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
        [Params(42,0xf000,-0xfffffff,0x7fff_ffff_ffff_ffffL)]
        public long N;

        [Benchmark(Baseline = true)]
        public int Branchy()
        {
            var value = N;
            if (-0x40 <= value && value < 0x40) return 1;
            if (-0x2000 <= value && value < 0x2000) return 2;
            if (-0x100000 <= value && value < 0x100000) return 3;
            if (-0x08000000 <= value && value < 0x08000000) return 4;
            if (-0x0400000000 <= value && value < 0x0400000000) return 5;
            if (-0x020000000000 <= value && value < 0x020000000000) return 6;
            if (-0x01000000000000 <= value && value < 0x01000000000000) return 7;
            return 9;
        }

        static ReadOnlySpan<byte> LzcToVIntLen => new byte[65]
        {
            9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5,
            4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1
        };

        [Benchmark]
        public int Branchless()
        {
            var n = (ulong)N;
            n ^= (ulong)((long) n >> 63);
            return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVIntLen),
                (IntPtr) BitOperations.LeadingZeroCount(n));
        }

        public void Verify()
        {
            N = -0xffff_ffffL;
            do
            {
                if (Branchy() != Branchless())
                    throw new Exception("Bad N=" + N + " " + Branchy() + "!=" + Branchless());
                N+=0x70;
            } while (N<0xffff_ffffL);
        }
    }
}
