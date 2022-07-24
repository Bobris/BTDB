using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace SimpleTester;

[DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
[SimpleJob(RuntimeMoniker.Net60, warmupCount: 1, targetCount: 1, launchCount: 1)]
public class BenchTest
{
    [Params(0x80, 0xf0, 0)] public int N;

    [Benchmark(Baseline = true)]
    public int Branchy()
    {
        var first = (byte)N;
        if (first is >= 0x40 and < 0xC0) return 1;
        if (first is >= 0x20 and < 0xE0) return 2;
        if (first is >= 0x10 and < 0xF0) return 3;
        if (first is >= 0x08 and < 0xF8) return 4;
        if (first is >= 0x04 and < 0xFC) return 5;
        if (first is >= 0x02 and < 0xFE) return 6;
        if (first is >= 0x01 and < 0xFF) return 7;
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

[DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
[SimpleJob(RuntimeMoniker.Net60, warmupCount: 1, targetCount: 1, launchCount: 1)]
public class BenchTest2
{
    [Params(32, 1, 0)] public int N;


    static ReadOnlySpan<byte> LzcToVIntLen => new byte[33]
    {
        5, 5, 5, 5, 5, 4, 4, 4,
        4, 4, 4, 4, 3, 3, 3, 3,
        3, 3, 3, 2, 2, 2, 2, 2,
        2, 2, 1, 1, 1, 1, 1, 1,
        1
    };

    [Benchmark(Baseline = true)]
    public uint MemAccess()
    {
        return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVIntLen), (nuint)N);
    }

    [Benchmark]
    public uint JustCalc()
    {
        return ((uint)(40 - N) * 9) >> 6;
    }

    public void Verify()
    {
        N = 0;
        do
        {
            if (MemAccess() != JustCalc())
                throw new Exception("Bad N=" + N + " " + MemAccess() + "!=" + JustCalc());
            N++;
        } while (N <= 33);
    }
}

[DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
[SimpleJob(RuntimeMoniker.Net60, warmupCount: 1, targetCount: 1, launchCount: 1)]
public class BenchTest3
{
    [Params(32, 1, 0)] public int N;


    static ReadOnlySpan<byte> LzcToVUintLen => new byte[ /*65*/]
    {
        9, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5,
        4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1
    };

    [Benchmark(Baseline = true)]
    public uint MemAccess()
    {
        return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVUintLen), (nuint)N);
    }

    /* Z3 helped to solve this:
(declare-const a Int)
(declare-const b Int)
(declare-const c Int)
(declare-const d Int)
(assert (= d (* 256 8)))
(assert (> a 50))
(assert (< a 200))
(assert (> b 1))
(assert (< b d))
(assert (> c -10))
(assert (< c 1000000))
(assert (= 9 (div (+ (* (- a 0) b) c) d)))
(assert (= 9 (div (+ (* (- a 7) b) c) d)))
(assert (= 3 (div (+ (* (- a 43) b) c) d)))
(assert (= 1 (div (+ (* (- a 57) b) c) d)))
(assert (= 1 (div (+ (* (- a 64) b) c) d)))
(minimize (* c c))
(check-sat)
(get-model)
     */
    [Benchmark]
    public uint JustCalc()
    {
        return (uint)(20441 - N * 287) >> 11;
    }

    public void Verify()
    {
        N = 0;
        do
        {
            if (MemAccess() != JustCalc())
                throw new Exception("Bad N=" + N + " " + MemAccess() + "!=" + JustCalc());
            N++;
        } while (N <= 65);
    }
}

[DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
[SimpleJob(RuntimeMoniker.Net60, warmupCount: 1, targetCount: 1, launchCount: 1)]
public class BenchTest4
{
    [Params(32, 1, 0)] public int N;

    static ReadOnlySpan<byte> LzcToVUintLen => new byte[ /*33*/]
    {
        5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1
    };

    [Benchmark(Baseline = true)]
    public uint MemAccess()
    {
        return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVUintLen), (nuint)N);
    }

    /* Z3 helped to solve this:
(declare-const a Int)
(declare-const b Int)
(declare-const d Int)
(assert (= d 64))
(assert (> a 0))
(assert (< a 2000))
(assert (> b 0))
(assert (< b d))
(assert (= 5 (div (- a (* b 0)) d)))
(assert (= 5 (div (- a (* b 3)) d)))
(assert (= 4 (div (- a (* b 4)) d)))
(assert (= 1 (div (- a (* b 25)) d)))
(assert (= 1 (div (- a (* b 32)) d)))
(minimize (* b b))
(check-sat)
(get-model)
     */
    [Benchmark]
    public uint JustCalc()
    {
        return (uint)(352 - N * 9) >> 6;
    }

    public void Verify()
    {
        N = 0;
        do
        {
            if (MemAccess() != JustCalc())
                throw new Exception("Bad N=" + N + " " + MemAccess() + "!=" + JustCalc());
            N++;
        } while (N <= 32);
    }
}
