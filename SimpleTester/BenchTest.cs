using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB.Buffer;

namespace SimpleTester;

[DisassemblyDiagnoser(printSource: true, maxDepth: 3)]
[SimpleJob(RuntimeMoniker.Net80, warmupCount: 1, launchCount: 1)]
public class BenchTest
{
    [Params(0, 30, 1024 * 1024)] public int N;

    Memory<byte> _buf;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _buf = new byte[N];
        Random.Shared.NextBytes(_buf.Span);
    }

    [Benchmark(Baseline = true)]
    public ulong MyOptimized()
    {
        return XxHash64.Hash(_buf.Span);
    }

    [Benchmark]
    public ulong MyStreaming()
    {
        var hash = new XxHash64();
        hash.Update(_buf.Span);
        return hash.Digest();
    }

    [Benchmark]
    public ulong ThirdParty()
    {
        return XXHash.Managed.XXHash64.XXH64(_buf.Span, 0);
    }
}
