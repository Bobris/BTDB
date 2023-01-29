using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB.StreamLayer;

namespace SimpleTester;

[DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
[SimpleJob(RuntimeMoniker.HostProcess, warmupCount: 1, targetCount: 1, launchCount: 1)]
public class BenchTestSpanReaderWriter
{
    [Params(1,5,20,2000,34567)] public int N;

    string _str = "";
    Memory<byte> _buf;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _str = "";
        while (_str.Length < N)
            _str += "ABCDefgh1234!@#$";
        _str = _str[..N];
        SpanWriter writer = new();
        writer.WriteStringOrdered(_str);
        _buf = writer.GetPersistentMemoryAndReset();
    }

    /*
    [Benchmark(Baseline = true)]
    public void Original()
    {
        SpanReader reader = new(_buf);
        reader.SkipStringOrderedSlow();
    }
    */

    [Benchmark]
    public void Faster()
    {
        SpanReader reader = new(_buf);
        reader.SkipStringOrdered();
    }
}
