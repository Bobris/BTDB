using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB.StreamLayer;

namespace SimpleTester;

[DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
[SimpleJob(RuntimeMoniker.HostProcess, warmupCount: 1, launchCount: 1)]
public class BenchTestSpanReaderWriter
{
    [Params(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 2000, 2015, 34567)]
    public int N;

    string _str = "";
    Memory<byte> _buf;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _str = "";
        while (_str.Length < N)
            _str += "ABCDefgh1234!@#$";
        _str = _str[..N];
        MemWriter writer = new();
        writer.WriteStringOrdered(_str);
        _buf = writer.GetPersistentMemoryAndReset();
    }

    /*
    [Benchmark(Baseline = true)]
    public string? Original()
    {
        SpanReader reader = new(_buf);
        return reader.ReadStringOrderedSlow();
    }
    */

    [Benchmark]
    public unsafe string? Faster()
    {
        fixed (void* _ = _buf.Span)
        {
            var reader = MemReader.CreateFromPinnedSpan(_buf.Span);
            return reader.ReadStringOrdered();
        }
    }
}
