using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB.StreamLayer;

namespace SimpleTester;

[DisassemblyDiagnoser(printSource: true, maxDepth: 2)]
[SimpleJob(RuntimeMoniker.HostProcess, warmupCount: 1, targetCount: 1, launchCount: 1)]
public class BenchTestWriteString
{
    [Params(1,5,20,2000,34567)] public int N;

    string _str = "";

    [GlobalSetup]
    public void GlobalSetup()
    {
        _str = "";
        while (_str.Length < N)
            _str += "ABCDefgh1234!@#$";
        _str = _str[..N];
    }

    /*
    [Benchmark(Baseline = true)]
    public int Original()
    {
        SpanWriter writer = new();
        writer.WriteStringSlow(_str);
        return writer.GetSpan().Length;
    }
    */

    [Benchmark]
    public int Faster()
    {
        SpanWriter writer = new();
        writer.WriteString(_str);
        return writer.GetSpan().Length;
    }
}
