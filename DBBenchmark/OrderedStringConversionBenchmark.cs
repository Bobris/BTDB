using System;
using BenchmarkDotNet.Attributes;
using BTDB.StreamLayer;

namespace DBBenchmark;

// Isolates conversion from BTree/log costs. Both variants reuse pinned input/output buffers.
[MemoryDiagnoser]
[ShortRunJob]
public class OrderedStringConversionBenchmark
{
    [Params(24, 8192)] public int Length { get; set; }
    [Params(false, true)] public bool MixedUnicode { get; set; }
    byte[] _input = null!;
    byte[] _output = null!;

    [GlobalSetup]
    public void Setup()
    {
        const string pattern = "Příliš žluťoučký 😀\0\u007f";
        var chars = new char[Length];
        for (var i = 0; i < chars.Length; i++) chars[i] = MixedUnicode ? pattern[i % pattern.Length] : 'a';
        var writer = new MemWriter();
        writer.WriteString(new string(chars));
        _input = GC.AllocateUninitializedArray<byte>(writer.GetSpan().Length, pinned: true);
        writer.GetSpan().CopyTo(_input);
        _output = GC.AllocateUninitializedArray<byte>(Length * 4 + 8, pinned: true);
    }

    [Benchmark(Baseline = true)]
    public uint MaterializeString()
    {
        var reader = MemReader.CreateFromPinnedSpan(_input);
        var writer = MemWriter.CreateFromPinnedSpan(_output);
        writer.WriteStringOrdered(reader.ReadString());
        return writer.NoControllerGetCurrentPosition();
    }

    [Benchmark]
    public uint Fused()
    {
        var reader = MemReader.CreateFromPinnedSpan(_input);
        var writer = MemWriter.CreateFromPinnedSpan(_output);
        reader.CopyStringToOrdered(ref writer);
        return writer.NoControllerGetCurrentPosition();
    }
}
