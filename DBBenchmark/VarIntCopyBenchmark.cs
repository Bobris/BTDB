using System;
using BenchmarkDotNet.Attributes;
using BTDB.StreamLayer;

namespace DBBenchmark;

[MemoryDiagnoser]
[ShortRunJob]
public class VarIntCopyBenchmark
{
    [Params(false, true)] public bool Signed { get; set; }
    [Params(false, true)] public bool WideValues { get; set; }
    byte[] _input = null!;
    byte[] _output = null!;

    [GlobalSetup]
    public void Setup()
    {
        var writer = new MemWriter();
        if (Signed) writer.WriteVInt64(WideValues ? long.MinValue : 42);
        else writer.WriteVUInt64(WideValues ? ulong.MaxValue : 42);
        _input = GC.AllocateUninitializedArray<byte>(writer.GetSpan().Length, pinned: true);
        writer.GetSpan().CopyTo(_input);
        _output = GC.AllocateUninitializedArray<byte>(16, pinned: true);
    }

    [Benchmark(Baseline = true)]
    public uint DecodeEncode()
    {
        var reader = MemReader.CreateFromPinnedSpan(_input);
        var writer = MemWriter.CreateFromPinnedSpan(_output);
        if (Signed) writer.WriteVInt64(reader.ReadVInt64());
        else writer.WriteVUInt64(reader.ReadVUInt64());
        return writer.NoControllerGetCurrentPosition();
    }

    [Benchmark]
    public uint Copy()
    {
        var reader = MemReader.CreateFromPinnedSpan(_input);
        var writer = MemWriter.CreateFromPinnedSpan(_output);
        if (Signed) reader.CopyVInt64ToWriter(ref writer);
        else reader.CopyVUInt64ToWriter(ref writer);
        return writer.NoControllerGetCurrentPosition();
    }
}
