using System;
using System.Buffers.Binary;
using System.Numerics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BTDB.Buffer;

namespace SimpleTester;

[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[MemoryDiagnoser]
[ShortRunJob]
public class RoaringBitmapsBenchmark
{
    readonly byte[] _sparseLeftBitmap = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _sparseRightBitmap = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _rleLeftBitmap = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _rleRightBitmap = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _denseLeft = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _denseRight = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _enumerateBitmap = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _sparseLeft = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _sparseRight = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _rleLeft = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _rleRight = new byte[RoaringBitmaps.BitmapSize];
    readonly byte[] _output = new byte[RoaringBitmaps.BitmapSize];
    RoaringBitmaps.RoaringBitmapEnumerable _sparseEnumerable;
    RoaringBitmaps.RoaringBitmapEnumerable _rleEnumerable;
    OldRoaringBitmapEnumerable _oldSparseEnumerable;
    OldRoaringBitmapEnumerable _oldRleEnumerable;
    int _sparseLeftLength;
    int _sparseRightLength;
    int _rleLeftLength;
    int _rleRightLength;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var random = new Random(12345);
        for (var i = 0; i < 512; i++)
        {
            RoaringBitmaps.SetBit(_sparseLeftBitmap, (ushort)random.Next(65536));
            RoaringBitmaps.SetBit(_sparseRightBitmap, (ushort)random.Next(65536));
        }

        AddRuns(_rleLeftBitmap,
            (13, 19), (71, 4), (128, 220), (601, 37), (915, 11),
            (1520, 640), (2901, 15), (4096, 93), (7333, 1200), (12000, 7),
            (18111, 321), (25000, 2048), (33777, 29), (41000, 777), (52222, 987),
            (65000, 420));
        AddRuns(_rleRightBitmap,
            (0, 33), (80, 9), (180, 260), (590, 91), (1400, 800),
            (2800, 500), (4090, 111), (7200, 1400), (11990, 41), (17777, 999),
            (25100, 1500), (33000, 900), (40960, 1024), (53000, 333), (64000, 700));

        random.NextBytes(_denseLeft);
        random.NextBytes(_denseRight);
        FillEnumerationBitmap(_enumerateBitmap);
        _sparseLeftLength = RoaringBitmaps.Compress(_sparseLeftBitmap, _sparseLeft);
        _sparseRightLength = RoaringBitmaps.Compress(_sparseRightBitmap, _sparseRight);
        _rleLeftLength = RoaringBitmaps.Compress(_rleLeftBitmap, _rleLeft);
        _rleRightLength = RoaringBitmaps.Compress(_rleRightBitmap, _rleRight);
        _sparseEnumerable = RoaringBitmaps.Enumerate(_sparseLeft.AsMemory(0, _sparseLeftLength), 123);
        _rleEnumerable = RoaringBitmaps.Enumerate(_rleLeft.AsMemory(0, _rleLeftLength), 123);
        _oldSparseEnumerable = new(_sparseLeft.AsMemory(0, _sparseLeftLength), 123);
        _oldRleEnumerable = new(_rleLeft.AsMemory(0, _rleLeftLength), 123);
    }

    [Benchmark]
    public int CompressSparseArray()
    {
        return RoaringBitmaps.Compress(_sparseLeftBitmap, _output);
    }

    [Benchmark]
    public int CompressRle()
    {
        return RoaringBitmaps.Compress(_rleLeftBitmap, _output);
    }

    [Benchmark]
    public int AndSparseArrays()
    {
        return RoaringBitmaps.And(_sparseLeft.AsSpan(0, _sparseLeftLength), _sparseRight.AsSpan(0, _sparseRightLength),
            _output);
    }

    [Benchmark]
    public int OrRleRuns()
    {
        return RoaringBitmaps.Or(_rleLeft.AsSpan(0, _rleLeftLength), _rleRight.AsSpan(0, _rleRightLength), _output);
    }

    [Benchmark]
    public int AndDenseBitmaps()
    {
        return RoaringBitmaps.And(_denseLeft, _denseRight, _output);
    }

    [Benchmark]
    public int AndNotDenseWithSparse()
    {
        return RoaringBitmaps.AndNot(_denseLeft, _sparseLeft.AsSpan(0, _sparseLeftLength), _output);
    }

    [Benchmark]
    public int NotRle()
    {
        return RoaringBitmaps.Not(_rleLeft.AsSpan(0, _rleLeftLength), _output);
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("DenseEnumeration")]
    public ulong EnumerateDenseBitmap32BitBaseline()
    {
        var result = 0ul;
        var bitmap = _enumerateBitmap.AsSpan();
        for (var index = 0; index < RoaringBitmaps.BitmapSize;)
        {
            var word = BinaryPrimitives.ReadUInt32LittleEndian(bitmap.Slice(index, sizeof(uint)));
            var bitBase = index * 8;
            index += sizeof(uint);
            while (word != 0)
            {
                var bit = BitOperations.TrailingZeroCount(word);
                word &= word - 1;
                result += 123 + (uint)(bitBase + bit);
            }
        }

        return result;
    }

    [Benchmark]
    [BenchmarkCategory("DenseEnumeration")]
    public ulong EnumerateDenseBitmap64Bit()
    {
        var result = 0ul;
        foreach (var value in RoaringBitmaps.EnumerateBitmap(_enumerateBitmap.AsSpan(), 123))
            result += value;
        return result;
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("SparseEnumeration")]
    public ulong EnumerateSparseArrayBinaryPrimitivesBaseline()
    {
        var result = 0ul;
        foreach (var value in _oldSparseEnumerable)
            result += value;
        return result;
    }

    [Benchmark]
    [BenchmarkCategory("SparseEnumeration")]
    public ulong EnumerateSparseArrayUnsafe()
    {
        var result = 0ul;
        foreach (var value in _sparseEnumerable)
            result += value;
        return result;
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("RleEnumeration")]
    public ulong EnumerateRleBinaryPrimitivesBaseline()
    {
        var result = 0ul;
        foreach (var value in _oldRleEnumerable)
            result += value;
        return result;
    }

    [Benchmark]
    [BenchmarkCategory("RleEnumeration")]
    public ulong EnumerateRleUnsafe()
    {
        var result = 0ul;
        foreach (var value in _rleEnumerable)
            result += value;
        return result;
    }

    static void AddRuns(byte[] bitmap, params ReadOnlySpan<(int Start, int Length)> runs)
    {
        foreach (var (start, length) in runs)
        {
            var end = Math.Min(start + length, 65536);
            for (var i = start; i < end; i++)
                RoaringBitmaps.SetBit(bitmap, (ushort)i);
        }
    }

    static void FillEnumerationBitmap(byte[] bitmap)
    {
        AddRuns(bitmap,
            (3, 2), (61, 9), (255, 1), (511, 27), (1024, 128),
            (4093, 17), (8191, 65), (12000, 333), (21001, 7), (32760, 96),
            (44000, 1024), (52000, 513), (61000, 900));
        for (var i = 17; i < 65536; i += 521)
            RoaringBitmaps.SetBit(bitmap, (ushort)i);
    }

    readonly struct OldRoaringBitmapEnumerable
    {
        readonly ReadOnlyMemory<byte> _input;
        readonly ulong _addOffset;

        public OldRoaringBitmapEnumerable(ReadOnlyMemory<byte> input, ulong addOffset)
        {
            _input = input;
            _addOffset = addOffset;
        }

        public OldEnumerator GetEnumerator()
        {
            return new(_input, _addOffset);
        }
    }

    struct OldEnumerator
    {
        readonly ReadOnlyMemory<byte> _input;
        readonly ulong _addOffset;
        readonly bool _rle;
        int _index;
        int _currentOffset;
        int _rleEnd;

        public OldEnumerator(ReadOnlyMemory<byte> input, ulong addOffset)
        {
            _input = input;
            _addOffset = addOffset;
            _rle = (input.Length & 1) != 0;
            _index = 0;
            _currentOffset = 1;
            _rleEnd = 0;
            Current = 0;
        }

        public ulong Current { get; private set; }

        public bool MoveNext()
        {
            var input = _input.Span;
            return _rle ? MoveNextRle(input) : MoveNextArray(input);
        }

        bool MoveNextArray(ReadOnlySpan<byte> input)
        {
            if (_index >= input.Length) return false;
            Current = _addOffset + BinaryPrimitives.ReadUInt16LittleEndian(input.Slice(_index, sizeof(ushort)));
            _index += sizeof(ushort);
            return true;
        }

        bool MoveNextRle(ReadOnlySpan<byte> input)
        {
            if (_currentOffset <= _rleEnd)
            {
                Current = _addOffset + (uint)_currentOffset++;
                return true;
            }

            if (_index >= input.Length - 1) return false;
            _currentOffset = BinaryPrimitives.ReadUInt16LittleEndian(input.Slice(_index, sizeof(ushort)));
            _rleEnd = _currentOffset + BinaryPrimitives.ReadUInt16LittleEndian(input.Slice(_index + sizeof(ushort),
                sizeof(ushort)));
            _index += 2 * sizeof(ushort);
            Current = _addOffset + (uint)_currentOffset++;
            return true;
        }
    }
}
