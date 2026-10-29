using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using BTDB.Buffer;
using BTDB.SnappyCompression;
using Xunit;

namespace BTDBTest;

public class BufferCoverageTest
{
    [Fact]
    public void FixedSizePackUnpackMatchesBinaryPrimitives()
    {
        var rnd = new Random(1);
        var buf = new byte[20];
        for (var i = 0; i < 200; i++)
        {
            var ofs = rnd.Next(12);
            var v64 = (ulong)rnd.NextInt64() ^ ((ulong)rnd.Next() << 40);
            PackUnpack.PackUInt16LE(buf, ofs, (ushort)v64);
            Assert.Equal((ushort)v64, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(ofs)));
            Assert.Equal((ushort)v64, PackUnpack.UnpackUInt16LE(buf, ofs));
            PackUnpack.PackInt16LE(buf, ofs, (short)v64);
            Assert.Equal((short)v64, PackUnpack.UnpackInt16LE(buf, ofs));
            PackUnpack.PackUInt32LE(buf, ofs, (uint)v64);
            Assert.Equal((uint)v64, PackUnpack.UnpackUInt32LE(buf, ofs));
            PackUnpack.PackInt32LE(buf, ofs, (int)v64);
            Assert.Equal((int)v64, PackUnpack.UnpackInt32LE(buf, ofs));
            Assert.Equal((int)v64, BinaryPrimitives.ReadInt32LittleEndian(buf.AsSpan(ofs)));
            PackUnpack.PackInt32BE(buf, ofs, (int)v64);
            Assert.Equal((int)v64, BinaryPrimitives.ReadInt32BigEndian(buf.AsSpan(ofs)));
            Assert.Equal((int)v64, PackUnpack.UnpackInt32BE(buf, ofs));
            PackUnpack.PackUInt64LE(buf, ofs, v64);
            Assert.Equal(v64, PackUnpack.UnpackUInt64LE(buf, ofs));
            PackUnpack.PackInt64LE(buf, ofs, (long)v64);
            Assert.Equal((long)v64, PackUnpack.UnpackInt64LE(buf, ofs));
            PackUnpack.IncrementInt64LE(buf, ofs);
            Assert.Equal((long)v64 + 1, PackUnpack.UnpackInt64LE(buf, ofs));
            PackUnpack.PackInt64BE(buf, ofs, (long)v64);
            Assert.Equal((long)v64, BinaryPrimitives.ReadInt64BigEndian(buf.AsSpan(ofs)));
            Assert.Equal((long)v64, PackUnpack.UnpackInt64BE(buf, ofs));
        }

        Assert.ThrowsAny<ArgumentException>(() => PackUnpack.PackUInt32LE(new byte[3], 0, 1));
    }

    [Fact]
    public unsafe void UnsafeUnpackVUIntFromPointer()
    {
        foreach (var v in MemReaderWriterRoundTripTest.VUInt64Boundaries)
        {
            var buf = new byte[10];
            var ofs = 0;
            PackUnpack.PackVUInt(buf, ref ofs, v);
            fixed (byte* p = buf)
            {
                Assert.Equal(v, PackUnpack.UnsafeUnpackVUInt((nuint)p));
            }
        }
    }

    [Fact]
    public void UnsafeSpanHelpers()
    {
        ReadOnlySpan<byte> ro = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        ref var first = ref PackUnpack.UnsafeGetAndAdvance(ref ro, 2);
        Assert.Equal(1, first);
        Assert.Equal(7, ro.Length);
        Assert.Equal(0x06050403u, PackUnpack.UnsafeGet<uint>(ro));
        PackUnpack.UnsafeAdvance(ref ro, 3);
        Assert.Equal(new byte[] { 6, 7, 8, 9 }, ro.ToArray());

        Span<byte> rw = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        ref var b = ref PackUnpack.UnsafeGetAndAdvance(ref rw, 1);
        b = 100;
        Assert.Equal(9, rw.Length);
        ref var u = ref PackUnpack.UnsafeGetAndAdvance<ushort>(ref rw);
        Assert.Equal(0x0302, u);
        u = 0xffff;
        Assert.Equal(7, rw.Length);
        Assert.Equal(4, rw[0]);
        PackUnpack.UnsafeAdvance(ref rw, 2);
        Assert.Equal(6, rw[0]);
        Assert.Equal(5, rw.Length);
        Assert.Equal(30, PackUnpack.UnsafeAlignedGet<int>(new[] { 10, 20, 30 }, 2));
    }

    [Fact]
    public void AsciiHelpers()
    {
        Assert.True(PackUnpack.AllCharsInUInt32AreAscii(0x007F0041));
        Assert.False(PackUnpack.AllCharsInUInt32AreAscii(0x00800041));
        Assert.True(PackUnpack.AllCharsInUInt64AreAscii(0x007F0041_00200030));
        Assert.False(PackUnpack.AllCharsInUInt64AreAscii(0x01000041_00200030));
        var v32 = 0x007E0041u;
        Assert.True(PackUnpack.AllCharsInUInt32AreAsciiM1(ref v32));
        Assert.Equal(0x007F0042u, v32);
        v32 = 0x007F0041u;
        Assert.False(PackUnpack.AllCharsInUInt32AreAsciiM1(ref v32));
        v32 = 0x01000041u;
        Assert.False(PackUnpack.AllCharsInUInt32AreAsciiM1(ref v32));
        var v64 = 0x007E0041_00000030ul;
        Assert.True(PackUnpack.AllCharsInUInt64AreAsciiM1(ref v64));
        Assert.Equal(0x007F0042_00010031ul, v64);
        v64 = 0x007F0041_00000030ul;
        Assert.False(PackUnpack.AllCharsInUInt64AreAsciiM1(ref v64));
        Assert.True(PackUnpack.AllCharsInVectorAreAscii(Vector128.Create((ushort)0x7f)));
        Assert.False(PackUnpack.AllCharsInVectorAreAscii(Vector128.Create(0, 0, 0, 0, 0, 0, 0, (ushort)0x80)));
        Assert.Equal(0x44434241u, PackUnpack.NarrowFourUtf16CharsToAscii(0x0044_0043_0042_0041ul));
        var eight = PackUnpack.NarrowEightUtf16CharsToAscii(Vector128.Create((ushort)'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'));
        Assert.Equal("abcdefgh"u8.ToArray(), BitConverter.GetBytes(eight));
        Assert.Equal(Vector128.Create((ushort)2), PackUnpack.Add1Saturate(Vector128.Create((ushort)1)));
        Assert.Equal(Vector128.Create(ushort.MaxValue), PackUnpack.Add1Saturate(Vector128.Create(ushort.MaxValue)));
    }

    static (nuint Count, bool WasEnd) ReferenceDetect(ReadOnlySpan<byte> data)
    {
        for (var i = 0; i < data.Length; i++)
        {
            if (data[i] == 0) return ((nuint)i + 1, true);
            if (data[i] >= 0x80) return ((nuint)i, false);
        }

        return ((nuint)data.Length, false);
    }

    static byte[] RandomSimpleBytes(Random rnd, int length)
    {
        var res = new byte[length];
        for (var i = 0; i < length; i++)
        {
            res[i] = rnd.Next(30) switch
            {
                0 => 0,
                1 => (byte)rnd.Next(0x80, 0x100),
                _ => (byte)rnd.Next(1, 0x80)
            };
        }

        return res;
    }

    [Fact]
    public void DetectLengthOfSimpleCharactersMatchesReference()
    {
        var rnd = new Random(42);
        for (var i = 0; i < 20000; i++)
        {
            var data = RandomSimpleBytes(rnd, rnd.Next(0, 40));
            Assert.Equal(ReferenceDetect(data), PackUnpack.DetectLengthOfSimpleCharacters(data));
            var padded = data.Concat(new byte[16]).ToArray();
            var v4 = BinaryPrimitives.ReadUInt32LittleEndian(padded);
            var expected4 = ReferenceDetect(padded.AsSpan(0, 4));
            Assert.Equal(expected4, PackUnpack.DetectLengthOfSimpleCharacters(v4));
            Assert.Equal(expected4 == (4, false), PackUnpack.AreAllBytes1to127(v4));
            var v8 = BinaryPrimitives.ReadUInt64LittleEndian(padded);
            var expected8 = ReferenceDetect(padded.AsSpan(0, 8));
            Assert.Equal(expected8, PackUnpack.DetectLengthOfSimpleCharacters(v8));
            Assert.Equal(expected8 == (8, false), PackUnpack.AreAllBytes1to127(v8));
            var v16 = Vector128.Create(padded.AsSpan(0, 16));
            var expected16 = ReferenceDetect(padded.AsSpan(0, 16));
            Assert.Equal(expected16, PackUnpack.DetectLengthOfSimpleCharacters(v16));
            Assert.Equal(expected16 == (16, false), PackUnpack.AreAllBytes1to127(v16));
        }
    }

    [Fact]
    public void ReadAndExpandSimpleCharactersMatchesReferenceAndGrowsBuffer()
    {
        var rnd = new Random(7);
        for (var i = 0; i < 5000; i++)
        {
            var data = RandomSimpleBytes(rnd, rnd.Next(0, 100));
            var expected = ReferenceDetect(data);
            Span<char> buf = new char[2];
            var ofs = 1u;
            buf[0] = 'X';
            var res = PackUnpack.ReadAndExpandSimpleCharacters(data, ref buf, ref ofs);
            Assert.Equal(expected, res);
            var chars = (int)expected.Count - (expected.WasEnd ? 1 : 0);
            Assert.Equal(1 + (uint)chars, ofs);
            Assert.Equal('X', buf[0]);
            for (var j = 0; j < chars; j++)
            {
                Assert.Equal((char)(data[j] - 1), buf[1 + j]);
            }
        }
    }

    // Before the fix Expand restarted a slice at the start of its array, so [4, 5] expanded to 3 bytes read 0, 1, 2,
    // marked a buffer valid only synchronously as async safe and checked the size against the whole array.
    [Fact]
    public void ExpandKeepsStartAndAsyncSafety()
    {
        var array = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 };
        var slice = ByteBuffer.NewAsync(array, 4, 2);
        slice.Expand(3);
        Assert.Same(array, slice.Buffer);
        Assert.Equal(new byte[] { 4, 5, 6 }, slice.ToByteArray());

        var sync = ByteBuffer.NewSync(array, 1, 2);
        sync.Expand(3);
        Assert.False(sync.AsyncSafe);
        Assert.Equal(new byte[] { 1, 2, 3 }, sync.AsSyncReadOnlySpan().ToArray());

        var tail = ByteBuffer.NewAsync(array, 6, 2);
        tail.Expand(4);
        Assert.NotSame(array, tail.Buffer);
        Assert.Equal(4, tail.Length);
        Assert.True(tail.AsyncSafe);
    }

    // Before the fix appending to a buffer without an array, like default or NewAsync(null), threw.
    [Fact]
    public void ResizingAppendToBufferWithoutArray()
    {
        var appended = ByteBuffer.NewAsync((byte[]?)null).ResizingAppend(ByteBuffer.NewAsync(new byte[] { 9 }));
        Assert.Equal(new byte[] { 9 }, appended.ToByteArray());
        Assert.Equal(new byte[] { 9 }, default(ByteBuffer).ResizingAppend(ByteBuffer.NewAsync(new byte[] { 9 })).ToByteArray());
        Assert.Equal(new byte[] { 1 }, ByteBuffer.NewAsync(new byte[] { 1 }).ResizingAppend(default).ToByteArray());
    }

    [Fact]
    public void ByteBufferBasics()
    {
        var arr = new byte[] { 0, 1, 2, 3, 4, 5 };
        var async = ByteBuffer.NewAsync(arr, 1, 4);
        Assert.True(async.AsyncSafe);
        Assert.Equal(1, async.Offset);
        Assert.Equal(1, async[0]);
        async[0] = 10;
        Assert.Equal(10, arr[1]);
        arr[1] = 1;
        var sync = ByteBuffer.NewSync(arr, 1, 4);
        Assert.False(sync.AsyncSafe);
        Assert.Equal(1, sync.Offset);
        Assert.True(async == sync);
        Assert.False(async != sync);
        Assert.True(async.Equals(sync));
        Assert.True(async.Equals((object)sync));
        Assert.False(async.Equals("x"));
        Assert.Equal(async.GetHashCode(), sync.GetHashCode());
        Assert.NotEqual(async.GetHashCode(), ByteBuffer.NewAsync(new byte[] { 1, 2, 3 }).GetHashCode());
        Assert.Equal(new byte[] { 2, 3, 4 }, sync.Slice(1).ToByteArray());
        Assert.False(sync.Slice(1).AsyncSafe);
        Assert.True(async.Slice(1).AsyncSafe);
        Assert.Equal(new byte[] { 2, 3 }, sync.Slice(1, 2).ToByteArray());
        Assert.Equal(new byte[] { 2, 3 }, async.Slice(1, 2).ToByteArray());
        var safe = sync.ToAsyncSafe();
        Assert.True(safe.AsyncSafe);
        Assert.NotSame(arr, safe.Buffer);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, safe.ToByteArray());
        Assert.True(ByteBuffer.NewSync(arr, 0, 0).ToAsyncSafe().AsyncSafe);
        var toMake = ByteBuffer.NewSync(arr, 2, 2);
        toMake.MakeAsyncSafe();
        Assert.True(toMake.AsyncSafe);
        Assert.Equal(new byte[] { 2, 3 }, toMake.ToByteArray());
        var emptySync = ByteBuffer.NewSync(arr, 2, 0);
        emptySync.MakeAsyncSafe();
        Assert.True(emptySync.AsyncSafe);
        Assert.Equal(0, emptySync.Length);
        async.MakeAsyncSafe();
        Assert.Same(arr, async.Buffer);
        var segment = async.ToArraySegment();
        Assert.Same(arr, segment.Array);
        Assert.Equal(1, segment.Offset);
        Assert.Empty(ByteBuffer.NewAsync((byte[]?)null).ToArraySegment());
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, async.AsSyncReadOnlyMemory().ToArray());
        Assert.Same(arr, ByteBuffer.NewAsync(arr).ToByteArray());
        Assert.Empty(ByteBuffer.NewEmpty().ToByteArray());
        Assert.Equal(new byte[] { 1, 2 }, ByteBuffer.NewAsync(new byte[] { 1, 2 }.AsSpan()).ToByteArray());
        Assert.Equal(new byte[] { 9 }, ByteBuffer.NewSync(new byte[] { 9 }).ToByteArray());
    }

    [Fact]
    public void ByteBufferFromReadOnlyMemory()
    {
        var arr = new byte[] { 1, 2, 3 };
        var fromArray = ByteBuffer.NewAsync(new ReadOnlyMemory<byte>(arr, 1, 2));
        Assert.Same(arr, fromArray.Buffer);
        Assert.Equal(1, fromArray.Offset);
        using var manager = new NativeByteMemoryManager(3);
        manager.GetSpan().Fill(7);
        var fromNative = ByteBuffer.NewAsync((ReadOnlyMemory<byte>)manager.Memory);
        Assert.Equal(new byte[] { 7, 7, 7 }, fromNative.ToByteArray());
    }

    sealed unsafe class NativeByteMemoryManager : System.Buffers.MemoryManager<byte>
    {
        readonly byte* _ptr;
        readonly int _length;

        public NativeByteMemoryManager(int length)
        {
            _ptr = (byte*)NativeMemory.Alloc((nuint)length);
            _length = length;
        }

        public override Span<byte> GetSpan() => new(_ptr, _length);
        public override System.Buffers.MemoryHandle Pin(int elementIndex = 0) => new(_ptr + elementIndex);
        public override void Unpin() { }
        protected override void Dispose(bool disposing) => NativeMemory.Free(_ptr);
    }

    [Fact]
    public void ByteBufferResizingAppendAndExpand()
    {
        var buf = ByteBuffer.NewAsync(new byte[8], 0, 2);
        buf.AsSyncSpan().Fill(1);
        var appended = buf.ResizingAppend(ByteBuffer.NewAsync(new byte[] { 2, 3 }));
        Assert.Same(buf.Buffer, appended.Buffer);
        Assert.Equal(new byte[] { 1, 1, 2, 3 }, appended.ToByteArray());
        var grown = appended.ResizingAppend(ByteBuffer.NewAsync(Enumerable.Repeat((byte)4, 10).ToArray()));
        Assert.NotSame(buf.Buffer, grown.Buffer);
        Assert.Equal(new byte[] { 1, 1, 2, 3 }.Concat(Enumerable.Repeat((byte)4, 10)).ToArray(), grown.ToByteArray());
        var sync = ByteBuffer.NewSync(new byte[] { 5, 6 });
        var fromSync = sync.ResizingAppend(ByteBuffer.NewAsync(new byte[] { 7 }));
        Assert.True(fromSync.AsyncSafe);
        Assert.Equal(new byte[] { 5, 6, 7 }, fromSync.ToByteArray());

        var expanding = new ByteBuffer();
        expanding.Expand(10);
        Assert.Equal(10, expanding.Length);
        Assert.Equal(15, expanding.Buffer!.Length);
        var before = expanding.Buffer;
        expanding.Expand(12);
        Assert.Same(before, expanding.Buffer);
        Assert.Equal(12, expanding.Length);
        expanding.Expand(100);
        Assert.Equal(150, expanding.Buffer!.Length);
    }

    [Fact]
    public void Key20RoundTrips()
    {
        var data = Enumerable.Range(1, 30).Select(i => (byte)i).ToArray();
        var key = new ByteStructs.Key20(ByteBuffer.NewAsync(data, 3, 20));
        var target = new byte[25];
        key.FillBuffer(ByteBuffer.NewAsync(target, 2, 20));
        Assert.Equal(data.AsSpan(3, 20).ToArray(), target.AsSpan(2, 20).ToArray());
        var comparer = new ByteStructs.Key20EqualityComparer();
        var same = new ByteStructs.Key20(ByteBuffer.NewAsync(target, 2, 20));
        Assert.True(comparer.Equals(key, same));
        Assert.Equal(comparer.GetHashCode(key), comparer.GetHashCode(same));
        target[21] ^= 1;
        Assert.False(comparer.Equals(key, new ByteStructs.Key20(ByteBuffer.NewAsync(target, 2, 20))));
    }

    [Fact]
    public void Key32RoundTrips()
    {
        var data = Enumerable.Range(1, 40).Select(i => (byte)i).ToArray();
        var key = new ByteStructs.Key32(ByteBuffer.NewAsync(data, 3, 32));
        var target = new byte[36];
        key.FillBuffer(ByteBuffer.NewAsync(target, 2, 32));
        Assert.Equal(data.AsSpan(3, 32).ToArray(), target.AsSpan(2, 32).ToArray());
        var comparer = new ByteStructs.Key32EqualityComparer();
        var same = new ByteStructs.Key32(ByteBuffer.NewAsync(target, 2, 32));
        Assert.True(comparer.Equals(key, same));
        Assert.Equal(comparer.GetHashCode(key), comparer.GetHashCode(same));
        target[33] ^= 1;
        Assert.False(comparer.Equals(key, new ByteStructs.Key32(ByteBuffer.NewAsync(target, 2, 32))));
    }

    [Theory]
    [InlineData(new byte[] { }, new byte[] { }, 0)]
    [InlineData(new byte[] { 1 }, new byte[] { }, 1)]
    [InlineData(new byte[] { }, new byte[] { 1 }, -1)]
    [InlineData(new byte[] { 1, 2 }, new byte[] { 1, 3 }, -1)]
    [InlineData(new byte[] { 1, 3 }, new byte[] { 1, 2 }, 1)]
    [InlineData(new byte[] { 1, 2 }, new byte[] { 1, 2, 0 }, -1)]
    [InlineData(new byte[] { 1, 2, 0 }, new byte[] { 1, 2 }, 1)]
    [InlineData(new byte[] { 5, 6 }, new byte[] { 5, 6 }, 0)]
    public void CompareByteArrayIsLexicographic(byte[] a, byte[] b, int expected)
    {
        Assert.Equal(expected, BitArrayManipulation.CompareByteArray(a, a.Length, b, b.Length));
        Assert.Equal(Math.Sign(a.AsSpan().SequenceCompareTo(b)), expected);
    }

    [Fact]
    public unsafe void CreateReadOnlySpanFromPointer()
    {
        var data = new byte[] { 1, 2, 3 };
        fixed (byte* p = data)
        {
            Assert.Equal(new byte[] { 2, 3 }, BitArrayManipulation.CreateReadOnlySpan((nuint)(p + 1), 2).ToArray());
        }
    }

    [Fact]
    public void TreeNodeUtilsHelpers()
    {
        Assert.Throws<InvalidOperationException>(TreeNodeUtils.ThrowCursorHaveToBeValid);
        Assert.Throws<InvalidOperationException>(TreeNodeUtils.ThrowCursorNotWritable);
        TreeNodeUtils.AssertLittleEndian();
        Assert.Equal(4u, TreeNodeUtils.AlignUIntUpInt16(3));
        Assert.Equal(4u, TreeNodeUtils.AlignUIntUpInt32(3));
        Assert.Equal(8u, TreeNodeUtils.AlignUIntUpInt64(1));
        Assert.Equal(16, (int)TreeNodeUtils.AlignPtrUpInt64(9));
        Assert.Equal(12, (int)TreeNodeUtils.AlignPtrUpInt32(9));
        Assert.Equal(10, (int)TreeNodeUtils.AlignPtrUpInt16(9));
        var rnd = new Random(3);
        for (var i = 0; i < 2000; i++)
        {
            var a = RandomPrefixBytes(rnd);
            var b = RandomPrefixBytes(rnd);
            var c = RandomPrefixBytes(rnd);
            var d = RandomPrefixBytes(rnd);
            var ab = a.Concat(b).ToArray();
            var cd = c.Concat(d).ToArray();
            var expected = ab.AsSpan().CommonPrefixLength(cd);
            Assert.Equal(expected, TreeNodeUtils.FindFirstDifference(a.AsSpan(), b.AsSpan(), c.AsSpan(), d.AsSpan()));
            Assert.Equal(ab.AsSpan().CommonPrefixLength(c.Concat(d).ToArray()),
                TreeNodeUtils.FindFirstDifference(ab, c, d));
        }
    }

    static byte[] RandomPrefixBytes(Random rnd)
    {
        var len = rnd.Next(0, 5);
        var res = new byte[len];
        for (var i = 0; i < len; i++) res[i] = (byte)rnd.Next(2);
        return res;
    }

    [Fact]
    public void CalcCommonPrefix()
    {
        Span<byte[]> arrays = new[] { new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 4 }, new byte[] { 1, 2 } };
        Assert.Equal(2u, TreeNodeUtils.CalcCommonPrefix(arrays));
        Span<byte[]> noCommon = new[] { new byte[] { 1 }, new byte[] { 2 } };
        Assert.Equal(0u, TreeNodeUtils.CalcCommonPrefix(noCommon));
        Span<ByteBuffer> buffers = new[] { ByteBuffer.NewAsync(new byte[] { 5, 6, 7 }), ByteBuffer.NewAsync(new byte[] { 5, 6, 7, 8 }) };
        Assert.Equal(3u, TreeNodeUtils.CalcCommonPrefix(buffers));
        Span<ByteBuffer> noCommonBuffers = new[] { ByteBuffer.NewAsync(new byte[] { 5 }), ByteBuffer.NewAsync(new byte[] { 6 }) };
        Assert.Equal(0u, TreeNodeUtils.CalcCommonPrefix(noCommonBuffers));
    }

    [Fact]
    public unsafe void UnmanagedMemoryManagerWrapsPointer()
    {
        var data = GC.AllocateArray<int>(4, pinned: true);
        data[2] = 42;
        var fromSpan = new UnmanagedMemoryManager<int>(data.AsSpan());
        Assert.Equal(42, fromSpan.Memory.Span[2]);
        var ptr = (int*)Unsafe.AsPointer(ref data[0]);
        var fromPointer = new UnmanagedMemoryManager<int>(ptr, 4);
        Assert.Equal(42, fromPointer.GetSpan()[2]);
        var fromIntPtr = new UnmanagedMemoryManager<int>((IntPtr)ptr, 4);
        using var handle = fromIntPtr.Memory.Slice(2).Pin();
        Assert.Equal(42, *(int*)handle.Pointer);
        fromIntPtr.Unpin();
        ((IDisposable)fromIntPtr).Dispose();
    }

    [Fact]
    public void XxHash64KnownVectorsAndStreaming()
    {
        Assert.Equal(0xEF46DB3751D8E999ul, XxHash64.Hash(ReadOnlySpan<byte>.Empty));
        Assert.Equal(0x44BC2CF5AD770999ul, XxHash64.Hash("abc"u8));
        var rnd = new Random(9);
        var data = new byte[1000];
        rnd.NextBytes(data);
        foreach (var seed in new ulong[] { 0, 1, 0x123456789abcdef0 })
        {
            var oneShot = XxHash64.Hash(data, seed);
            var streaming = seed == 0 ? new XxHash64() : new XxHash64(seed);
            var pos = 0;
            while (pos < data.Length)
            {
                var len = Math.Min(rnd.Next(1, 70), data.Length - pos);
                streaming.Update(data.AsSpan(pos, len));
                pos += len;
            }

            Assert.Equal(oneShot, streaming.Digest());
        }

        Assert.NotEqual(XxHash64.Hash(data, 0), XxHash64.Hash(data, 1));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(60)]
    [InlineData(61)]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(65536)]
    [InlineData(65537)]
    [InlineData(200_000)]
    public void SnappyRoundTripsIncompressibleData(int length)
    {
        var data = new byte[length];
        new Random(length).NextBytes(data);
        AssertSnappyRoundTrip(data);
    }

    [Fact]
    public void SnappyRoundTripsHugeIncompressibleLiteral()
    {
        // literal longer than 16MB needs 4 byte length in literal tag
        var data = new byte[(1 << 24) + 100];
        new Random(1).NextBytes(data);
        AssertSnappyRoundTrip(data);
    }

    [Fact]
    public void SnappyRoundTripsCompressibleData()
    {
        var data = Enumerable.Range(0, 3_000_000).Select(i => (byte)(i % 1000 < 500 ? i % 7 : i % 251)).ToArray();
        var compressed = AssertSnappyRoundTrip(data);
        Assert.True(compressed < data.Length / 2);
        var repeated = string.Concat(Enumerable.Repeat("abcdefgh", 100)).Select(c => (byte)c).ToArray();
        AssertSnappyRoundTrip(repeated);
    }

    static int AssertSnappyRoundTrip(byte[] data)
    {
        var compressed = new byte[data.Length + data.Length / 6 + 32];
        var len = SnappyCompress.Compress(compressed, data);
        Assert.True(len > 0);
        Assert.Equal(data.Length, SnappyDecompress.DecompressedSize(compressed.AsSpan(0, len), out _));
        Assert.True(data.AsSpan().SequenceEqual(SnappyDecompress.Decompress(compressed.AsSpan(0, len))));
        Assert.True(data.AsSpan().SequenceEqual(SnappyDecompress.Decompress(ByteBuffer.NewAsync(compressed, 0, len)).AsSyncReadOnlySpan()));
        return len;
    }

    [Fact]
    public void SnappyDecompressesLiteralWithFourByteLength()
    {
        // size 5, literal tag with 4 byte length (63 << 2), length-1 = 4, "hello"
        var compressed = new byte[] { 5, 63 << 2, 4, 0, 0, 0, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' };
        Assert.Equal("hello"u8.ToArray(), SnappyDecompress.Decompress(compressed));
    }

    [Fact]
    public void SnappyDecompressesLiteralWithAllLengthEncodings()
    {
        Assert.Equal("hi"u8.ToArray(), SnappyDecompress.Decompress(new byte[] { 2, 60 << 2, 1, (byte)'h', (byte)'i' }));
        Assert.Equal("hi"u8.ToArray(), SnappyDecompress.Decompress(new byte[] { 2, 61 << 2, 1, 0, (byte)'h', (byte)'i' }));
        Assert.Equal("hi"u8.ToArray(), SnappyDecompress.Decompress(new byte[] { 2, 62 << 2, 1, 0, 0, (byte)'h', (byte)'i' }));
    }

    [Fact]
    public void SnappyCompressFailsWhenDestinationTooSmall()
    {
        var data = new byte[1000];
        new Random(5).NextBytes(data);
        Assert.Equal(-1, SnappyCompress.Compress(new byte[500], data));
        var compressible = Enumerable.Repeat((byte)'a', 100_000).ToArray();
        Assert.Equal(-1, SnappyCompress.Compress(new byte[10], compressible));
        var buffer = ByteBuffer.NewAsync(data);
        Assert.False(SnappyCompress.TryCompress(ref buffer, 50));
        var compressibleBuffer = ByteBuffer.NewAsync(compressible);
        Assert.True(SnappyCompress.TryCompress(ref compressibleBuffer, 50));
        Assert.Equal(compressible, SnappyDecompress.Decompress(compressibleBuffer).ToByteArray());
        ReadOnlySpan<byte> span = data;
        Assert.False(SnappyCompress.TryCompress(ref span, 50));
        ReadOnlySpan<byte> compressibleSpan = compressible;
        Assert.True(SnappyCompress.TryCompress(ref compressibleSpan, 50));
        Assert.Equal(compressible, SnappyDecompress.Decompress(compressibleSpan));
    }

    [Theory]
    [InlineData(new byte[] { })]
    [InlineData(new byte[] { 0x80 })]
    [InlineData(new byte[] { 0x80, 0x80 })]
    [InlineData(new byte[] { 0x80, 0x80, 0x80 })]
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x80 })]
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x10 })]
    public void SnappyRejectsInvalidSizeHeader(byte[] compressed)
    {
        Assert.Equal(-1, SnappyDecompress.DecompressedSize(compressed, out var length));
        Assert.Equal(0, length);
        Assert.Throws<InvalidDataException>(() => SnappyDecompress.Decompress(compressed));
        Assert.Throws<InvalidDataException>(() => SnappyDecompress.Decompress(ByteBuffer.NewAsync(compressed)));
    }

    [Theory]
    [InlineData(new byte[] { 2, 60 << 2 })] // missing 1 byte length
    [InlineData(new byte[] { 2, 61 << 2, 1 })] // missing 2 byte length
    [InlineData(new byte[] { 2, 62 << 2, 1, 0 })] // missing 3 byte length
    [InlineData(new byte[] { 2, 63 << 2, 1, 0, 0 })] // missing 4 byte length
    [InlineData(new byte[] { 2, 63 << 2, 0xff, 0xff, 0xff, 0xff, 1 })] // negative length
    [InlineData(new byte[] { 2, 2 << 2, (byte)'a' })] // literal longer than input
    [InlineData(new byte[] { 1, 1 << 2, (byte)'a', (byte)'b' })] // literal longer than output
    [InlineData(new byte[] { 2, 1 })] // short copy without offset byte
    [InlineData(new byte[] { 2, 2, 1 })] // long copy without both offset bytes
    [InlineData(new byte[] { 2, 3 })] // unsupported tag
    [InlineData(new byte[] { 5, 0, (byte)'a', 1, 2 })] // copy offset larger than output
    [InlineData(new byte[] { 3, 0, (byte)'a', 1, 1 })] // copy longer than output
    [InlineData(new byte[] { 3, 0, (byte)'a' })] // output not filled
    public void SnappyRejectsCorruptedData(byte[] compressed)
    {
        Assert.Throws<InvalidDataException>(() => SnappyDecompress.Decompress(compressed));
    }

    [Fact]
    public void SnappyDecompressesCopies()
    {
        // "a" literal then copy offset 1 length 4 (tag 1), then copy offset 1 length 2 (tag 2)
        var compressed = new byte[] { 7, 0, (byte)'a', 1, 1, (1 << 2) | 2, 1, 0 };
        Assert.Equal("aaaaaaa"u8.ToArray(), SnappyDecompress.Decompress(compressed));
    }
}
