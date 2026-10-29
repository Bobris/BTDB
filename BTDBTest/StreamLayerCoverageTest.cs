using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class StreamLayerCoverageTest
{
    static byte[] Bytes(MemReaderWriterRoundTripTest.WriteAction action)
    {
        var writer = new MemWriter();
        action(ref writer);
        return writer.GetSpan().ToArray();
    }

    static MemReader Reader(byte[] data) => MemReader.CreateFromReadOnlyMemory(data);

    // Reading helpers which take MemReader by value so they can be used inside lambdas
    static string? ReadString(MemReader r) => r.ReadString();
    static void SkipString(MemReader r) => r.SkipString();
    static string? ReadStringOrdered(MemReader r) => r.ReadStringOrdered();
    static void SkipStringOrdered(MemReader r) => r.SkipStringOrdered();

    static string ReadStringOrderedAsSpan(MemReader r)
    {
        Span<char> buf = stackalloc char[8];
        return r.ReadStringOrderedAsSpan(ref MemoryMarshal.GetReference(buf), buf.Length).ToString();
    }

    static void CopyStringToOrdered(MemReader r)
    {
        var w = new MemWriter();
        r.CopyStringToOrdered(ref w);
    }

    [Fact]
    public void ReadStringWithSurrogatePairExceedingDeclaredLengthThrows()
    {
        // declared length 1 UTF-16 char, but content is code point needing surrogate pair (2 chars)
        var data = Bytes((ref MemWriter w) =>
        {
            w.WriteVUInt32(2);
            w.WriteVUInt32(0x1F600);
            w.WriteString(new string('x', 50));
        });
        Assert.Throws<InvalidDataException>(() => ReadString(Reader(data)));
        Assert.Throws<InvalidDataException>(() => SkipString(Reader(data)));
        Assert.Throws<InvalidDataException>(() => CopyStringToOrdered(Reader(data)));
    }

    [Fact]
    public void ReadStringValidatesUnicodeAndLength()
    {
        var tooBigChar = Bytes((ref MemWriter w) =>
        {
            w.WriteVUInt32(3);
            w.WriteVUInt32(0x110000);
            w.WriteUInt8(65);
        });
        Assert.Throws<InvalidDataException>(() => ReadString(Reader(tooBigChar)));
        Assert.Throws<InvalidDataException>(() => SkipString(Reader(tooBigChar)));
        Assert.Throws<InvalidDataException>(() => CopyStringToOrdered(Reader(tooBigChar)));
        var tooLong = Bytes((ref MemWriter w) => w.WriteVUInt64((ulong)int.MaxValue + 2));
        Assert.Throws<InvalidDataException>(() => ReadString(Reader(tooLong)));
        Assert.Throws<InvalidDataException>(() => SkipString(Reader(tooLong)));
        Assert.Throws<InvalidDataException>(() => CopyStringToOrdered(Reader(tooLong)));
        Assert.Throws<InvalidDataException>(() => ReadStringInUtf8(Reader(tooLong)));
        Assert.Throws<InvalidDataException>(() => SkipStringInUtf8(Reader(tooLong)));
    }

    static string ReadStringInUtf8(MemReader r) => r.ReadStringInUtf8();
    static void SkipStringInUtf8(MemReader r) => r.SkipStringInUtf8();

    [Fact]
    public void ReadStringOrderedValidatesUnicode()
    {
        // first character not simple
        var firstBad = Bytes((ref MemWriter w) => w.WriteVUInt32(0x110002));
        Assert.Throws<InvalidDataException>(() => ReadStringOrdered(Reader(firstBad)));
        Assert.Throws<InvalidDataException>(() => ReadStringOrderedAsSpan(Reader(firstBad)));
        Assert.Throws<InvalidDataException>(() => SkipStringOrdered(Reader(firstBad)));
        // later character not valid
        var laterBad = Bytes((ref MemWriter w) =>
        {
            w.WriteStringOrderedPrefix("abĀ");
            w.WriteVUInt32(0x110005);
            w.WriteByteZero();
        });
        Assert.Throws<InvalidDataException>(() => ReadStringOrdered(Reader(laterBad)));
        Assert.Throws<InvalidDataException>(() => ReadStringOrderedAsSpan(Reader(laterBad)));
        Assert.Throws<InvalidDataException>(() => SkipStringOrdered(Reader(laterBad)));
        // null is encoded as 0x110001
        var nullString = Bytes((ref MemWriter w) => w.WriteStringOrdered(null));
        Assert.Null(ReadStringOrdered(Reader(nullString)));
        Assert.Equal("", ReadStringOrderedAsSpan(Reader(nullString)));
        var r = Reader(nullString);
        r.SkipStringOrdered();
        Assert.True(r.Eof);
    }

    [Theory]
    [InlineData("😀")]
    [InlineData("😀abc")]
    [InlineData("Ā😀")]
    [InlineData("ĀāĂ and long ascii continuation to grow buffer beyond initial size 0123456789")]
    public void ReadStringOrderedStartingWithNonSimpleCharacters(string value)
    {
        var data = Bytes((ref MemWriter w) =>
        {
            w.WriteStringOrdered(value);
            w.WriteUInt8(0xEE);
        });
        var r = Reader(data);
        Assert.Equal(value, r.ReadStringOrdered());
        Assert.Equal(0xEE, r.ReadUInt8());
        Assert.Equal(value, ReadStringOrderedAsSpan(Reader(data)));
        r = Reader(data);
        r.SkipStringOrdered();
        Assert.Equal(0xEE, r.ReadUInt8());
    }

    [Fact]
    public void ReadStringOrderedGrowsBufferForLongNonAsciiStrings()
    {
        var value = string.Concat(Enumerable.Range(0, 2000).Select(i => i % 3 == 0 ? "😀" : ((char)(0x100 + i)).ToString()));
        var data = Bytes((ref MemWriter w) => w.WriteStringOrdered(value));
        Assert.Equal(value, ReadStringOrdered(Reader(data)));
        Assert.Equal(value, ReadStringOrderedAsSpan(Reader(data)));
    }

    [Fact]
    public void VarIntOverflowsAreDetected()
    {
        var big = Bytes((ref MemWriter w) => w.WriteVInt64(long.MaxValue));
        var bigU = Bytes((ref MemWriter w) => w.WriteVUInt64(ulong.MaxValue));
        var negative = Bytes((ref MemWriter w) => w.WriteVInt64(long.MinValue));
        Assert.Throws<InvalidDataException>(() => Reader(big).ReadVInt16());
        Assert.Throws<InvalidDataException>(() => Reader(negative).ReadVInt16());
        Assert.Throws<InvalidDataException>(() => Reader(big).SkipVInt16());
        Assert.Throws<InvalidDataException>(() => Reader(big).ReadVInt32());
        Assert.Throws<InvalidDataException>(() => Reader(negative).ReadVInt32());
        Assert.Throws<InvalidDataException>(() => Reader(negative).SkipVInt32());
        Assert.Throws<InvalidDataException>(() => Reader(bigU).ReadVUInt16());
        Assert.Throws<InvalidDataException>(() => Reader(bigU).SkipVUInt16());
        Assert.Throws<InvalidDataException>(() => Reader(bigU).ReadVUInt32());
        Assert.Throws<InvalidDataException>(() => Reader(bigU).SkipVUInt32());
    }

    [Fact]
    public void UnknownIPAddressTypeThrows()
    {
        Assert.Throws<InvalidDataException>(() => Reader([4]).ReadIPAddress());
        Assert.Throws<InvalidDataException>(() => Reader([4]).SkipIPAddress());
    }

    [Fact]
    public unsafe void ToStringShowsHexAroundCurrentPosition()
    {
        var shortData = new byte[] { 1, 2, 0xAB };
        fixed (byte* p = shortData)
        {
            var r = new MemReader(p, shortData.Length);
            r.ReadUInt8();
            Assert.Equal("01|02AB", r.ToString());
        }

        var longData = Enumerable.Range(0, 200).Select(i => (byte)i).ToArray();
        fixed (byte* p = longData)
        {
            var r = new MemReader((nint)p, longData.Length);
            r.SkipBlock(10);
            Assert.Equal(Convert.ToHexString(longData, 0, 10) + "|" + Convert.ToHexString(longData, 10, 50),
                r.ToString());
            r.SkipBlock(100);
            Assert.Equal(Convert.ToHexString(longData, 0, 20) + "..." + Convert.ToHexString(longData, 90, 20) + "|" +
                         Convert.ToHexString(longData, 110, 50), r.ToString());
            r.SkipBlock(80);
            Assert.EndsWith("|" + Convert.ToHexString(longData, 190, 10), r.ToString());
        }
    }

    [Fact]
    public void UnreadByteWorksInsideBufferAndAtBufferStart()
    {
        var data = Enumerable.Range(0, 20).Select(i => (byte)i).ToArray();
        var r = Reader(data);
        r.ReadUInt8();
        r.ReadUInt8();
        r.UnreadByte();
        Assert.Equal(1, r.ReadUInt8());

        var stream = new MemoryPositionLessStream();
        stream.Write(data, 0);
        var controlled = new MemReader(new PositionLessStreamReader(stream, 4));
        controlled.SkipBlock(8);
        Assert.Equal(8, controlled.GetCurrentPosition());
        controlled.UnreadByte();
        Assert.Equal(7, controlled.GetCurrentPosition());
        Assert.Equal(7, controlled.ReadUInt8());
        Assert.Equal(8, controlled.ReadUInt8());
    }

    [Fact]
    public void CheckMagicWithoutController()
    {
        var data = "MAGICrest"u8.ToArray();
        var r = Reader(data);
        Assert.False(r.CheckMagic("MAGIX"u8));
        Assert.Equal(0, r.GetCurrentPosition());
        Assert.True(r.CheckMagic("MAGIC"u8));
        Assert.Equal(5, r.GetCurrentPosition());
        Assert.False(r.CheckMagic("toolongmagic"u8));
        var pinned = GC.AllocateArray<byte>(3, pinned: true);
        var plain = MemReader.CreateFromPinnedArray(pinned, 0, 3);
        Assert.False(plain.CheckMagic("1234"u8));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(100)]
    public void CheckMagicWithController(int bufferSize)
    {
        var data = "abMAGICrest"u8.ToArray();
        var stream = new MemoryPositionLessStream();
        stream.Write(data, 0);
        var r = new MemReader(new PositionLessStreamReader(stream, bufferSize));
        r.Skip2Bytes();
        Assert.False(r.CheckMagic("MAGIX"u8));
        Assert.Equal(2, r.GetCurrentPosition());
        Assert.True(r.CheckMagic("MAGIC"u8));
        Assert.Equal(7, r.GetCurrentPosition());
        Assert.False(r.CheckMagic("restAndMore"u8));
        Assert.Equal('r', (char)r.ReadUInt8());
    }

    [Fact]
    public void SetCurrentPositionValidatesRange()
    {
        var r = Reader([1, 2, 3]);
        Assert.Throws<ArgumentOutOfRangeException>(() => SetPosition(r, -1));
        var plain = MemReader.CreateFromPinnedArray(GC.AllocateArray<byte>(3, pinned: true), 0, 3);
        Assert.Throws<EndOfStreamException>(() => SetPosition(plain, 4));
        plain.SetCurrentPosition(3);
        Assert.True(plain.Eof);
        Assert.Throws<EndOfStreamException>(() => SetPositionWithoutController(plain, 4));
        plain.SetCurrentPositionWithoutController(1);
        Assert.Equal(1ul, plain.GetCurrentPositionWithoutController());
        Assert.Equal(3, plain.GetLength());
        new MemorizedPosition(2).Restore(ref plain);
        Assert.Equal(2, plain.GetCurrentPosition());
    }

    static void SetPosition(MemReader r, long pos) => r.SetCurrentPosition(pos);
    static void SetPositionWithoutController(MemReader r, ulong pos) => r.SetCurrentPositionWithoutController(pos);

#if DEBUG
    [Fact]
    public void MethodsRequiringSimpleReaderThrowWithStreamingController()
    {
        var stream = new MemoryPositionLessStream();
        stream.Write(new byte[] { 1, 2, 3 }, 0);
        var r = new MemReader(new PositionLessStreamReader(stream));
        Assert.Throws<BTDBException>(() => GetPositionWithoutController(r));
    }

    static ulong GetPositionWithoutController(MemReader r) => r.GetCurrentPositionWithoutController();
#endif

    [Fact]
    public void InitHelpersReuseOrReplaceBuffers()
    {
        var reader = new MemReader();
        Assert.True(reader.Eof);
        MemReader.InitFromReadOnlyMemory(ref reader, new byte[] { 1, 2 });
        Assert.IsType<ReadOnlyMemoryMemReader>(reader.Controller);
        Assert.Equal(0x0201, reader.ReadUInt16LE());
        MemReader.InitFromReadOnlyMemory(ref reader, new byte[] { 3, 4, 5 });
        Assert.Equal(3, reader.ReadUInt8());
        Assert.Equal(new byte[] { 4, 5 }, reader.PeekSpanTillEof().ToArray());
        reader.Dispose();
        Assert.Null(reader.Controller);

        var span = MemReader.InitFromLen(ref reader, 4);
        Assert.Equal(4, span.Length);
        Assert.IsType<byte[]>(reader.Controller);
        "abcd"u8.CopyTo(span);
        Assert.Equal((byte)'a', reader.ReadUInt8());
        var array = (byte[])reader.Controller!;
        span = MemReader.InitFromLen(ref reader, 2);
        Assert.Same(array, reader.Controller);
        Assert.Equal(2, span.Length);
        Assert.Equal((byte)'a', reader.ReadUInt8());
        Assert.Equal(1, reader.PeekSpanTillEof().Length);
        span = MemReader.InitFromLen(ref reader, 10);
        Assert.NotSame(array, reader.Controller);
        Assert.Equal(10, span.Length);
        MemReader.InitFromSpan(ref reader, "xyz"u8);
        Assert.Equal("xyz"u8.ToArray(), reader.ReadByteArrayRawTillEof());

        var pinned = GC.AllocateArray<byte>(10, pinned: true);
        var plain = MemReader.CreateFromPinnedSpan(pinned);
        plain.Controller = null;
        span = MemReader.InitFromLen(ref plain, 5);
        Assert.Equal(5, plain.GetLength());
        Assert.Null(plain.Controller);
        span = MemReader.InitFromLen(ref plain, 20);
        Assert.Equal(20, plain.GetLength());
        span = MemReader.InitFromLen(ref reader, 0);
        Assert.Equal(0, span.Length);
        reader = MemReader.CreateFromReadOnlyMemory(new byte[] { 1 });
        span = MemReader.InitFromLen(ref reader, 0);
        Assert.Equal(0, span.Length);
        Assert.True(reader.Eof);
    }

    [Fact]
    public void AsReadOnlyMemoryAndRawTillEof()
    {
        var pinned = GC.AllocateArray<byte>(6, pinned: true);
        "abcdef"u8.CopyTo(pinned);
        var r = MemReader.CreateFromPinnedArray(pinned, 1, 5);
        r.ReadUInt8();
        var mem = r.AsReadOnlyMemory();
        Assert.Equal("cdef"u8.ToArray(), mem.ToArray());
        Assert.True(MemoryMarshal.TryGetArray(mem, out var segment));
        Assert.Same(pinned, segment.Array);
        var mem2 = r.ReadByteArrayRawTillEofAsMemory();
        Assert.Equal("cdef"u8.ToArray(), mem2.ToArray());
        Assert.True(r.Eof);

        var plain = MemReader.CreateFromPinnedSpan(pinned);
        plain.Skip4Bytes();
        Assert.Equal("ef"u8.ToArray(), plain.AsReadOnlyMemory().ToArray());
        Assert.Equal("ef"u8.ToArray(), plain.ReadByteArrayRawTillEofAsMemory().ToArray());
        Assert.True(plain.Eof);

        var withLength = MemReader.CreateFromPinnedArray(pinned, 0, 6);
        Assert.Throws<EndOfStreamException>(() => ReadByteArrayAsMemoryCopy(withLength, [10, 1, 2]));
    }

    static ReadOnlyMemory<byte> ReadByteArrayAsMemoryCopy(MemReader r, byte[] content)
    {
        var pinned = GC.AllocateArray<byte>(content.Length, pinned: true);
        content.CopyTo(pinned, 0);
        r = MemReader.CreateFromPinnedArray(pinned, 0, content.Length);
        return r.ReadByteArrayAsMemory();
    }

    [Fact]
    public void ByteArrayAsMemoryOfPinnedArrayDoesNotCopy()
    {
        var pinned = GC.AllocateArray<byte>(5, pinned: true);
        new byte[] { 4, 1, 2, 3, 9 }.CopyTo(pinned, 0);
        var r = MemReader.CreateFromPinnedArray(pinned, 0, 5);
        var mem = r.ReadByteArrayAsMemory();
        Assert.Equal(new byte[] { 1, 2, 3 }, mem.ToArray());
        Assert.True(MemoryMarshal.TryGetArray(mem, out var segment));
        Assert.Same(pinned, segment.Array);
        Assert.Equal(9, r.ReadUInt8());
        r = MemReader.CreateFromPinnedArray(pinned, 0, 5);
        Assert.Equal(new byte[] { 1, 2, 3 }, r.ReadByteArrayAsSpan().ToArray());
    }

    [Fact]
    public void CopyAbsoluteAndFromPosToWriter()
    {
        var r = Reader("0123456789"u8.ToArray());
        var w = new MemWriter();
        r.CopyAbsoluteToWriter(2, 3, ref w);
        Assert.Equal("234"u8.ToArray(), w.GetSpan().ToArray());
        Assert.Throws<ArgumentOutOfRangeException>(() => CopyAbsolute(r, 8, 3));
        r.SkipBlock(6);
        w = new();
        r.CopyFromPosToWriter(4, ref w);
        Assert.Equal("45"u8.ToArray(), w.GetSpan().ToArray());

        var src = new MemWriter();
        src.WriteBlock("abcdef"u8);
        w = new();
        src.CopyAbsoluteToWriter(1, 2, ref w);
        Assert.Equal("bc"u8.ToArray(), w.GetSpan().ToArray());
        Assert.Throws<ArgumentOutOfRangeException>(() => CopyAbsolute(src, 5, 300));
    }

    static void CopyAbsolute(MemReader r, uint start, uint len)
    {
        var w = new MemWriter();
        r.CopyAbsoluteToWriter(start, len, ref w);
    }

    static void CopyAbsolute(MemWriter src, uint start, uint len)
    {
        var w = new MemWriter();
        src.CopyAbsoluteToWriter(start, len, ref w);
    }

    [Fact]
    public void ReadOnlySequenceReaderEdgeCases()
    {
        using (var empty = new ReadOnlySequenceMemReader(System.Buffers.ReadOnlySequence<byte>.Empty))
        {
            var r = new MemReader(empty);
            Assert.True(r.Eof);
            Assert.Equal(0, r.GetCurrentPosition());
            Assert.Throws<EndOfStreamException>(() => ReadUInt8(r));
        }

        var sequence = MemReaderWriterRoundTripTest.CreateSequence(Enumerable.Range(0, 10).Select(i => (byte)i).ToArray(), _ => 4);
        using (var controller = new ReadOnlySequenceMemReader(sequence))
        {
            var r = new MemReader(controller);
            Assert.Throws<NotSupportedException>(() => SetPosition(r, 1));
            r.SkipBlock(9);
            Assert.Equal(9, r.GetCurrentPosition());
            Assert.Throws<EndOfStreamException>(() => SkipBlock(r, 2));
        }
    }

    static byte ReadUInt8(MemReader r) => r.ReadUInt8();
    static void SkipBlock(MemReader r, uint len) => r.SkipBlock(len);

    [Fact]
    public void MemoryMappedReaderEdgeCases()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            File.WriteAllBytes(fileName, [5, 1, 2, 3, 4, 5, 6, 7]);
            using var controller = new MemoryMappedMemReader(fileName);
            var r = new MemReader(controller);
            Assert.Equal(8, r.GetLength());
            var mem = r.ReadByteArrayAsMemory();
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, mem.ToArray());
            Assert.Equal(5, r.GetCurrentPosition());
            r.SetCurrentPosition(7);
            Assert.Equal(7, r.ReadUInt8());
            Assert.Throws<ArgumentOutOfRangeException>(() => SetPosition(r, 9));
            r.SetCurrentPosition(0);
            Assert.Throws<EndOfStreamException>(() => ReadByteArrayAsMemory(r));
            r.SetCurrentPosition(8);
            Assert.Throws<EndOfStreamException>(() => ReadUInt8(r));
            Assert.Throws<EndOfStreamException>(() => SkipBlock(r, 1));
            r.SetCurrentPosition(1);
            Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, r.ReadByteArrayRawTillEof());
        }
        finally
        {
            File.Delete(fileName);
        }
    }

    static ReadOnlyMemory<byte> ReadByteArrayAsMemory(MemReader r)
    {
        r.SetCurrentPosition(7); // value 7 means length 6
        return r.ReadByteArrayAsMemory();
    }

    [Fact]
    public void ReadOnlyMemoryReaderEdgeCases()
    {
        var r = MemReader.CreateFromReadOnlyMemory(new byte[] { 10, 1, 2 });
        Assert.Throws<EndOfStreamException>(() => r.ReadByteArrayAsMemory());
        r.SetCurrentPosition(2);
        Assert.Equal(2, r.ReadUInt8());
        Assert.Throws<EndOfStreamException>(() => ReadUInt8(r));
        Assert.Throws<EndOfStreamException>(() => SkipBlock(r, 1));
        r.SetCurrentPosition(1);
        Assert.Throws<EndOfStreamException>(() => ReadBlock(r, 5));
        Assert.Equal(3, r.GetLength());
        r.Dispose();
        r.Dispose();
    }

    static byte[] ReadBlock(MemReader r, int len) => r.ReadByteArrayRaw(len);

    [Fact]
    public void PositionLessStreamReaderEdgeCases()
    {
        var stream = new MemoryPositionLessStream();
        Assert.Throws<ArgumentOutOfRangeException>(() => new PositionLessStreamReader(stream, 0));
        stream.Write(Enumerable.Range(0, 100).Select(i => (byte)i).ToArray(), 0);
        var r = new MemReader(new PositionLessStreamReader(stream, 10));
        r.SetCurrentPosition(50);
        Assert.Equal(50, r.ReadUInt8());
        r.SkipBlock(48);
        Assert.Equal(99, r.ReadUInt8());
        Assert.True(r.Eof);
        r.SetCurrentPosition(90);
        Assert.Throws<EndOfStreamException>(() => SkipBlock(r, 20));
        r.SetCurrentPosition(95);
        Assert.Throws<EndOfStreamException>(() => ReadBlock(r, 30));
        r.SetCurrentPosition(95);
        Assert.Throws<EndOfStreamException>(() => ReadBlock(r, 7));
    }

    [Fact]
    public void MemWriterBasicHelpers()
    {
        var w = new MemWriter();
        Assert.Throws<InvalidOperationException>(() => Flush(w));
        w.WriteBlock("abc"u8.ToArray(), 1, 2);
        Assert.Equal("bc"u8.ToArray(), w.GetSpan().ToArray());
        Assert.Equal("bc"u8.ToArray(), w.GetPersistentSpanAndReset().ToArray());
        Assert.Equal(0, w.GetCurrentPosition());
        w.WriteUInt8(1);
        w.WriteUInt8(2);
        var persistent = w.GetPersistentMemoryAndReset();
        Assert.Equal(new byte[] { 1, 2 }, persistent.ToArray());
        Assert.Null(w.Controller);
        Assert.Equal(0, w.GetCurrentPosition());

        Span<byte> stack = stackalloc byte[16];
        var sw = MemWriter.CreateFromStackAllocatedSpan(stack);
        sw.WriteUInt8(7);
        var copy = sw.GetPersistentMemoryAndReset();
        Assert.Equal(new byte[] { 7 }, copy.ToArray());
        sw.WriteUInt8(8);
        sw.WriteUInt8(9);
        Assert.Equal(new byte[] { 8, 9 }, sw.GetScopedSpanAndReset().ToArray());
        Assert.Equal(0, sw.GetSpan().Length);
        sw.WriteUInt8(10);
        Assert.Equal(new byte[] { 10 }, sw.GetSpanAndReset().ToArray());

        w = new();
        w.WriteInt32LE(0x04030201);
        Assert.Equal(new byte[] { 2, 3 }, w.AsReadOnlySpan(1, 2).ToArray());
        w.SetCurrentPosition(1);
        w.WriteUInt8(0xff);
        w.NoControllerSetCurrentPosition(4);
        Assert.Equal(4u, w.NoControllerGetCurrentPosition());
        Assert.Equal(new byte[] { 1, 0xff, 3, 4 }, w.GetSpan().ToArray());
        w.UpdateBuffer("xyz"u8);
        Assert.Equal("xyz"u8.ToArray(), w.GetSpan().ToArray());
        Assert.Equal("xyz"u8.ToArray(), w.GetByteBufferAndReset().ToByteArray());
    }

    static void Flush(MemWriter w) => w.Flush();

    [Fact]
    public unsafe void MemWriterPointerConstructorsAndBlockWrites()
    {
        var buf = GC.AllocateArray<byte>(8, pinned: true);
        fixed (byte* p = buf)
        {
            var w = new MemWriter(p, 8);
            w.WriteBlock((nint)(p + 4), 0);
            w.WriteUInt16LE(0x0201);
            Assert.Equal(2, w.GetCurrentPosition());
            var w2 = new MemWriter((nint)p, 8);
            Assert.Equal(0, w2.GetCurrentPosition());
        }

        var src = "0123456789"u8.ToArray();
        fixed (byte* s = src)
        {
            var w = new MemWriter();
            w.WriteBlock(s, 3);
            w.WriteBlock((nint)(s + 3), 2);
            Assert.Equal("01234"u8.ToArray(), w.GetSpan().ToArray());
            var controller = new MemReaderWriterRoundTripTest.ChunkedMemWriter(4);
            var cw = new MemWriter(controller);
            cw.WriteUInt8((byte)'x');
            cw.WriteBlock(s, 10);
            cw.WriteBlock(s, 2);
            Assert.Equal("x012345678901"u8.ToArray(), controller.GetBytes(ref cw));
        }

        var nullController = new MemWriter((IMemWriter?)null);
        nullController.WriteUInt8(1);
        Assert.Equal(1, nullController.GetCurrentPosition());
    }

    [Fact]
    public void MemWriterMethodsNotAllowedWithController()
    {
        var w = new MemWriter(new MemReaderWriterRoundTripTest.ChunkedMemWriter(16));
        Assert.Throws<InvalidOperationException>(() => GetPersistentSpan(w));
        Assert.Throws<InvalidOperationException>(() => GetPersistentMemory(w));
        Assert.Throws<InvalidOperationException>(() => StartWriteByteArray(w));
        Assert.Throws<InvalidOperationException>(() => StartXor(w));
        Assert.Throws<NotSupportedException>(() => SetPosition(w, 0));
    }

    static void GetPersistentSpan(MemWriter w) => w.GetPersistentSpanAndReset();
    static void GetPersistentMemory(MemWriter w) => w.GetPersistentMemoryAndReset();
    static void StartWriteByteArray(MemWriter w) => w.StartWriteByteArray();
    static void StartXor(MemWriter w) => w.StartXor();
    static void SetPosition(MemWriter w, long pos) => w.SetCurrentPosition(pos);

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(126)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(16382)]
    [InlineData(16383)]
    [InlineData(20000)]
    public void StartFinishWriteByteArrayEncodesLength(int length)
    {
        var w = new MemWriter();
        w.WriteUInt8(0xAA);
        var start = w.StartWriteByteArray();
        var content = Enumerable.Range(0, length).Select(i => (byte)(i * 3)).ToArray();
        w.WriteBlock(content);
        w.FinishWriteByteArray(start);
        w.WriteUInt8(0xBB);
        var r = Reader(w.GetSpan().ToArray());
        Assert.Equal(0xAA, r.ReadUInt8());
        Assert.Equal(content, r.ReadByteArray());
        Assert.Equal(0xBB, r.ReadUInt8());
        Assert.True(r.Eof);
    }

    [Theory]
    [InlineData(0, 0xFF)]
    [InlineData(5, 0xFF)]
    [InlineData(100, 0x5A)]
    public void XorRegion(int length, byte xorValue)
    {
        var w = new MemWriter();
        w.WriteUInt8(1);
        var start = w.StartXor();
        var content = Enumerable.Range(0, length).Select(i => (byte)i).ToArray();
        w.WriteBlock(content);
        w.FinishXor(start, xorValue);
        var expected = new byte[] { 1 }.Concat(content.Select(b => (byte)(b ^ xorValue))).ToArray();
        Assert.Equal(expected, w.GetSpan().ToArray());
    }

    [Fact]
    public void BlockWriteToSpan()
    {
        var w = new MemWriter();
        var span = w.BlockWriteToSpan(3, out var needToBeWritten);
        Assert.False(needToBeWritten);
        "abc"u8.CopyTo(span);
        Assert.Equal("abc"u8.ToArray(), w.GetSpan().ToArray());
        Assert.Throws<ArgumentOutOfRangeException>(() => BlockWriteToSpanHelper(w, -1));

        var controller = new MemReaderWriterRoundTripTest.ChunkedMemWriter(8);
        var cw = new MemWriter(controller);
        span = cw.BlockWriteToSpan(4, out needToBeWritten);
        Assert.False(needToBeWritten);
        "1234"u8.CopyTo(span);
        span = cw.BlockWriteToSpan(20, out needToBeWritten);
        Assert.True(needToBeWritten);
        Assert.Equal(20, span.Length);
        span.Fill((byte)'z');
        cw.WriteBlock(span);
        Assert.Equal("1234"u8.ToArray().Concat(Enumerable.Repeat((byte)'z', 20)).ToArray(), controller.GetBytes(ref cw));
    }

    static void BlockWriteToSpanHelper(MemWriter w, int len) => w.BlockWriteToSpan(len, out _);

    [Fact]
    public void WriteDateTimeForbidUnspecifiedKind()
    {
        var w = new MemWriter();
        w.WriteDateTimeForbidUnspecifiedKind(DateTime.MinValue);
        w.WriteDateTimeForbidUnspecifiedKind(DateTime.MaxValue);
        var utc = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        w.WriteDateTimeForbidUnspecifiedKind(utc);
        Assert.Throws<ArgumentOutOfRangeException>(() => WriteUnspecified(new DateTime(2020, 1, 1)));
        var r = Reader(w.GetSpan().ToArray());
        Assert.Equal(DateTimeKind.Utc, r.ReadDateTime().Kind);
        Assert.Equal(DateTimeKind.Utc, r.ReadDateTime().Kind);
        Assert.Equal(utc, r.ReadDateTime());
    }

    static void WriteUnspecified(DateTime value)
    {
        var w = new MemWriter();
        w.WriteDateTimeForbidUnspecifiedKind(value);
    }

    [Fact]
    public void WriteByteArrayOverloads()
    {
        var w = new MemWriter();
        w.WriteByteArray(ByteBuffer.NewAsync(new byte[] { 9, 1, 2, 9 }, 1, 2));
        w.WriteBlock(ByteBuffer.NewSync(new byte[] { 3 }));
        w.WriteByteArrayRaw(null);
        w.WriteByteArrayRaw([4]);
        var r = Reader(w.GetSpan().ToArray());
        Assert.Equal(new byte[] { 1, 2 }, r.ReadByteArray());
        Assert.Equal(3, r.ReadUInt8());
        Assert.Equal(4, r.ReadUInt8());
        Assert.True(r.Eof);
    }

    [Fact]
    public void WriteStringInUtf8WithoutControllerGrowsBuffer()
    {
        Span<byte> small = stackalloc byte[3];
        var w = MemWriter.CreateFromStackAllocatedSpan(small);
        var value = string.Concat(Enumerable.Repeat("€😀ř", 100));
        w.WriteStringInUtf8(value);
        Assert.Equal(value, Reader(w.GetSpan().ToArray()).ReadStringInUtf8());
    }

    [Fact]
    public void LoggingPositionLessStreamLogsAllOperations()
    {
        var log = new List<string>();
        var inner = new MemoryPositionLessStream();
        var logging = new LoggingPositionLessStream(inner, true, log.Add);
        var data = Enumerable.Range(0, 40).Select(i => (byte)i).ToArray();
        logging.Write(data, 0);
        var read = new byte[50];
        Assert.Equal(40, logging.Read(read, 0));
        Assert.Equal(data, read[..40]);
        Assert.Equal(0, logging.Read(read, 40));
        logging.Flush();
        logging.HardFlush();
        Assert.Equal(40ul, logging.GetSize());
        logging.SetSize(20);
        Assert.Equal(20ul, inner.GetSize());
        logging.Dispose();
        logging.Dispose();
        Assert.StartsWith("LoggingStream created", log[0]);
        Assert.Contains("write size:40 pos:0 datalen:40", log);
        Assert.Contains("00000000 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F", log);
        Assert.Contains("00000020 20 21 22 23 24 25 26 27", log);
        Assert.Contains("read size:40/50 pos:0", log);
        Assert.Contains("read size:0/50 pos:40", log);
        Assert.Contains("flushing stream", log);
        Assert.Contains("hard flushing stream", log);
        Assert.Contains("get stream size:40", log);
        Assert.Contains("setting stream size:20", log);
        Assert.Equal(2, log.Count(l => l == "LoggingStream disposed"));
    }

    sealed class ThrowingPositionLessStream : IPositionLessStream
    {
        public int Read(Span<byte> data, ulong pos) => throw new IOException("read");
        public void Write(ReadOnlySpan<byte> data, ulong pos) => throw new IOException("write");
        public void Flush() { }
        public void HardFlush() { }
        public ulong GetSize() => 0;
        public void SetSize(ulong size) => throw new IOException("setsize");
        public void Dispose() { }
    }

    [Fact]
    public void LoggingPositionLessStreamLogsExceptions()
    {
        var log = new List<string>();
        var logging = new LoggingPositionLessStream(new ThrowingPositionLessStream(), false, log.Add);
        Assert.Throws<IOException>(() => logging.Write(new byte[] { 1 }, 0));
        Assert.Throws<IOException>(() => logging.SetSize(1));
        logging.Dispose();
        Assert.Contains(log, l => l.StartsWith("Exception in write:"));
        Assert.Contains(log, l => l.StartsWith("Exception in setSize:"));
    }

    [Fact]
    public void PositionLessStreamProxyOverFileBuffersWrites()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PositionLessStreamProxy(""));
        Assert.Throws<ArgumentNullException>(() => new PositionLessStreamProxy(null!, false));
        var fileName = Path.GetTempFileName();
        try
        {
            using (var proxy = new PositionLessStreamProxy(fileName))
            {
                proxy.Write("hello"u8, 0);
                // contiguous append stays in buffer
                proxy.Write(" world"u8, 5);
                // overwrite inside buffer
                proxy.Write("W"u8, 6);
                Assert.Equal(11ul, proxy.GetSize());
                var buf = new byte[11];
                Assert.Equal(11, proxy.Read(buf, 0));
                Assert.Equal("hello World"u8.ToArray(), buf);
                // non-contiguous write forces flush of previous buffer
                proxy.Write("A"u8, 20);
                proxy.Write("B"u8, 30);
                Assert.Equal(31ul, proxy.GetSize());
                // large write bypasses buffer
                var large = Enumerable.Range(0, 40000).Select(i => (byte)i).ToArray();
                proxy.Write(large, 100);
                Assert.Equal(40100ul, proxy.GetSize());
                var readLarge = new byte[40000];
                Assert.Equal(40000, proxy.Read(readLarge, 100));
                Assert.Equal(large, readLarge);
                Assert.Equal(1, proxy.Read(buf.AsSpan(0, 1), 20));
                Assert.Equal((byte)'A', buf[0]);
                proxy.Write("x"u8, 40100);
                proxy.Flush();
                proxy.Write("y"u8, 40101);
                proxy.HardFlush();
                proxy.Write("z"u8, 40102);
                proxy.SetSize(40102);
                Assert.Equal(40102ul, proxy.GetSize());
                Assert.Equal(0, proxy.Read(buf, 40102));
            }

            var content = File.ReadAllBytes(fileName);
            Assert.Equal(40102, content.Length);
            Assert.Equal("hello World"u8.ToArray(), content[..11]);
            Assert.Equal((byte)'B', content[30]);
            Assert.Equal((byte)'y', content[40101]);
        }
        finally
        {
            File.Delete(fileName);
        }
    }

    sealed class FailingStream : MemoryStream
    {
        public bool FailRead;
        public bool FailWrite;

        public override int Read(Span<byte> buffer)
        {
            if (FailRead) throw new IOException("read");
            return base.Read(buffer);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (FailWrite) throw new IOException("write");
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (FailWrite) throw new IOException("write");
            base.Write(buffer);
        }
    }

    [Fact]
    public void PositionLessStreamProxyOverStreamWritesDirectlyAndRecoversFromErrors()
    {
        var stream = new FailingStream();
        stream.Write("abc"u8);
        var proxy = new PositionLessStreamProxy(stream, false);
        proxy.Write("XY"u8, 1);
        Assert.Equal("aXY"u8.ToArray(), stream.ToArray());
        var buf = new byte[3];
        Assert.Equal(3, proxy.Read(buf, 0));
        Assert.Equal("aXY"u8.ToArray(), buf);
        stream.FailRead = true;
        Assert.Throws<IOException>(() => proxy.Read(buf, 0));
        stream.FailRead = false;
        Assert.Equal(2, proxy.Read(buf, 1));
        stream.FailWrite = true;
        Assert.Throws<IOException>(() => proxy.Write("Q"u8, 0));
        stream.FailWrite = false;
        proxy.Write("Q"u8, 0);
        proxy.HardFlush();
        proxy.Flush();
        proxy.SetSize(2);
        Assert.Equal(2ul, proxy.GetSize());
        Assert.Equal("QX"u8.ToArray(), stream.ToArray());
        proxy.Dispose();
        Assert.True(stream.CanRead);
        var disposing = new PositionLessStreamProxy(stream, true);
        disposing.Dispose();
        Assert.False(stream.CanRead);
    }

    [Fact]
    public void PositionLessStreamProxyFailingBufferedFlushIsReported()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var proxy = new PositionLessStreamProxy(fileName);
            proxy.Write("abc"u8, 0);
            proxy.Dispose();
            // after dispose of underlying stream, flushing buffered data fails
            Assert.ThrowsAny<Exception>(() => proxy.Flush());
            // buffer was discarded so subsequent flush does nothing harmful besides stream call
            Assert.ThrowsAny<Exception>(() => proxy.Flush());
        }
        finally
        {
            File.Delete(fileName);
        }
    }

    [Fact]
    public void PositionLessFileStreamProxyReadsAndWrites()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            using (var proxy = new PositionLessFileStreamProxy(fileName))
            {
                proxy.Write("hello"u8, 0);
                proxy.Write("!"u8, 10);
                Assert.Equal(11ul, proxy.GetSize());
                var buf = new byte[11];
                Assert.Equal(11, proxy.Read(buf, 0));
                Assert.Equal("hello"u8.ToArray(), buf[..5]);
                proxy.Flush();
                proxy.HardFlush();
                proxy.SetSize(5);
                Assert.Equal(5ul, proxy.GetSize());
            }

            Assert.Equal("hello"u8.ToArray(), File.ReadAllBytes(fileName));
            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite))
            {
                using (var proxy = new PositionLessFileStreamProxy(fs, false))
                {
                    proxy.Write("J"u8, 0);
                }

                Assert.True(fs.CanRead);
                using var disposing = new PositionLessFileStreamProxy(fs, true);
            }

            Assert.Equal("Jello"u8.ToArray(), File.ReadAllBytes(fileName));
        }
        finally
        {
            File.Delete(fileName);
        }
    }

    [Fact]
    public void FileMemWriterSupportsSeekingAndLargeBlocks()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var controller = new FileMemWriter(fileName);
            var w = new MemWriter(controller);
            w.WriteInt32BE(0);
            var big = Enumerable.Range(0, 3 * 1024 * 1024 + 5).Select(i => (byte)(i >> 3)).ToArray();
            w.WriteBlock(big);
            Assert.Equal(4 + big.Length, w.GetCurrentPosition());
            w.Flush();
            w.Flush();
            w.SetCurrentPosition(0);
            w.WriteInt32BE(0x01020304);
            w.Flush();
            controller.Dispose();
            var content = File.ReadAllBytes(fileName);
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, content[..4]);
            Assert.True(big.AsSpan().SequenceEqual(content.AsSpan(4)));
        }
        finally
        {
            File.Delete(fileName);
        }
    }

    [Fact]
    public void PositionLessStreamWriterSupportsSeeking()
    {
        var stream = new MemoryPositionLessStream();
        var controller = new PositionLessStreamWriter(stream);
        var w = new MemWriter(controller);
        w.WriteInt64LE(0);
        w.WriteBlock(new byte[20000]);
        w.SetCurrentPosition(2);
        w.WriteUInt16LE(0xBEEF);
        w.Flush();
        controller.Dispose();
        var buf = new byte[4];
        stream.Read(buf, 0);
        Assert.Equal(new byte[] { 0, 0, 0xEF, 0xBE }, buf);
        Assert.Equal(20008ul, stream.GetSize());
    }

    [Fact]
    public void PlatformMethodsResolveRealPath()
    {
        var dir = Directory.CreateTempSubdirectory();
        try
        {
            var file = Path.Combine(dir.FullName, "file.txt");
            File.WriteAllText(file, "x");
            var real = PlatformMethods.Instance.RealPath(file);
            Assert.NotNull(real);
            Assert.EndsWith("file.txt", real);
            Assert.Equal("x", File.ReadAllText(real!));
            Assert.Null(PlatformMethods.Instance.RealPath(Path.Combine(dir.FullName, "missing", "file")));
        }
        finally
        {
            dir.Delete(true);
        }
    }

    [Fact]
    public void Utf8StringBoundaryCasesRoundTrip()
    {
        foreach (var value in new[] { "", "a", new string('a', 127), new string('a', 128), new string('ž', 64), "😀" })
        {
            var w = new MemWriter();
            w.WriteStringInUtf8(value);
            var bytes = w.GetSpan().ToArray();
            Assert.Equal(Encoding.UTF8.GetByteCount(value), (int)Reader(bytes).ReadVUInt32());
            var r = Reader(bytes);
            Assert.Equal(value, r.ReadStringInUtf8());
            Assert.True(r.Eof);
        }
    }

    // Before the fix a memory stream kept the bytes beyond a smaller size, so growing it again read them back, unlike a
    // file, which reads zeros there.
    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(128 * 1024 + 5, false)]
    [InlineData(128 * 1024 + 5, true)]
    [InlineData(128 * 1024, true)]
    public void MemoryStreamReadsZerosWhereItWasCutOff(int newSize, bool growByWrite)
    {
        const int size = 3 * 128 * 1024;
        var data = new byte[size];
        new Random(1).NextBytes(data);
        Array.Fill(data, (byte)0xAA, 0, newSize);
        var memory = new MemoryPositionLessStream();
        var path = Path.GetTempFileName();
        try
        {
            using var file = new PositionLessFileStreamProxy(path);
            foreach (var stream in new IPositionLessStream[] { memory, file })
            {
                stream.Write(data, 0);
                stream.SetSize((ulong)newSize);
                if (growByWrite) stream.Write([7], size - 1);
                else stream.SetSize(size);
            }

            var expected = new byte[size];
            Array.Fill(expected, (byte)0xAA, 0, newSize);
            if (growByWrite) expected[size - 1] = 7;
            foreach (var stream in new IPositionLessStream[] { memory, file })
            {
                var read = new byte[size];
                Assert.Equal(size, stream.Read(read, 0));
                Assert.Equal(expected, read);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }
}
