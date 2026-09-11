using System;
using System.IO;
using System.Linq;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class OrderedStringTranscodeTest
{
    static void AssertEquivalent(string? value, int readerBuffer = 0, bool streamedWriter = false)
    {
        var input = new MemWriter();
        input.WriteString(value);
        input.WriteUInt8(0x5a);
        var expected = new MemWriter();
        expected.WriteUInt8(0x42);
        expected.WriteStringOrdered(value);
        using var inputStream = new MemoryPositionLessStream();
        if (readerBuffer != 0) inputStream.Write(input.GetSpan(), 0);
        var reader = readerBuffer == 0
            ? MemReader.CreateFromReadOnlyMemory(input.GetPersistentMemoryAndReset())
            : new MemReader(new PositionLessStreamReader(inputStream, readerBuffer));
        using var outputStream = new MemoryPositionLessStream();
        using var controller = streamedWriter ? new PositionLessStreamWriter(outputStream) : null;
        Span<byte> stack = stackalloc byte[7];
        var actual = streamedWriter ? new MemWriter(controller) : MemWriter.CreateFromStackAllocatedSpan(stack);
        actual.WriteUInt8(0x42);
        reader.CopyStringToOrdered(ref actual);
        Assert.Equal(0x5a, reader.ReadUInt8());
        Assert.True(reader.Eof);
        if (streamedWriter)
        {
            actual.Flush();
            var bytes = new byte[(int)outputStream.GetSize()];
            Assert.Equal(bytes.Length, outputStream.Read(bytes, 0));
            Assert.Equal(expected.GetSpan().ToArray(), bytes);
        }
        else
        {
            Assert.Equal(expected.GetSpan().ToArray(), actual.GetSpan().ToArray());
        }
        reader.Dispose();
    }

    [Fact]
    public void AllUtf16CodeUnitsMatchExistingEncoding()
    {
        for (var c = 0; c <= char.MaxValue; c++) AssertEquivalent(new string((char)c, 1));
    }

    [Fact]
    public void UnicodeSequencesAndAsciiBoundariesMatch()
    {
        AssertEquivalent(null);
        AssertEquivalent("");
        foreach (var length in new[] { 1, 15, 16, 17, 31, 32, 33, 127, 128, 4096, 8192, 20000 })
        {
            AssertEquivalent(new string('a', length));
            AssertEquivalent(new string('\0', length));
            foreach (var special in new[] { "\u007e", "\u007f", "\u0080", "\ud800", "\udfff", "😀", "\ud800\ud800\udc00", "\udc00\ud800" })
                AssertEquivalent(new string('x', length) + special + new string('y', 33));
        }
        var random = new Random(42);
        for (var i = 0; i < 1000; i++)
        {
            var chars = new char[random.Next(1, 256)];
            for (var j = 0; j < chars.Length; j++) chars[j] = (char)random.Next(65536);
            AssertEquivalent(new string(chars));
        }
        foreach (var scalar in new[] { 0x10000, 0x1ffff, 0x20000, 0x10ffff })
            AssertEquivalent(char.ConvertFromUtf32(scalar));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    [InlineData(16)]
    [InlineData(31)]
    public void ReaderAndWriterControllersCrossBoundaries(int bufferSize)
    {
        foreach (var value in new[] { null, "", "\0\u007e\u007f\u0080\u07ff\u0800😀\ud800!\udfff", new string('a', 20000), string.Concat(Enumerable.Repeat("a😀\ud800!\u007f", 2000)) })
            AssertEquivalent(value, bufferSize, true);
    }

    [Fact]
    public void SeparatelyEncodedSurrogatesAreNormalized()
    {
        var input = new MemWriter();
        input.WriteVUInt32(5); // Four UTF-16 units, encoded separately rather than as two scalar values.
        input.WriteVUInt32(0xd800);
        input.WriteVUInt32(0xdc00);
        input.WriteVUInt32(0xdbff);
        input.WriteVUInt32(0xdfff);
        var bytes = input.GetPersistentMemoryAndReset();
        var referenceReader = MemReader.CreateFromReadOnlyMemory(bytes);
        var expected = new MemWriter();
        expected.WriteStringOrdered(referenceReader.ReadString());
        var reader = MemReader.CreateFromReadOnlyMemory(bytes);
        var actual = new MemWriter();
        reader.CopyStringToOrdered(ref actual);
        Assert.Equal(expected.GetSpan().ToArray(), actual.GetSpan().ToArray());
        Assert.True(reader.Eof);
        referenceReader.Dispose();
        reader.Dispose();
    }

    [Fact]
    public void InvalidLengthsAndCodePointsAreRejected()
    {
        foreach (var (length, codePoint) in new[] { ((ulong)int.MaxValue + 2, 0ul), (2ul, 0x110000ul), (2ul, 0x10000ul) })
        {
            var input = new MemWriter();
            input.WriteVUInt64(length);
            input.WriteVUInt64(codePoint);
            var bytes = input.GetPersistentMemoryAndReset();
            Assert.Throws<InvalidDataException>(() =>
            {
                var reader = MemReader.CreateFromReadOnlyMemory(bytes);
                var writer = new MemWriter();
                try { reader.CopyStringToOrdered(ref writer); }
                finally { reader.Dispose(); }
            });
        }
        Assert.Throws<EndOfStreamException>(() =>
        {
            var reader = MemReader.CreateFromReadOnlyMemory(new byte[] { 2 });
            var writer = new MemWriter();
            try { reader.CopyStringToOrdered(ref writer); }
            finally { reader.Dispose(); }
        });
    }
}
