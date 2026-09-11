using System;
using System.Collections.Generic;
using System.IO;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class VarIntCopyTest
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 0)]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    [InlineData(false, 7)]
    [InlineData(true, 7)]
    public void EncodingsAreCopiedExactlyAcrossAllWidths(bool signed, int readerBufferSize)
    {
        var values = new List<ulong> { 0, ulong.MaxValue };
        for (var bit = 0; bit < 64; bit++)
        {
            var value = 1ul << bit;
            values.Add(value);
            values.Add(value - 1);
            values.Add(value + 1);
            values.Add(~value);
        }
        var encoded = new MemWriter();
        // Enough values to cross output controller boundaries as well as input boundaries.
        for (var repeat = 0; repeat < 12; repeat++)
        foreach (var value in values)
        {
            if (signed) encoded.WriteVInt64(unchecked((long)value));
            else encoded.WriteVUInt64(value);
        }
        encoded.WriteUInt8(0x5a);
        var bytes = encoded.GetPersistentMemoryAndReset();
        using var input = new MemoryPositionLessStream();
        input.Write(bytes.Span, 0);
        var reader = readerBufferSize == 0
            ? MemReader.CreateFromReadOnlyMemory(bytes)
            : new MemReader(new PositionLessStreamReader(input, readerBufferSize));
        using var output = new MemoryPositionLessStream();
        using var controller = new PositionLessStreamWriter(output);
        var writer = new MemWriter(controller);
        for (var repeat = 0; repeat < 12; repeat++)
        foreach (var _ in values)
        {
            if (signed) reader.CopyVInt64ToWriter(ref writer);
            else reader.CopyVUInt64ToWriter(ref writer);
        }
        Assert.Equal(0x5a, reader.ReadUInt8());
        Assert.True(reader.Eof);
        reader.Dispose();
        writer.WriteUInt8(0x5a);
        writer.Flush();
        var actual = new byte[(int)output.GetSize()];
        Assert.Equal(actual.Length, output.Read(actual, 0));
        Assert.Equal(bytes.ToArray(), actual);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TruncatedEncodingIsRejected(bool signed)
    {
        var encoded = new MemWriter();
        if (signed) encoded.WriteVInt64(long.MinValue);
        else encoded.WriteVUInt64(ulong.MaxValue);
        var bytes = encoded.GetSpan()[..^1].ToArray();
        Assert.Throws<EndOfStreamException>(() =>
        {
            var reader = MemReader.CreateFromReadOnlyMemory(bytes);
            var writer = new MemWriter();
            try
            {
                if (signed) reader.CopyVInt64ToWriter(ref writer);
                else reader.CopyVUInt64ToWriter(ref writer);
            }
            finally { reader.Dispose(); }
        });
    }
}
