using System;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class InMemoryFileCollectionTest
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ExclusiveReaderPreservesPositionAfterUnalignedSkipOrSeek(bool seek)
    {
        const int chunkSize = 128 * 1024;
        using var files = new InMemoryFileCollection();
        var file = files.AddFile("trl");
        var data = new byte[3 * chunkSize + 17];
        new Random(318804).NextBytes(data);
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(data);
        writer.Flush();

        var reader = new MemReader(file.GetExclusiveReader());
        const int offset = chunkSize + 13;
        if (seek) reader.SetCurrentPosition(offset);
        else reader.SkipBlock(offset);
        Assert.Equal(data[offset], reader.ReadUInt8());
        Assert.Equal(offset + 1, reader.GetCurrentPosition());
        var remaining = new byte[data.Length - offset - 1];
        reader.ReadBlock(remaining);
        Assert.Equal(data.AsSpan(offset + 1).ToArray(), remaining);
        Assert.Equal(data.Length, reader.GetCurrentPosition());
        Assert.True(reader.Eof);
    }

    // Before the fix a read far past the end, as a damaged value pointer asks for, failed with an index or overflow
    // error instead of the end of stream the other file collections report.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadFarPastTheEndIsEndOfStream(bool replicated)
    {
        using IFileCollection files = replicated ? new InMemoryReplicationFileStorage() : new InMemoryFileCollection();
        var file = files.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteInt32LE(42);
        writer.Flush();
        var data = new byte[16];
        Assert.Throws<System.IO.EndOfStreamException>(() => file.RandomRead(data, 1ul << 32, false));
        Assert.Throws<System.IO.EndOfStreamException>(() => file.RandomRead(data, ulong.MaxValue - 4, false));
        file.RandomRead(data.AsSpan(0, 4), 0, false);
        Assert.Equal(42, BTDB.Buffer.PackUnpack.UnpackInt32LE(data, 0));
    }
}
