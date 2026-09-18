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
}
