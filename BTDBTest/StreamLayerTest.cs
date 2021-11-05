using System;
using System.IO;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class StreamLayerTest : IDisposable
{
    const string TestFileName = "testfile.txt";
    byte[] buff = new byte[8];

    public StreamLayerTest()
    {
        if (!File.Exists(TestFileName))
        {
            using (var s = File.Create(TestFileName))
            {
                s.WriteByte(11);
            }
        }
    }

    [Fact]
    public void ReadingStreamManaged()
    {
        using (var s = File.OpenRead(TestFileName))
        using (var p = new PositionLessStreamProxy(s, false))
        {
            Assert.Equal(1, p.Read(buff, 0));
            Assert.Equal(11, buff[0]);
            Assert.Equal(0, p.Read(buff, 1));
            Assert.Equal(0, p.Read(buff, 1));
            Assert.Equal(0, p.Read(buff, 10));
        }
    }

    [Fact]
    public void ReadingStreamNatively()
    {
        using (var s = File.OpenRead(TestFileName))
        using (var p = new PositionLessFileStreamProxy(s, false))
        {
            Assert.Equal(1, p.Read(buff, 0));
            Assert.Equal(11, buff[0]);
            Assert.Equal(0, p.Read(buff, 1));
            Assert.Equal(0, p.Read(buff, 1));
            Assert.Equal(0, p.Read(buff, 10));
        }
    }

    public void Dispose()
    {
        try
        {
            File.Delete(TestFileName);
        }
        catch
        {
            // ignored
        }
    }

    [Fact]
    public void ContinuousMemoryBlockWriterBasicsWorks()
    {
        var byteArrayWriter = new ContinuousMemoryBlockWriter();
        Assert.Equal(0, byteArrayWriter.GetCurrentPositionWithoutWriter());
        Assert.Equal(Array.Empty<byte>(), byteArrayWriter.GetSpan().ToArray());
        Assert.Equal(Array.Empty<byte>(), byteArrayWriter.GetByteBuffer().ToByteArray());
        var writer = new SpanWriter(byteArrayWriter);
        writer.WriteInt8(42);
        writer.Sync();
        Assert.Equal(1, byteArrayWriter.GetCurrentPositionWithoutWriter());
        Assert.Equal(new byte[] { 42 }, byteArrayWriter.GetSpan().ToArray());
        Assert.Equal(new byte[] { 42 }, byteArrayWriter.GetByteBuffer().ToByteArray());
        writer.WriteInt8(1);
        Assert.Equal(2, writer.GetCurrentPosition());
        writer.SetCurrentPosition(1);
        writer.WriteBlock(new byte[] { 43, 44 });
        writer.Sync();
        Assert.Equal(3, byteArrayWriter.GetCurrentPositionWithoutWriter());
        Assert.Equal(new byte[] { 42, 43, 44 }, byteArrayWriter.GetSpan().ToArray());
        Assert.Equal(new byte[] { 42, 43, 44 }, byteArrayWriter.GetByteBuffer().ToByteArray());
    }

    [Fact]
    public void SpanReaderSyncingWorks()
    {
        var memFile = new MemoryPositionLessStream();
        memFile.Write(new byte[] { 1, 2 }, 0);
        var controller = new PositionLessStreamReader(memFile);
        var reader = new SpanReader(controller);
        Assert.Equal(1, reader.ReadInt8());
        reader.Sync();
        reader = new SpanReader(controller);
        Assert.Equal(2, reader.ReadInt8());
        reader.Sync();
        reader = new SpanReader(controller);
        Assert.True(reader.Eof);
    }
}

