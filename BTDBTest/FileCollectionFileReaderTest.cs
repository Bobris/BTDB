using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class FileCollectionFileReaderTest
{
    sealed class TrackingFile(byte[] data) : IFileCollectionFile
    {
        public byte[] Data = data;
        public readonly List<(ulong Offset, int Length)> Reads = [];
        public uint Index => 1;
        public ulong GetSize() => (ulong)Data.Length;
        public void RandomRead(Span<byte> data, ulong position, bool doNotCache)
        {
            Reads.Add((position, data.Length));
            Data.AsSpan((int)position, data.Length).CopyTo(data);
        }
        public IMemReader GetExclusiveReader() => throw new NotSupportedException();
        public void AdvisePrefetch() => throw new NotSupportedException();
        public IMemWriter GetAppenderWriter() => throw new NotSupportedException();
        public IMemWriter GetExclusiveAppenderWriter() => throw new NotSupportedException();
        public void HardFlush() => throw new NotSupportedException();
        public void HardFlushTruncateSwitchToReadOnlyMode() => throw new NotSupportedException();
        public void HardFlushTruncateSwitchToDisposedMode() => throw new NotSupportedException();
        public void Remove() => throw new NotSupportedException();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(16)]
    [InlineData(8192)]
    public void ReadsAcrossBufferBoundariesWithinSelectedRange(int bufferSize)
    {
        var file = new TrackingFile(Enumerable.Range(0, 30000).Select(i => (byte)i).ToArray());
        var controller = new FileCollectionFileReader(file, 123, 25000, bufferSize);
        var reader = new MemReader(controller);
        Assert.Equal(123, reader.GetCurrentPosition());
        Assert.Equal(file.Data[123], reader.ReadUInt8());
        Assert.Equal(BinaryPrimitives.ReadUInt64BigEndian(file.Data.AsSpan(124)), reader.ReadUInt64BE());
        reader.SkipBlock(100);
        Assert.Equal(232, reader.GetCurrentPosition());
        var block = new byte[20000];
        reader.ReadBlock(block);
        Assert.Equal(file.Data.AsSpan(232, block.Length).ToArray(), block);
        for (var i = 20232; i < 25000; i++) Assert.Equal(file.Data[i], reader.ReadUInt8());
        Assert.True(reader.Eof);
        Assert.Equal(25000, reader.GetCurrentPosition());
        Assert.Equal(123ul, file.Reads[0].Offset);
        Assert.All(file.Reads, read =>
        {
            Assert.True(read.Offset >= 123);
            Assert.True(read.Offset + (uint)read.Length <= 25000);
        });
        Assert.Throws<EndOfStreamException>(() => ReadByte(controller));
    }

    [Fact]
    public void SeekAndSkipDoNotReadSkippedBytes()
    {
        var file = new TrackingFile(new byte[10000]);
        var controller = new FileCollectionFileReader(file, 1000, bufferSize: 16);
        var reader = new MemReader(controller);
        reader.SkipBlock(5000);
        Assert.Equal(6000, reader.GetCurrentPosition());
        Assert.Single(file.Reads);
        reader.ReadUInt8();
        Assert.Equal((6000ul, 16), file.Reads[1]);
        reader.SetCurrentPosition(7000);
        Assert.Equal(2, file.Reads.Count);
        reader.ReadUInt8();
        Assert.Equal((7000ul, 16), file.Reads[2]);
        reader.SetCurrentPosition(10000);
        Assert.True(reader.Eof);
        Assert.Equal(3, file.Reads.Count);
    }

    [Fact]
    public void RestartSwitchesFilesAndRangesWithoutOldBufferedBytes()
    {
        var first = new TrackingFile(Enumerable.Repeat((byte)1, 100).ToArray());
        var second = new TrackingFile(Enumerable.Repeat((byte)2, 200).ToArray());
        var controller = new FileCollectionFileReader(first, bufferSize: 16);
        var reader = new MemReader(controller);
        Assert.Equal(1, reader.ReadUInt8());
        controller.Restart(second, 50, 60);
        reader = new MemReader(controller);
        var result = new byte[10];
        reader.ReadBlock(result);
        Assert.All(result, value => Assert.Equal(2, value));
        Assert.Equal((50ul, 10), Assert.Single(second.Reads));
        Assert.Equal(60, reader.GetCurrentPosition());
        Assert.True(reader.Eof);
        controller.Restart(first, 100);
        reader = new MemReader(controller);
        Assert.True(reader.Eof);
        Assert.Single(first.Reads);
    }

    [Fact]
    public void FileGrowthBecomesVisibleOnlyAfterRestart()
    {
        var file = new TrackingFile(new byte[100]);
        var controller = new FileCollectionFileReader(file, bufferSize: 8);
        var reader = new MemReader(controller);
        file.Data = [..file.Data, 42];
        reader.SkipBlock(100);
        Assert.True(reader.Eof);
        Assert.Throws<EndOfStreamException>(() => ReadByte(controller));
        controller.Restart(file, 100);
        reader = new MemReader(controller);
        Assert.Equal(42, reader.ReadUInt8());
        Assert.True(reader.Eof);
    }

    [Fact]
    public void RejectsInvalidRangesAndReadsPastEnd()
    {
        var file = new TrackingFile(new byte[10]);
        Assert.Throws<ArgumentOutOfRangeException>(() => new FileCollectionFileReader(file, bufferSize: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FileCollectionFileReader(file, 11));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FileCollectionFileReader(file, 5, 4));
        Assert.Throws<EndOfStreamException>(() =>
        {
            var reader = new MemReader(new FileCollectionFileReader(file, bufferSize: 3));
            reader.ReadBlock(new byte[11]);
        });
        Assert.Throws<EndOfStreamException>(() =>
        {
            var reader = new MemReader(new FileCollectionFileReader(file));
            reader.SkipBlock(11);
        });
        Assert.Throws<EndOfStreamException>(() =>
        {
            var reader = new MemReader(new FileCollectionFileReader(file));
            reader.SetCurrentPosition(11);
        });
    }

    static byte ReadByte(FileCollectionFileReader controller)
    {
        var reader = new MemReader(controller);
        return reader.ReadUInt8();
    }
}
