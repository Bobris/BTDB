using System;
using System.IO;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class FileCollectionTests : IDisposable
{
    string _dir;

    public FileCollectionTests()
    {
        _dir = Path.GetTempPath() + "/deleteMe";
        Directory.CreateDirectory(_dir);
    }

    [Fact]
    public void CanCreateFile()
    {
        using (var dc = new OnDiskFileCollection(_dir))
        {
            var f = dc.AddFile("kvi");
            var w = f.GetAppenderWriter();
            var writer = new MemWriter(w);
            for (var i = 0; i < 32000; i++)
            {
                writer.WriteInt32LE(i);
            }

            writer.Flush();

            var data = new byte[4];
            for (var i = 0; i < 32000; i++)
            {
                f.RandomRead(data, (ulong)i * 4, false);
                Assert.Equal(i, PackUnpack.UnpackInt32LE(data, 0));
            }
        }
    }

    [Fact]
    public void CanCreateAndWriteMemoryMappedFileBackedByEmptyFile()
    {
        using var dc = new OnDiskMemoryMappedFileCollection(_dir);
        var file = dc.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteUInt8(42);
        writer.Flush();

        var data = new byte[1];
        file.RandomRead(data, 0, false);
        Assert.Equal((byte)42, data[0]);
    }

    // Before the fix the largest file ID was kept in an int, so a stray file named with the largest uint made the next
    // file get ID 0, which every later open skips as no file: a transaction log created there was lost.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FileAfterTheLargestPossibleIdIsRefused(bool memoryMapped)
    {
        File.WriteAllBytes(Path.Combine(_dir, uint.MaxValue + ".bin"), [1]);
        using IFileCollection collection = memoryMapped
            ? new OnDiskMemoryMappedFileCollection(_dir)
            : new OnDiskFileCollection(_dir);
        Assert.Throws<InvalidOperationException>(() => collection.AddFile("trl"));
        Assert.DoesNotContain(Directory.GetFiles(_dir), path => Path.GetFileName(path).StartsWith("00000000"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FileAfterAnIdAboveIntMaxValueGetsTheNextId(bool memoryMapped)
    {
        File.WriteAllBytes(Path.Combine(_dir, "3000000000.bin"), [1]);
        File.WriteAllBytes(Path.Combine(_dir, "00000005.trl"), [1]);
        using IFileCollection collection = memoryMapped
            ? new OnDiskMemoryMappedFileCollection(_dir)
            : new OnDiskFileCollection(_dir);
        Assert.Equal(3000000001u, collection.AddFile("trl").Index);
    }

    // Before the fix file names were parsed allowing a sign and spaces, so a stray "+5.bin" beside "00000005.trl" took
    // the same ID and opening failed with a duplicate key.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StrayFileNamedLikeAnIdWithASignIsIgnored(bool memoryMapped)
    {
        File.WriteAllBytes(Path.Combine(_dir, "00000005.trl"), [1]);
        File.WriteAllBytes(Path.Combine(_dir, "+5.bin"), [1]);
        File.WriteAllBytes(Path.Combine(_dir, " 6.bin"), [1]);
        using IFileCollection collection = memoryMapped
            ? new OnDiskMemoryMappedFileCollection(_dir)
            : new OnDiskFileCollection(_dir);
        Assert.Equal(1u, collection.GetCount());
        Assert.Equal(6u, collection.AddFile("trl").Index);
    }

    // Before the fix a read past the end, as a damaged value pointer asks for, failed with ArgumentOutOfRangeException
    // in OnDiskFileCollection and read memory outside the mapping in OnDiskMemoryMappedFileCollection.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadFarPastTheEndIsEndOfStream(bool memoryMapped)
    {
        using IFileCollection collection = memoryMapped
            ? new OnDiskMemoryMappedFileCollection(_dir)
            : new OnDiskFileCollection(_dir);
        var file = collection.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteInt32LE(42);
        writer.Flush();
        var data = new byte[16];
        Assert.Throws<EndOfStreamException>(() => file.RandomRead(data, 1ul << 32, false));
        Assert.Throws<EndOfStreamException>(() => file.RandomRead(data, ulong.MaxValue - 4, false));
        file.RandomRead(data.AsSpan(0, 4), 0, false);
        Assert.Equal(42, PackUnpack.UnpackInt32LE(data, 0));
    }

    public void Dispose()
    {
        Directory.Delete(_dir, true);
    }
}
