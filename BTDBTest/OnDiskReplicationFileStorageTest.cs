using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public sealed class OnDiskReplicationFileStorageTest : IDisposable
{
    readonly string _directory = Path.Combine(Path.GetTempPath(), "btdb-replication-storage-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
    }

    static void Append(IFileCollectionFile file, ReadOnlySpan<byte> bytes)
    {
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(bytes);
        writer.Flush();
    }

    static byte[] ReadAll(IFileCollectionFile file)
    {
        var bytes = new byte[file.GetSize()];
        file.RandomRead(bytes, 0, false);
        return bytes;
    }

    static byte[] Pattern(int length, int seed) => Enumerable.Range(0, length).Select(i => (byte)(i * 31 + seed)).ToArray();

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 128 * 1024)]
    [InlineData(false, 128 * 1024 + 17)]
    [InlineData(true, 0)]
    [InlineData(true, 128 * 1024)]
    [InlineData(true, 128 * 1024 + 17)]
    public void RewindUnfinishedPrefixAcrossBuffersPreservesEarlierBytes(bool disk, int retained)
    {
        using IReplicationFileStorage files = disk ? new OnDiskReplicationFileStorage(_directory) : new InMemoryReplicationFileStorage();
        var file = files.ImportFile(1, "trl");
        var original = Pattern(512 * 1024, 1);
        Append(file, original);
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.SetCurrentPosition(retained);
        writer.Flush();
        var suffix = Pattern(256 * 1024, 2);
        Append(file, suffix);
        Assert.Equal(original.Take(retained).Concat(suffix).ToArray(), ReadAll(file));
    }

    [Fact]
    public void IdentitiesTypesAndContentSurviveReopenWithTruncatedLengths()
    {
        using (var files = new OnDiskReplicationFileStorage(_directory))
        {
            Assert.Equal(1u, files.AddFile("trl", FileIdParity.Odd).Index);
            Assert.Equal(2u, files.AddFile("kvi", FileIdParity.Even).Index);
            var imported = files.ImportFile(100, "pvl");
            Append(imported, Pattern(10_000, 1));
            imported.HardFlushTruncateSwitchToReadOnlyMode();
            Append(files.GetFile(1), Pattern(123, 2));
            Assert.Throws<InvalidOperationException>(() => files.ImportFile(100, "pvl"));
            Assert.Throws<InvalidOperationException>(() => Append(imported, [1]));
        }
        Assert.Equal(10_000, new FileInfo(Path.Combine(_directory, "00000100.pvl")).Length);
        Assert.Equal(123, new FileInfo(Path.Combine(_directory, "00000001.trl")).Length);
        using (var files = new OnDiskReplicationFileStorage(_directory))
        {
            Assert.Equal(3u, files.GetCount());
            Assert.Equal(KVFileType.TransactionLog, files.GetFileType(1));
            Assert.Equal(KVFileType.KeyIndex, files.GetFileType(2));
            Assert.Equal(KVFileType.PureValues, files.GetFileType(100));
            Assert.Equal(Pattern(10_000, 1), ReadAll(files.GetFile(100)));
            Assert.Equal(Pattern(123, 2), ReadAll(files.GetFile(1)));
            Assert.Equal(0ul, files.GetFile(2).GetSize());
            // Allocation continues above every existing ID of the requested parity.
            Assert.Equal(3u, files.AddFile("trl", FileIdParity.Odd).Index);
            Assert.Equal(102u, files.AddFile("pvl", FileIdParity.Even).Index);
            Append(files.GetFile(1), Pattern(50, 3));
            Assert.Equal(Pattern(123, 2).Concat(Pattern(50, 3)).ToArray(), ReadAll(files.GetFile(1)));
        }
    }

    [Fact]
    public async Task ReadersCrossBuffersAndGrowthWhileTheAppenderRemaps()
    {
        using var files = new OnDiskReplicationFileStorage(_directory);
        var file = files.AddFile("trl", FileIdParity.Odd);
        var expected = Pattern(9 * 1024 * 1024 + 17, 5); // Several mapping growth chunks.
        var writer = new MemWriter(file.GetAppenderWriter());
        using var stop = new CancellationTokenSource();
        var reader = Task.Run(() =>
        {
            var buffer = new byte[4096];
            var checks = 0;
            while (!stop.IsCancellationRequested)
            {
                var size = file.GetSize();
                if (size < (ulong)buffer.Length) continue;
                var position = size - (ulong)buffer.Length;
                file.RandomRead(buffer, position, false);
                Assert.True(buffer.AsSpan().SequenceEqual(expected.AsSpan((int)position, buffer.Length)));
                checks++;
            }
            return checks;
        });
        for (var offset = 0; offset < expected.Length; offset += 1000)
        {
            writer.WriteBlock(expected.AsSpan(offset, Math.Min(1000, expected.Length - offset)));
            writer.Flush();
        }
        stop.Cancel();
        Assert.True(await reader > 0);
        var exclusive = new MemReader(file.GetExclusiveReader());
        var copy = new byte[expected.Length];
        exclusive.ReadBlock(copy.AsSpan(0, 70_000));
        exclusive.SkipBlock(1_000_000);
        exclusive.ReadBlock(copy.AsSpan(1_070_000));
        Assert.True(exclusive.Eof);
        Assert.Equal(expected.AsSpan(0, 70_000).ToArray(), copy.AsSpan(0, 70_000).ToArray());
        Assert.Equal(expected.AsSpan(1_070_000).ToArray(), copy.AsSpan(1_070_000).ToArray());
        Assert.Throws<EndOfStreamException>(() => file.RandomRead(new byte[2], (ulong)expected.Length - 1, false));
    }

    [Fact]
    public void RemovedFileIsDeletedAndReadsReportItMissing()
    {
        using var files = new OnDiskReplicationFileStorage(_directory);
        var file = files.ImportFile(4, "pvl");
        Append(file, Pattern(100, 7));
        file.Remove();
        Assert.Null(files.GetFile(4));
        Assert.False(File.Exists(Path.Combine(_directory, "00000004.pvl")));
        Assert.Throws<FileNotFoundException>(() => file.RandomRead(new byte[1], 0, false));
        Assert.Equal(4u, files.ImportFile(4, "pvl").Index); // Replication may reimport a discarded cache identity.
    }

    [Fact]
    public async Task ReplicatedDatabaseReopensFromDisk()
    {
        using (var files = new OnDiskReplicationFileStorage(_directory))
        using (var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
               {
                   FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = new(),
                   Compression = new NoCompressionStrategy(), CompactorScheduler = null
               }))
        {
            for (ulong id = 1; id <= 300; id++)
            {
                using var transaction = await db.StartWritingTransaction(id);
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([(byte)id], Pattern(5000, (int)id));
                transaction.Commit();
            }
        }
        using (var files = new OnDiskReplicationFileStorage(_directory))
        using (var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
               {
                   FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = new(),
                   Compression = new NoCompressionStrategy(), CompactorScheduler = null
               }))
        {
            using var read = db.StartReadOnlyTransaction();
            Assert.Equal(300ul, read.GetCommitUlong());
            using var cursor = read.CreateCursor();
            Assert.True(cursor.FindExactKey([200]));
            var buffer = Span<byte>.Empty;
            Assert.Equal(Pattern(5000, 200), cursor.GetValueSpan(ref buffer, true).ToArray());
        }
    }
}
