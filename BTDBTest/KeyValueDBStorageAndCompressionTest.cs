using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.SnappyCompression;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class KeyValueDBStorageAndCompressionTest
{
    [Fact]
    public void SnappyCompressionStrategyRoundTrips()
    {
        var strategy = new SnappyCompressionStrategy();
        var compressible = Enumerable.Repeat((byte)'a', 1000).ToArray();
        var data = new ByteBuffer();
        data = ByteBuffer.NewAsync(compressible);
        Assert.True(strategy.CompressValue(ref data));
        Assert.True(data.Length < compressible.Length);
        var copy = ByteBuffer.NewAsync(data.ToByteArray());
        strategy.DecompressValue(ref copy);
        Assert.Equal(compressible, copy.ToByteArray());
        var copy2 = ByteBuffer.NewAsync(data.ToByteArray());
        strategy.DecompressKey(ref copy2);
        Assert.Equal(compressible, copy2.ToByteArray());
        Assert.Equal(compressible, strategy.DecompressValue(data.AsSyncReadOnlySpan()));
        Assert.Equal(compressible, strategy.DecompressKey(data.AsSyncReadOnlySpan()));

        ReadOnlySpan<byte> span = compressible;
        Assert.True(strategy.CompressValue(ref span));
        Assert.Equal(compressible, strategy.DecompressValue(span));

        // Too short values are never compressed
        var shortBuf = ByteBuffer.NewAsync(new byte[31]);
        Assert.False(strategy.CompressValue(ref shortBuf));
        ReadOnlySpan<byte> shortSpan = new byte[31];
        Assert.False(strategy.CompressValue(ref shortSpan));
        Assert.Equal(31, shortSpan.Length);

        // Incompressible data is left untouched
        var random = new byte[1000];
        new Random(1).NextBytes(random);
        var randomBuf = ByteBuffer.NewAsync(random);
        Assert.False(strategy.CompressValue(ref randomBuf));
        Assert.Equal(random, randomBuf.ToByteArray());
        ReadOnlySpan<byte> randomSpan = random;
        Assert.False(strategy.CompressValue(ref randomSpan));
        Assert.Equal(random, randomSpan.ToArray());
    }

    [Fact]
    public void NoCompressionStrategyNeverCompressesAndRefusesDecompression()
    {
        var strategy = new NoCompressionStrategy();
        var data = ByteBuffer.NewAsync(new byte[1000]);
        Assert.False(strategy.CompressValue(ref data));
        ReadOnlySpan<byte> span = new byte[1000];
        Assert.False(strategy.CompressValue(ref span));
        Assert.Throws<BTDBException>(() =>
        {
            var b = ByteBuffer.NewAsync(new byte[10]);
            strategy.DecompressKey(ref b);
        });
        Assert.Throws<BTDBException>(() =>
        {
            var b = ByteBuffer.NewAsync(new byte[10]);
            strategy.DecompressValue(ref b);
        });
        Assert.Throws<BTDBException>(() => strategy.DecompressKey(new byte[10]));
        Assert.Throws<BTDBException>(() => strategy.DecompressValue(new byte[10]));
    }

    [Fact]
    public void DefaultCompressionKviStrategyChoosesCompression()
    {
        Assert.Equal(KeyIndexCompression.None, new DefaultCompressionKviStrategy().ChooseCompression(1000000));
        var strategy = new DefaultCompressionKviStrategy(5, 22, 100);
        Assert.Equal(KeyIndexCompression.None, strategy.ChooseCompression(99));
        Assert.Equal(KeyIndexCompression.Brotli, strategy.ChooseCompression(100));
        var reader = new MemReader();
        Assert.Throws<NotSupportedException>(() => strategy.StartDecompression(KeyIndexCompression.Old, reader));
        Assert.Throws<InvalidDataException>(() => strategy.StartDecompression((KeyIndexCompression)42, reader));
    }

    [Theory]
    [InlineData(-1, 0ul)]
    [InlineData(5, 10ul)]
    [InlineData(1, 100000ul)]
    public async Task KeyIndexWithAndWithoutBrotliCompressionCanBeReopened(int strength, ulong threshold)
    {
        using var files = new InMemoryFileCollection();
        var rnd = new Random(strength + 10);
        var expected = new (byte[] Key, byte[] Value)[500];
        KeyValueDBOptions Options() => new()
        {
            FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
            KviCompressionStrategy = new DefaultCompressionKviStrategy(strength, 22, threshold)
        };
        using (var db = new BTreeKeyValueDB(Options()))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < expected.Length; i++)
                {
                    var key = new byte[4 + rnd.Next(30)];
                    rnd.NextBytes(key);
                    PackUnpack.PackInt32BE(key, 0, i);
                    var value = new byte[rnd.Next(30)];
                    rnd.NextBytes(value);
                    expected[i] = (key, value);
                    cursor.CreateOrUpdateKeyValue(key, value);
                }

                tr.SetCommitUlong(77);
                tr.SetUlong(2, 3);
                tr.Commit();
            }

            db.CreateKvi(CancellationToken.None);
            await db.Compact(CancellationToken.None);
        }

        Assert.Contains(files.Enumerate(), f => f.GetSize() > 0);
        using (var db = new BTreeKeyValueDB(Options()))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(expected.Length, tr.GetKeyValueCount());
            Assert.Equal(77ul, tr.GetCommitUlong());
            Assert.Equal(3ul, tr.GetUlong(2));
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.True(cursor.FindNextKey([]));
                Assert.Equal(expected[i].Key, cursor.SlowGetKey());
                Assert.Equal(expected[i].Value, cursor.SlowGetValue());
            }
        }
    }

    [Fact]
    public void ReplicationStorageReaderWriterWorksAcrossChunks()
    {
        const int chunkSize = 128 * 1024;
        using var storage = new InMemoryReplicationFileStorage();
        var file = storage.AddFile("trl");
        Assert.Equal(KVFileType.TransactionLog, storage.GetFileType(file.Index));
        Assert.Null(storage.GetFileType(12345));
        var data = new byte[3 * chunkSize + 17];
        new Random(5).NextBytes(data);
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(data.AsSpan(0, 1000));
        writer.WriteBlock(data.AsSpan(1000));
        writer.Flush();
        Assert.Equal((ulong)data.Length, file.GetSize());
        Assert.Equal(data.Length, writer.GetCurrentPosition());

        var reader = new MemReader(file.GetExclusiveReader());
        var all = new byte[data.Length];
        reader.ReadBlock(all);
        Assert.Equal(data, all);
        Assert.True(reader.Eof);
        Assert.Throws<EndOfStreamException>(() => reader.ReadUInt8());

        var reader2 = new MemReader(file.GetExclusiveReader());
        reader2.SkipBlock((uint)chunkSize + 5);
        Assert.Equal(data[chunkSize + 5], reader2.ReadUInt8());
        Assert.False(reader2.Eof);
        reader2.SetCurrentPosition(7);
        Assert.Equal(data[7], reader2.ReadUInt8());
        Assert.Equal(8, reader2.GetCurrentPosition());
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var r = new MemReader(file.GetExclusiveReader());
            r.SetCurrentPosition(-1);
        });
        Assert.Throws<EndOfStreamException>(() =>
        {
            var r = new MemReader(file.GetExclusiveReader());
            r.SkipBlock((uint)data.Length + 1);
        });

        var random = new byte[300];
        file.RandomRead(random, chunkSize - 100, false);
        Assert.Equal(data.AsSpan(chunkSize - 100, 300).ToArray(), random);
        file.AdvisePrefetch();

        // Truncate by moving writer position back and append again
        var writer2 = new MemWriter(file.GetExclusiveAppenderWriter());
        writer2.SetCurrentPosition(chunkSize + 3);
        Assert.Equal((ulong)(chunkSize + 3), file.GetSize());
        writer2.WriteUInt8(42);
        writer2.Flush();
        var check = new byte[1];
        file.RandomRead(check, chunkSize + 3, true);
        Assert.Equal(42, check[0]);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var w = new MemWriter(file.GetAppenderWriter());
            w.SetCurrentPosition(10 * chunkSize);
        });
        file.HardFlushTruncateSwitchToReadOnlyMode();
        file.HardFlushTruncateSwitchToDisposedMode();
    }

    [Fact]
    public void ReplicationStorageFileManagement()
    {
        using var storage = new InMemoryReplicationFileStorage();
        var odd = storage.AddFile("pvl", FileIdParity.Odd);
        Assert.Equal(1u, odd.Index % 2);
        var even = storage.AddFile("kvi", FileIdParity.Even);
        Assert.Equal(0u, even.Index % 2);
        Assert.Equal(KVFileType.PureValues, storage.GetFileType(odd.Index));
        Assert.Equal(KVFileType.KeyIndex, storage.GetFileType(even.Index));
        var imported = storage.ImportFile(100, "trl");
        Assert.Equal(100u, imported.Index);
        Assert.Throws<InvalidOperationException>(() => storage.ImportFile(100, "trl"));
        Assert.Throws<ArgumentOutOfRangeException>(() => storage.ImportFile(0, "trl"));
        Assert.True(storage.AddFile("trl").Index > 100);
        Assert.Equal(4u, storage.GetCount());
        Assert.Same(imported, storage.GetFile(100));
        Assert.Null(storage.GetFile(99));
        Assert.Equal(4, storage.Enumerate().Count());
        storage.ConcurrentTemporaryTruncate(100, 0);
        imported.Remove();
        imported.Remove();
        Assert.Null(storage.GetFile(100));
        Assert.Equal(3u, storage.GetCount());
    }

    [Fact]
    public void ReplicationStorageSimulatesDataLossOfNotHardFlushedData()
    {
        using var storage = new InMemoryReplicationFileStorage();
        var file = storage.AddFile("trl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(new byte[100]);
        writer.Flush();
        file.HardFlush();
        writer.WriteBlock(new byte[50]);
        writer.Flush();
        Assert.Equal(150ul, file.GetSize());
        storage.SimulateDataLossOfNotFlushedData();
        Assert.Equal(100ul, file.GetSize());
    }

    [Fact]
    public async Task KeyValueDBWorksOnReplicationStorageAndReportsFileStats()
    {
        using var storage = new InMemoryReplicationFileStorage();
        KeyValueDBOptions Options() => new()
        {
            FileCollection = storage, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
            FileSplitSize = 4096
        };
        using (var db = new BTreeKeyValueDB(Options()))
        {
            for (var t = 0; t < 4; t++)
            {
                using var tr = db.StartTransaction();
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 100; i++)
                {
                    cursor.CreateOrUpdateKeyValue(BitConverter.GetBytes(t == 0 ? i : i % 50), new byte[100 + t]);
                }

                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                Assert.True(cursor.FindFirstKey([]));
                cursor.EraseCurrent();
                tr.Commit();
            }

            await db.Compact(CancellationToken.None);
            db.CreateKvi(CancellationToken.None);
        }

        var stats = storage.CalcFileStats();
        Assert.Contains(":TRL", stats);
        Assert.Contains(":KVI", stats);
        Assert.Contains(":PVL", stats);
        using (var db = new BTreeKeyValueDB(Options()))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(99, tr.GetKeyValueCount());
        }

        var unknown = storage.AddFile("trl");
        var w = new MemWriter(unknown.GetAppenderWriter());
        w.WriteBlock(new byte[30]);
        w.Flush();
        Assert.Throws<InvalidDataException>(() =>
        {
            var tmp = new MemWriter(unknown.GetAppenderWriter());
            tmp.SetCurrentPosition(0);
            tmp.WriteBlock(Enumerable.Repeat((byte)200, 30).ToArray());
            tmp.Flush();
            storage.CalcFileStats();
        });
    }
}
