using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class ReplicationCompactorTest
{
    static KeyValueDBOptions Options(InMemoryReplicationFileStorage files, bool replicated = true) => new()
    {
        FileCollection = replicated ? new LocalReplicatedCollection(files) : files, CompactorScheduler = null, Compression = new NoCompressionStrategy(),
        FileSplitSize = 4096
    };

    static async Task Write(BTreeKeyValueDB db, byte key, byte value)
    {
        using var tr = await db.StartWritingTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], Enumerable.Repeat(value, 1000).ToArray());
        tr.Commit();
    }

    static void Read(IKeyValueDBTransaction tr, byte key, byte value)
    {
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindExactKey([key]));
        Span<byte> buffer = default;
        Assert.Equal(Enumerable.Repeat(value, 1000), cursor.GetValueSpan(ref buffer).ToArray());
    }

    static uint[] Pvls(InMemoryReplicationFileStorage files) => files.Enumerate()
        .Where(f => files.GetFileType(f.Index) == KVFileType.PureValues).Select(f => f.Index).ToArray();

    [Fact]
    public async Task CompactionPreservesIntermediateReadersAndReleasesFilesAfterTheirDisposal()
    {
        using var files = new InMemoryReplicationFileStorage();
        using var db = await BTreeKeyValueDB.OpenAsync(Options(files));
        for (byte key = 0; key < 40; key++) await Write(db, key, key);
        for (byte key = 0; key < 40; key += 2) await Write(db, key, 100);
        Assert.True(await db.Compact(default));
        var originalPvls = Pvls(files);
        Assert.NotEmpty(originalPvls);
        using var firstReader = db.StartReadOnlyTransaction();
        for (byte key = 1; key < 40; key += 4) await Write(db, key, 110);
        Assert.True(await db.Compact(default));
        var intermediatePvls = Pvls(files).Except(originalPvls).ToArray();
        Assert.NotEmpty(intermediatePvls);
        using var intermediateReader = db.StartReadOnlyTransaction();
        for (byte key = 0; key < 40; key++) await Write(db, key, 120);
        await db.Compact(default);
        await db.Compact(default);
        Read(firstReader, 0, 100);
        Read(intermediateReader, 3, 3);
        Assert.All(intermediatePvls, id => Assert.NotNull(files.GetFile(id)));
        intermediateReader.Dispose();
        await db.Compact(default);
        Assert.All(intermediatePvls, id => Assert.Null(files.GetFile(id)));
        Read(firstReader, 0, 100);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LoadedKviRetainsPvlAndOldTrlUntilLastReaderReleasesThem(bool compactToPvls)
    {
        using var files = new InMemoryReplicationFileStorage();
        using (var seed = new BTreeKeyValueDB(Options(files, replicated: false)))
        {
            for (byte key = 0; key < 40; key++) await Write(seed, key, key);
            if (compactToPvls)
            {
                for (byte key = 0; key < 40; key += 2) await Write(seed, key, 100);
                Assert.True(await seed.Compact(default));
            }
            seed.CreateKvi(default);
        }
        using var db = await BTreeKeyValueDB.OpenAsync(Options(files));
        using var reader = db.StartReadOnlyTransaction();
        var sources = compactToPvls ? Pvls(files) : files.Enumerate()
            .Where(f => files.GetFileType(f.Index) == KVFileType.TransactionLog).Select(f => f.Index).Order().SkipLast(1).ToArray();
        Assert.NotEmpty(sources);
        var kvis = files.Enumerate().Where(f => files.GetFileType(f.Index) == KVFileType.KeyIndex)
            .Select(f => f.Index).ToArray();
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.EraseAll([]);
            tr.Commit();
        }
        await db.Compact(default);
        Assert.All(sources, id => Assert.NotNull(files.GetFile(id)));
        Read(reader, 1, 1);
        reader.Dispose();
        await db.Compact(default);
        Assert.All(sources, id => Assert.Null(files.GetFile(id)));
        Assert.Equal(kvis, files.Enumerate().Where(f => files.GetFileType(f.Index) == KVFileType.KeyIndex)
            .Select(f => f.Index).ToArray()); // Local compaction never writes a replacement KVI.
    }

    [Fact]
    public async Task KviReferencesSurvivePointerRewritesAtTheSameTrlPosition()
    {
        using var files = new InMemoryReplicationFileStorage();
        using var seedFiles = new InMemoryReplicationFileStorage();
        using (var seed = new BTreeKeyValueDB(Options(seedFiles, replicated: false)))
        {
            for (byte key = 0; key < 40; key++) await Write(seed, key, key);
            for (byte key = 0; key < 40; key += 2) await Write(seed, key, 100);
            seed.CreateKvi(default);
            // Copy the KVI cut before Dispose appends a temporary end marker to the TRL.
            foreach (var source in seedFiles.Enumerate())
            {
                var bytes = new byte[checked((int)source.GetSize())];
                source.RandomRead(bytes, 0, false);
                var hint = seedFiles.GetFileType(source.Index) == KVFileType.KeyIndex ? "kvi" : "trl";
                var target = files.ImportFile(source.Index, hint);
                var writer = new MemWriter(target.GetAppenderWriter());
                writer.WriteBlock(bytes);
                writer.Flush();
            }
        }

        using var db = await BTreeKeyValueDB.OpenAsync(Options(files));
        uint[] originalSources;
        (uint FileId, uint Offset) position;
        using (var snapshot = db.CaptureKeyIndexSnapshot())
        {
            originalSources = snapshot.Sources.Select(s => s.FileId).ToArray();
            position = (snapshot.TransactionLogFileId, snapshot.TransactionLogOffset);
        }
        Assert.True(await db.Compact(default));
        using (var snapshot = db.CaptureKeyIndexSnapshot())
            Assert.Equal(position, (snapshot.TransactionLogFileId, snapshot.TransactionLogOffset));
        var rewrittenSources = Pvls(files);
        Assert.NotEmpty(rewrittenSources);
        using var reader = db.StartReadOnlyTransaction();
        await db.Compact(default);
        Assert.All(originalSources, id => Assert.NotNull(files.GetFile(id)));
        Assert.All(rewrittenSources, id => Assert.NotNull(files.GetFile(id)));
        Read(reader, 0, 100);
        Read(reader, 1, 1);

        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.EraseAll([]);
            tr.Commit();
        }
        await db.Compact(default);
        Assert.All(originalSources, id => Assert.NotNull(files.GetFile(id)));
        Assert.All(rewrittenSources, id => Assert.NotNull(files.GetFile(id)));
        Read(reader, 1, 1);
        reader.Dispose();
        await db.Compact(default);
        Assert.All(rewrittenSources, id => Assert.Null(files.GetFile(id)));
        // The current TRL remains open; older KVI value sources can now be released.
        Assert.All(originalSources.Where(id => id < originalSources.Max()),
            id => Assert.Null(files.GetFile(id)));
    }

    [Fact]
    public async Task ExportSnapshotPinsOldTrlAcrossConcurrentCompactionRequests()
    {
        using var files = new InMemoryReplicationFileStorage();
        using var db = await BTreeKeyValueDB.OpenAsync(Options(files));
        for (byte key = 0; key < 40; key++) await Write(db, key, key);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        var first = snapshot.Sources.Min(s => s.FileId);
        for (byte key = 0; key < 40; key++) await Write(db, key, 100);
        await Task.WhenAll(db.Compact(default).AsTask(), db.Compact(default).AsTask());
        Assert.All(snapshot.Sources, s => Assert.NotNull(files.GetFile(s.FileId)));
        snapshot.Dispose();
        await Task.WhenAll(db.Compact(default).AsTask(), db.Compact(default).AsTask());
        Assert.Null(files.GetFile(first));
        Assert.DoesNotContain(files.Enumerate(), f => files.GetFileType(f.Index) == KVFileType.KeyIndex);
        using var reader = db.StartReadOnlyTransaction();
        Read(reader, 0, 100);
    }

    [Theory]
    [InlineData("hid")]
    [InlineData("hpv")]
    public async Task ReplicationRejectsSubDatabaseFiles(string hint)
    {
        using var files = new InMemoryReplicationFileStorage();
        files.AddFile(hint);
        await Assert.ThrowsAsync<NotSupportedException>(() => BTreeKeyValueDB.OpenAsync(Options(files)).AsTask());
        Assert.Equal(1u, files.GetCount());
    }

    [Fact]
    public async Task ReplicationRejectsCreatingSubDatabases()
    {
        using var files = new InMemoryReplicationFileStorage();
        using var db = await BTreeKeyValueDB.OpenAsync(Options(files));
        Assert.Throws<NotSupportedException>(() => db.GetSubDB<IChunkStorage>(1));
        Assert.Equal(0u, files.GetCount());
    }
}
