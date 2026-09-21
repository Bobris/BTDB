using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class RestartRecoveryTest
{
    static KeyValueDBOptions Options(IFileCollection files, TransactionLogCapture? capture = null) => new()
    {
        FileCollection = files, TransactionLogCapture = capture,
        Compression = new NoCompressionStrategy(), CompactorScheduler = null, FileSplitSize = 8096
    };

    static async Task<CheckpointPublisherTest.Storage> PublishAndStop(bool compressed)
    {
        using var remote = new CheckpointPublisherTest.Storage();
        using (var local = new InMemoryReplicationFileStorage())
        {
            var capture = new TransactionLogCapture();
            using var db = await CheckpointPublisherTest.OpenForPublication(local, capture, compressed);
            await CheckpointPublisherTest.Populate(db);
            using var canonical = CheckpointPublisherTest.CreateCanonical(db, capture, remote);
            await using var files = new ReplicationFileSet(local, remote);
            using var snapshot = db.CaptureKeyIndexSnapshot();
            Assert.Equal(CheckpointPublishResult.Published, await new CheckpointPublisher(files, canonical).PublishAsync(snapshot));

            // Model cleanup of history no longer referenced by the published checkpoint. Genesis must be gone,
            // so recovery cannot accidentally pass by replaying all TRLs from the beginning.
            var genesisId = remote.Types.Where(p => p.Value == KVFileType.TransactionLog).Min(p => p.Key);
            var requiredTrls = snapshot.Sources.Where(s => s.FileType == KVFileType.TransactionLog)
                .Select(s => s.FileId).ToHashSet();
            foreach (var (id, type) in remote.Types.ToArray())
                if (type == KVFileType.TransactionLog && id < snapshot.TransactionLogFileId && !requiredTrls.Contains(id))
                {
                    remote.Files.GetFile(id)!.Remove();
                    remote.Types.Remove(id);
                }
            Assert.Null(remote.Files.GetFile(genesisId));

            await Write(db, 63, 250, "after checkpoint"u8.ToArray());
            await Write(db, 64, 251, new byte[24000], rollback: true);
            Assert.Equal(TrlPublishResult.Published, await canonical.PublishNextAsync());
            // A local-only suffix must disappear with the old process, not become restart authority.
            await Write(db, 65, 252, "unpublished"u8.ToArray());
        }
        return remote.CopyPersistedObjects();
    }

    static async Task Write(BTreeKeyValueDB db, ulong eventId, byte key, byte[] value, bool rollback = false)
    {
        using var tr = await db.StartWritingTransaction(eventId);
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], value);
        if (!rollback) tr.Commit();
    }

    static void AssertContents(BTreeKeyValueDB db, bool resumed)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.Equal(resumed ? 65ul : 63ul, tr.GetCommitUlong());
        Assert.Equal(resumed ? 65L : 64L, tr.GetKeyValueCount());
        Assert.Equal(1234ul, tr.GetUlong(2));
        for (byte key = 0; key < 60; key++)
        {
            Assert.True(cursor.FindExactKey([key]));
            Span<byte> buffer = default;
            Assert.Equal(Enumerable.Repeat(key % 2 == 0 ? (byte)0 : key, 2000), cursor.GetValueSpan(ref buffer).ToArray());
        }
        Assert.True(cursor.FindExactKey([200]));
        Span<byte> value = default;
        Assert.Equal(new byte[2000], cursor.GetValueSpan(ref value).ToArray());
        Assert.True(cursor.FindExactKey([201]));
        Assert.Equal("inline"u8.ToArray(), cursor.GetValueSpan(ref value).ToArray());
        Assert.True(cursor.FindExactKey([202]));
        Assert.Empty(cursor.GetValueSpan(ref value).ToArray());
        Assert.True(cursor.FindExactKey([250]));
        Assert.Equal("after checkpoint"u8.ToArray(), cursor.GetValueSpan(ref value).ToArray());
        Assert.Equal(resumed, cursor.FindExactKey([251]));
        if (resumed) Assert.Equal("after restart"u8.ToArray(), cursor.GetValueSpan(ref value).ToArray());
        Assert.False(cursor.FindExactKey([252]));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task RestartFromCheckpointAfterHistoryCleanupResumesPublication(bool compressed, bool staleCache)
    {
        using var remote = await PublishAndStop(compressed);
        using (var cache = new InMemoryReplicationFileStorage())
        {
            if (staleCache)
            {
                foreach (var file in remote.Files.Enumerate())
                {
                    // Same-size corrupt files cannot be reused merely because ID and length match.
                    var hint = remote.Types[file.Index] switch
                    {
                        KVFileType.TransactionLog => "trl", KVFileType.PureValues => "pvl", _ => "kvi"
                    };
                    var writer = new MemWriter(cache.ImportFile(file.Index, hint).GetAppenderWriter());
                    writer.WriteBlock(new byte[checked((int)file.GetSize())]);
                    writer.Flush();
                }
                cache.ImportFile(10001, "trl"); // Unselected local suffix, not a remotely published file.
            }
            await using var files = new ReplicationFileSet(cache, remote);
            await files.InitializeAsync();
            Assert.Null(cache.GetFile(10001));
            var capture = new TransactionLogCapture();
            using var db = await BTreeKeyValueDB.OpenAsync(Options(files, capture));
            AssertContents(db, resumed: false);

            // Recover the publisher's conditional-write token/term from the remote object, never from an old
            // publisher or snapshot. The fixture's native file-to-object naming is unchanged across sessions.
            var tailId = files.RemoteEnumerate().Where(f => files.GetFileType(f.Index) == KVFileType.TransactionLog)
                .Max(f => f.Index);
            var key = $"trl/{tailId}";
            var state = await remote.ReadAsync(key, CancellationToken.None);
            Assert.NotNull(state);
            Assert.Null(state.Metadata.Next);
            Assert.Equal((ulong)state.Length, db.FileCollection.GetFile(tailId)!.GetSize());
            using var publisher = new CanonicalTrlPublisher(db, capture, remote, CheckpointPublisherTest.CreateAuthority(),
                2, id => $"trl/{id}", new(tailId, key, state));
            Assert.Equal(TrlPublishResult.Adopted, await publisher.PublishNextAsync());
            await Write(db, 65, 251, "after restart"u8.ToArray());
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
            using var snapshot = db.CaptureKeyIndexSnapshot();
            Assert.Equal(CheckpointPublishResult.Published, await new CheckpointPublisher(files, publisher).PublishAsync(snapshot));
        }

        // A second entirely new process proves the resumed transaction and checkpoint were actually published.
        using var persisted = remote.CopyPersistedObjects();
        using var freshCache = new InMemoryReplicationFileStorage();
        await using var restoredFiles = new ReplicationFileSet(freshCache, persisted);
        await restoredFiles.InitializeAsync();
        using var restored = await BTreeKeyValueDB.OpenAsync(Options(restoredFiles));
        AssertContents(restored, resumed: true);
    }
}
