using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class TrlAllocationTest
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DifferentRetainedLegacyFilesDoNotChangeNewTrlIdsOrBytes(bool prepareLegacyBase)
    {
        using var remote = new CheckpointPublisherTest.Storage();
        SeedHeader(remote.Files, 1, prepareLegacyBase ? 42 : 0);
        remote.Types.Add(1, KVFileType.TransactionLog);
        if (prepareLegacyBase)
        {
            remote.Files.ImportFile(101, "pvl");
            remote.Types.Add(101, KVFileType.PureValues);
            using var preparationLocal = new InMemoryReplicationFileStorage();
            await using var preparationFiles = new ReplicationFileSet(preparationLocal, remote);
            await preparationFiles.InitializeAsync();
            using var preparation = await Open(preparationFiles, new TransactionLogCapture());
            Assert.Equal(103u, preparation.ReplicationRestoredPosition.FileId);
            Assert.Equal(preparation.ReplicationRestoredPosition.Offset, remote.Files.GetFile(103)!.GetSize());
        }
        using var firstLocal = new InMemoryReplicationFileStorage();
        await using var firstFiles = new ReplicationFileSet(firstLocal, remote);
        await firstFiles.InitializeAsync();
        var writer = new MemWriter(remote.Files.ImportFile(501, "pvl").GetAppenderWriter());
        writer.WriteBlock([1, 2, 3]);
        writer.Flush();
        remote.Types.Add(501, KVFileType.PureValues);
        using var secondLocal = new InMemoryReplicationFileStorage();
        await using var secondFiles = new ReplicationFileSet(secondLocal, remote);
        await secondFiles.InitializeAsync();
        var firstCapture = new TransactionLogCapture();
        var secondCapture = new TransactionLogCapture();
        using var first = await Open(firstFiles, firstCapture);
        using var second = await Open(secondFiles, secondCapture);
        for (ulong id = 1; id <= 8; id++)
        {
            await Write(first, id);
            await Write(second, id);
        }
        Assert.Equal(firstCapture.Completed, secondCapture.Completed);
        var logs = firstLocal.Enumerate().Where(f => firstLocal.GetFileType(f.Index) == KVFileType.TransactionLog)
            .OrderBy(f => f.Index).ToArray();
        Assert.True(logs.Length > 2);
        var newLogs = prepareLegacyBase ? logs.Skip(1).ToArray() : logs;
        Assert.Equal(Enumerable.Range(0, newLogs.Length).Select(i => newLogs[0].Index + (uint)i * 2), newLogs.Select(f => f.Index));
        foreach (var log in logs)
        {
            var other = secondLocal.GetFile(log.Index)!;
            Assert.Equal(log.GetSize(), other.GetSize());
            var bytes = new byte[checked((int)log.GetSize())];
            var otherBytes = new byte[bytes.Length];
            log.RandomRead(bytes, 0, false);
            other.RandomRead(otherBytes, 0, false);
            Assert.Equal(bytes, otherBytes);
        }
    }

    [Theory]
    [InlineData(100u)]
    [InlineData(101u)]
    public async Task LegacyOpenCreatesHeaderBeforeAnyApplicationWrite(uint legacyId)
    {
        using var files = new InMemoryReplicationFileStorage();
        SeedHeader(files, legacyId, 42);
        var before = files.GetFile(legacyId)!.GetSize();
        var capture = new TransactionLogCapture();
        using var db = await Open(new LocalReplicatedCollection(files), capture);
        var nextId = (legacyId + 1) | 1u;
        var next = files.GetFile(nextId)!;
        Assert.Equal(0, FileCollectionWithFileInfos.ReadFileInfo(next).Generation);
        Assert.Equal(new TransactionLogPosition(nextId, checked((uint)next.GetSize())), db.ReplicationRestoredPosition);
        Assert.Equal(default, capture.Completed);
        Assert.Equal(before, files.GetFile(legacyId)!.GetSize());
        Assert.Equal(2, files.Enumerate().Count());
        using var read = db.StartReadOnlyTransaction();
        Assert.Equal(0ul, read.GetCommitUlong());
        Assert.Equal(0, read.GetKeyValueCount());
    }

    [Theory]
    [InlineData(100u)]
    [InlineData(101u)]
    public async Task LegacyTransitionSkipsReservedIdsOnlyOnceIncludingAfterReopen(uint legacyId)
    {
        using var files = new InMemoryReplicationFileStorage();
        SeedHeader(files, legacyId, 42);
        files.ImportFile(201, "pvl");
        // Even allocations cannot affect the odd sequence, including the one-time transition.
        files.ImportFile(10000, "pvl");
        var capture = new TransactionLogCapture();
        using (var db = await Open(new LocalReplicatedCollection(files), capture))
        {
            for (ulong id = 1; id <= 6; id++) await Write(db, id);
        }
        var logs = files.Enumerate().Where(f => files.GetFileType(f.Index) == KVFileType.TransactionLog)
            .OrderBy(f => f.Index).ToArray();
        Assert.True(logs.Length >= 3);
        Assert.Equal(203u, logs[1].Index);
        Assert.Equal(legacyId, ((IFileTransactionLog)FileCollectionWithFileInfos.ReadFileInfo(logs[1])).PreviousFileId);
        foreach (var file in logs.Skip(1)) Assert.Equal(0, FileCollectionWithFileInfos.ReadFileInfo(file).Generation);
        var previousTail = capture.Completed.FileId;
        files.GetFile(201)!.Remove();
        files.ImportFile(501, "pvl");
        using (var db = await Open(new LocalReplicatedCollection(files), capture))
        {
            for (ulong id = 7; id <= 10; id++) await Write(db, id);
            using var read = db.StartReadOnlyTransaction();
            Assert.Equal(10, read.GetKeyValueCount());
        }
        var successors = files.Enumerate().Where(f => files.GetFileType(f.Index) == KVFileType.TransactionLog && f.Index > previousTail)
            .Select(f => f.Index).Order().ToArray();
        Assert.NotEmpty(successors);
        Assert.Equal(Enumerable.Range(1, successors.Length).Select(i => previousTail + (uint)i * 2), successors);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConcurrentStartupAndRestartKeepThePublishedHeader(bool loseResponse)
    {
        using var remote = new CheckpointPublisherTest.Storage();
        SeedHeader(remote.Files, 2, 42);
        remote.Types.Add(2, KVFileType.TransactionLog);
        remote.Files.ImportFile(101, "pvl");
        remote.Types.Add(101, KVFileType.PureValues);
        using var firstLocal = new InMemoryReplicationFileStorage();
        using var secondLocal = new InMemoryReplicationFileStorage();
        // Different untrusted caches must not influence the shared transition ID.
        firstLocal.ImportFile(501, "pvl");
        secondLocal.ImportFile(701, "pvl");
        await using var firstFiles = new ReplicationFileSet(firstLocal, remote);
        await using var secondFiles = new ReplicationFileSet(secondLocal, remote);
        await firstFiles.InitializeAsync();
        await secondFiles.InitializeAsync(); // Both discover before either creates the successor.
        remote.CancelAfterTrl = loseResponse;
        if (loseResponse)
            await Assert.ThrowsAsync<OperationCanceledException>(() => Open(firstFiles, new()).AsTask());
        else
        {
            using var first = await Open(firstFiles, new());
            Assert.Equal(103u, first.ReplicationRestoredPosition.FileId);
        }
        remote.CancelAfterTrl = false;
        var secondCapture = new TransactionLogCapture();
        using var second = await Open(secondFiles, secondCapture);
        var boundary = second.ReplicationRestoredPosition;
        Assert.Equal(103u, boundary.FileId);
        Assert.Equal((ulong)boundary.Offset, remote.Files.GetFile(103)!.GetSize());
        // No application commit is needed to preserve the transition over a fresh restore.
        using var restartedLocal = new InMemoryReplicationFileStorage();
        await using var restartedFiles = new ReplicationFileSet(restartedLocal, remote);
        await restartedFiles.InitializeAsync();
        var restartedCapture = new TransactionLogCapture();
        using var restarted = await Open(restartedFiles, restartedCapture);
        Assert.Equal(boundary, restarted.ReplicationRestoredPosition);
        for (ulong id = 1; id <= 6; id++)
        {
            await Write(second, id);
            await Write(restarted, id);
        }
        Assert.Equal(secondCapture.Completed, restartedCapture.Completed);
        foreach (var log in secondLocal.Enumerate().Where(f => secondLocal.GetFileType(f.Index) == KVFileType.TransactionLog))
        {
            var other = restartedLocal.GetFile(log.Index)!;
            Assert.Equal(log.GetSize(), other.GetSize());
            var expected = new byte[checked((int)log.GetSize())];
            var actual = new byte[expected.Length];
            log.RandomRead(expected, 0, false);
            other.RandomRead(actual, 0, false);
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task StaleInventoryCannotPublishAnotherTransitionIdAfterCleanup()
    {
        using var remote = new CheckpointPublisherTest.Storage();
        SeedHeader(remote.Files, 2, 42);
        remote.Types.Add(2, KVFileType.TransactionLog);
        remote.Files.ImportFile(101, "pvl");
        remote.Types.Add(101, KVFileType.PureValues);
        using var local = new InMemoryReplicationFileStorage();
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        using var first = await Open(files, new());
        Assert.Equal(103u, first.ReplicationRestoredPosition.FileId);
        // Another node saw the old TRL listing, then listed immutable files after cleanup removed 101.pvl.
        // Its selected snapshot omits 103.trl and would otherwise choose 3.trl.
        remote.Files.GetFile(101)!.Remove();
        remote.Types.Remove(101);
        using var staleLocal = new InMemoryReplicationFileStorage();
        remote.OmittedInventoryId = 103;
        await using var stale = new ReplicationFileSet(staleLocal, remote);
        await stale.InitializeAsync();
        await Assert.ThrowsAsync<IOException>(() => Open(stale, new()).AsTask());
        Assert.Null(remote.Files.GetFile(3));
        Assert.NotNull(remote.Files.GetFile(103));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HeaderOnlyTransitionPreservesLegacyDataAndCanActivateAfterRestart(bool disk)
    {
        using var remote = new CheckpointPublisherTest.Storage();
        using (var seed = new InMemoryReplicationFileStorage())
        {
            using (var db = new BTreeKeyValueDB(new KeyValueDBOptions
                   { FileCollection = seed, Compression = new NoCompressionStrategy(), CompactorScheduler = null }))
                await Write(db, 7);
            foreach (var source in seed.Enumerate())
                Assert.Equal(TrlWriteOutcome.Applied, (await remote.WriteAsync(
                    new(source.Index, TrlFileName.Key(source.Index), null, 0, (uint)source.GetSize(), source), default)).Outcome);
        }
        var path = Path.Combine(Path.GetTempPath(), "btdb-transition-" + Guid.NewGuid());
        Directory.CreateDirectory(path);
        try
        {
            using (IReplicationFileStorage local = disk ? new OnDiskReplicationFileStorage(path) : new InMemoryReplicationFileStorage())
            await using (var files = new ReplicationFileSet(local, remote))
            {
                await files.InitializeAsync();
                using var db = await Open(files, new());
                Assert.Equal(3u, db.ReplicationRestoredPosition.FileId);
            }
            using IReplicationFileStorage restartedLocal = disk ? new OnDiskReplicationFileStorage(path) : new InMemoryReplicationFileStorage();
            await using var restartedFiles = new ReplicationFileSet(restartedLocal, remote);
            await restartedFiles.InitializeAsync();
            var capture = new TransactionLogCapture();
            using var restarted = await Open(restartedFiles, capture);
            var boundary = restarted.ReplicationRestoredPosition;
            Assert.Equal(3u, boundary.FileId);
            Assert.Equal((ulong)boundary.Offset, remote.Files.GetFile(3)!.GetSize());
            using (var read = restarted.StartReadOnlyTransaction())
            {
                Assert.Equal(7ul, read.GetCommitUlong());
                Assert.Equal(1, read.GetKeyValueCount());
                using var cursor = read.CreateCursor();
                Assert.True(cursor.FindExactKey([7]));
                Span<byte> buffer = default;
                Assert.Equal(Enumerable.Repeat((byte)7, 700), cursor.GetValueSpan(ref buffer).ToArray());
            }
            var selected = new SelectedLeadership(CheckpointPublisherTest.CreateAuthority(), 2, "restarted", ["main"]);
            var publishers = await LeadershipActivation.ActivateAsync(selected,
                [new("main", restarted, capture, remote, new("1.trl", 1), boundary, TrlFileName.Key)]);
            using var publisher = Assert.Single(publishers!);
            await Write(restarted, 8);
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
            Assert.Equal(capture.Completed, publisher.PublishedPosition);
        }
        finally { Directory.Delete(path, true); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StartupRejectsConflictingOrAlreadyExtendedTransition(bool extended)
    {
        using var remote = new CheckpointPublisherTest.Storage();
        SeedHeader(remote.Files, 2, 42);
        remote.Types.Add(2, KVFileType.TransactionLog);
        using var local = new InMemoryReplicationFileStorage();
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        // A competing startup publishes after our inventory was selected.
        using var competing = new InMemoryReplicationFileStorage();
        var source = competing.ImportFile(3, "trl");
        var writer = new MemWriter(source.GetAppenderWriter());
        writer.WriteBlock("BTDB3"u8);
        writer.WriteGuid(new Guid("5d076258-e492-4931-a5b8-a19dc9fe6c76"));
        writer.WriteUInt8((byte)KVFileType.TransactionLog);
        writer.WriteVInt64(0);
        writer.WriteVInt32(extended ? 2 : 1); // Different predecessor is a conflicting history.
        if (extended) writer.WriteUInt8(0);
        writer.Flush();
        Assert.Equal(TrlWriteOutcome.Applied,
            (await remote.WriteAsync(new(3, "3.trl", null, 0, (uint)source.GetSize(), source), default)).Outcome);
        var version = await remote.ReadAsync("3.trl", default);
        if (extended) await Assert.ThrowsAsync<IOException>(() => Open(files, new()).AsTask());
        else await Assert.ThrowsAsync<InvalidDataException>(() => Open(files, new()).AsTask());
        Assert.Equal(version, await remote.ReadAsync("3.trl", default));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CollisionDoesNotSkipOrOverwriteTheRequiredSuccessor(bool downloaded)
    {
        using var remote = new CheckpointPublisherTest.Storage();
        SeedHeader(remote.Files, 1, 0);
        remote.Types.Add(1, KVFileType.TransactionLog);
        remote.Files.ImportFile(3, "pvl");
        remote.Types.Add(3, KVFileType.PureValues);
        using var local = new InMemoryReplicationFileStorage();
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        if (downloaded) await files.PrefetchAsync(3);
        var capture = new TransactionLogCapture();
        using var db = await Open(files, capture);
        await Write(db, 1);
        await Write(db, 2);
        var end = capture.Completed;
        await Assert.ThrowsAsync<IOException>(() => Write(db, 3));
        Assert.Equal(end, capture.Completed);
        Assert.Equal((ulong)end.Offset, local.GetFile(1)!.GetSize());
        Assert.Null(local.GetFile(5));
        Assert.Equal(KVFileType.PureValues, files.GetFileType(3));
        Assert.Equal(0ul, remote.Files.GetFile(3)!.GetSize());
    }

    [Fact]
    public async Task InvalidSizePolicyDoesNotConsumeTheSuccessorId()
    {
        using var files = new InMemoryReplicationFileStorage();
        SeedHeader(files, 1, 0);
        var policy = new InvalidSuccessorPolicy();
        var capture = new TransactionLogCapture();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = capture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null, TransactionLogSizeStrategy = policy
        });
        await Write(db, 1);
        await Write(db, 2);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Write(db, 3));
        Assert.Null(files.GetFile(3));
        // Fault injection only: production strategies are immutable.
        policy.Reject = false;
        await Write(db, 3);
        Assert.Equal(3u, capture.Completed.FileId);
    }

    sealed class InvalidSuccessorPolicy : ITransactionLogSizeStrategy
    {
        public bool Reject = true;
        public TransactionLogSizeLimits GetLimits(uint id) => id == 3 && Reject ? new(0, 0) : new(1024, 1536);
    }

    [Fact]
    public async Task ExhaustedTrlIdsDoNotWrapOrChangeTheCommittedTail()
    {
        using var files = new InMemoryReplicationFileStorage();
        SeedHeader(files, uint.MaxValue - 2, 0);
        var capture = new TransactionLogCapture();
        using var db = await Open(new LocalReplicatedCollection(files), capture);
        await Write(db, 1);
        await Write(db, 2);
        await Write(db, 3);
        await Write(db, 4);
        var end = capture.Completed;
        Assert.Equal(uint.MaxValue, end.FileId);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Write(db, 5));
        Assert.Equal(end, capture.Completed);
        Assert.Equal((ulong)end.Offset, files.GetFile(uint.MaxValue)!.GetSize());
        Assert.Equal(2, files.Enumerate().Count());
    }

    static void SeedHeader(InMemoryReplicationFileStorage files, uint id, long generation)
    {
        var writer = new MemWriter(files.ImportFile(id, "trl").GetAppenderWriter());
        writer.WriteBlock("BTDB3"u8);
        writer.WriteGuid(new Guid("5d076258-e492-4931-a5b8-a19dc9fe6c76"));
        writer.WriteUInt8((byte)KVFileType.TransactionLog);
        writer.WriteVInt64(generation);
        writer.WriteVInt32(0);
        writer.Flush();
    }

    static ValueTask<BTreeKeyValueDB> Open(IFileReplicatedCollection files, TransactionLogCapture capture) =>
        BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = capture, Compression = new NoCompressionStrategy(),
            CompactorScheduler = null, TransactionLogSizeStrategy = new TrlPrefixComparerTest.TinyLogs()
        });

    static async Task Write(BTreeKeyValueDB db, ulong id)
    {
        using var transaction = await db.StartWritingTransaction(id);
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([(byte)id], Enumerable.Repeat((byte)id, 700).ToArray());
        transaction.Commit();
    }
}
