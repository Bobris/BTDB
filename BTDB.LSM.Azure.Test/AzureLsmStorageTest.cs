using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using BTDB.Replication.Azure.Test;
using BTDBTest;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Azure.Test;

/// Conformance of AzureLsmStorage with the ILsmStorage contract, on Azurite or on live Azure when
/// BTDB_AZURE_BLOB_ENDPOINT is set.
public class AzureLsmStorageTest(BlobStorageFixture fixture) : IClassFixture<BlobStorageFixture>
{
    async Task<AzureLsmStorage> Storage() => new(await fixture.ContainerAsync(),
        new() { SingleUploadLimit = 64 * 1024, BlockSize = 16 * 1024 });

    [Fact]
    public async Task CreateIsCreateIfAbsent()
    {
        var storage = await Storage();
        Assert.Equal(LsmWriteOutcome.Applied, await storage.CreateAsync("a/1", new byte[] { 1, 2, 3 }, default));
        Assert.Equal(LsmWriteOutcome.Rejected, await storage.CreateAsync("a/1", new byte[] { 9 }, default));
        Assert.Equal(new byte[] { 1, 2, 3 }, await storage.TryReadAsync("a/1", default));
        Assert.Null(await storage.TryReadAsync("a/2", default));
    }

    [Fact]
    public async Task LargeObjectsAreStagedAndStillCreateIfAbsent()
    {
        var storage = await Storage();
        var content = new byte[200 * 1024];
        new Random(1).NextBytes(content);
        Assert.Equal(LsmWriteOutcome.Applied, await storage.CreateAsync("big", content, default));
        Assert.Equal(LsmWriteOutcome.Rejected, await storage.CreateAsync("big", content, default));
        Assert.Equal(content, await storage.TryReadAsync("big", default));
        var range = new byte[1000];
        storage.ReadRange("big", 150_000, range);
        Assert.Equal(content.AsSpan(150_000, 1000).ToArray(), range);
        var asyncRange = new byte[777];
        await storage.ReadRangeAsync("big", 12_345, asyncRange, default);
        Assert.Equal(content.AsSpan(12_345, 777).ToArray(), asyncRange);
    }

    [Fact]
    public async Task MissingObjectRangeReadThrowsFileNotFound()
    {
        var storage = await Storage();
        Assert.Throws<FileNotFoundException>(() => storage.ReadRange("missing", 0, new byte[1]));
        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await storage.ReadRangeAsync("missing", 0, new byte[1], default));
    }

    [Fact]
    public async Task RangeBeyondTheEndThrowsEndOfStream()
    {
        var storage = await Storage();
        await storage.CreateAsync("short", new byte[100], default);
        Assert.Throws<EndOfStreamException>(() => storage.ReadRange("short", 90, new byte[20]));
        Assert.Throws<EndOfStreamException>(() => storage.ReadRange("short", 200, new byte[20]));
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await storage.ReadRangeAsync("short", 90, new byte[20], default));
    }

    [Fact]
    public async Task ListIsOrderedByKeyAndDeleteAndPutWork()
    {
        var storage = await Storage();
        foreach (var name in new[] { "p/0003", "p/0001", "q/0001", "p/0002" })
            await storage.CreateAsync(name, new byte[] { 1 }, default);
        var listed = new List<string>();
        await foreach (var info in storage.ListAsync("p/", default)) listed.Add(info.Key);
        Assert.Equal(new[] { "p/0001", "p/0002", "p/0003" }, listed);
        await storage.DeleteAsync("p/0002", default);
        await storage.DeleteAsync("p/0002", default);
        Assert.Null(await storage.TryReadAsync("p/0002", default));
    }

    [Fact]
    public async Task LeaseIsExclusiveUntilReleased()
    {
        var storage = await Storage();
        var duration = TimeSpan.FromSeconds(15);
        Assert.True(await storage.TryAcquireLeaseAsync("LOCK", "11111111-1111-1111-1111-111111111111", duration, default));
        Assert.False(await storage.TryAcquireLeaseAsync("LOCK", "22222222-2222-2222-2222-222222222222", duration, default));
        Assert.True(await storage.RenewLeaseAsync("LOCK", "11111111-1111-1111-1111-111111111111", duration, default));
        Assert.False(await storage.RenewLeaseAsync("LOCK", "22222222-2222-2222-2222-222222222222", duration, default));
        await storage.ReleaseLeaseAsync("LOCK", "11111111-1111-1111-1111-111111111111", default);
        Assert.True(await storage.TryAcquireLeaseAsync("LOCK", "22222222-2222-2222-2222-222222222222", duration, default));
    }

    [Theory]
    [InlineData(10_000)]
    [InlineData(61_000)]
    [InlineData(15_500)]
    public async Task LeaseDurationsAzureCannotHonorAreRejected(int milliseconds)
    {
        var storage = await Storage();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await storage.TryAcquireLeaseAsync("LOCK", "11111111-1111-1111-1111-111111111111",
                TimeSpan.FromMilliseconds(milliseconds), default));
    }
}

/// The shared IKeyValueDB suite on Azure storage.
public class AzureLsmKeyValueDBTest : KeyValueDBTestBase, IClassFixture<BlobStorageFixture>
{
    readonly BlobStorageFixture _fixture;

    public AzureLsmKeyValueDBTest(BlobStorageFixture fixture, ITestOutputHelper output) : base(output)
    {
        _fixture = fixture;
    }

    protected override IKeyValueDB NewKeyValueDB() => LsmKeyValueDB.OpenAsync(new()
    {
        Storage = new AzureLsmStorage(_fixture.ContainerAsync().GetAwaiter().GetResult()),
        AcquireWriterLeaseOnOpen = true, MemtableSize = 64 * 1024, BlockSize = 4096, Level0CompactionTrigger = 2,
        LevelBaseBytes = 256 * 1024, LevelMultiplier = 4, LevelCount = 4, TargetFileSize = 64 * 1024,
        WalFlushInterval = TimeSpan.FromMilliseconds(2)
    }).AsTask().GetAwaiter().GetResult();
}

/// Crash recovery and a reader instance on Azure storage.
public class AzureLsmScenarioTest(BlobStorageFixture fixture) : IClassFixture<BlobStorageFixture>
{
    static LsmKeyValueDBOptions Options(ILsmStorage storage, bool writer) => new()
    {
        Storage = storage, AcquireWriterLeaseOnOpen = writer, MemtableSize = 32 * 1024, BlockSize = 2048,
        Level0CompactionTrigger = 2, LevelBaseBytes = 128 * 1024, LevelMultiplier = 4, LevelCount = 4,
        TargetFileSize = 32 * 1024, ValueSeparationThreshold = 500, WalChunkTargetSize = 8 * 1024,
        WalFlushInterval = TimeSpan.FromMilliseconds(2), DeletionGracePeriod = TimeSpan.FromSeconds(30),
        RefreshRetryInterval = TimeSpan.FromMilliseconds(10)
    };

    [Fact]
    public async Task CrashRecoveryAndReaderFollow()
    {
        var storage = new AzureLsmStorage(await fixture.ContainerAsync());
        var writer = await LsmKeyValueDB.OpenAsync(Options(storage, true));
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, false));
        var random = new Random(1);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        long durable = 0;
        for (var commit = 1; commit <= 150; commit++)
        {
            using var tr = await writer.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            for (var op = 0; op < 10; op++)
            {
                var key = new[] { (byte)random.Next(60), (byte)random.Next(4) };
                var value = new byte[random.Next(3) == 0 ? 900 : 8];
                random.NextBytes(value);
                cursor.CreateOrUpdateKeyValue(key, value);
                model[Convert.ToHexString(key)] = Convert.ToHexString(value);
            }

            tr.SetCommitUlong((ulong)commit);
            durable = tr.GetTransactionNumber();
            tr.Commit();
        }

        await writer.WaitForDurabilityAsync(durable);
        await reader.WaitForCommitUlongAsync(150);
        Assert.Equal(model, Content(reader));
        writer.SimulateCrash();
        // The reader takes over once the crashed writer's lease expires.
        using (var tr = await reader.StartWritingTransaction())
        {
            Assert.Equal(150ul, tr.GetCommitUlong());
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([200], [1]);
            tr.SetCommitUlong(151);
            tr.Commit();
        }

        model["C8"] = "01";
        Assert.Equal(model, Content(reader));
    }

    static SortedDictionary<string, string> Content(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (cursor.FindFirstKey([]))
            do result.Add(Convert.ToHexString(cursor.SlowGetKey()), Convert.ToHexString(cursor.SlowGetValue()));
            while (cursor.FindNextKey([]));
        return result;
    }
}
