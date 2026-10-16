using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using BTDBTest;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Api;

sealed class TemporaryDirectory : IDisposable
{
    public string Path { get; } =
        System.IO.Path.Combine(System.IO.Path.GetTempPath(), "btdb-lsm-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(Path)) Directory.Delete(Path, true);
        }
        catch (IOException)
        {
        }
    }
}

/// FileSystemLsmStorage against the ILsmStorage contract.
public class FileSystemLsmStorageTest : IDisposable
{
    readonly TemporaryDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    [Fact]
    public async Task ConcurrentCreatesApplyExactlyOnce()
    {
        var storage = new FileSystemLsmStorage(_directory.Path);
        for (var round = 0; round < 20; round++)
        {
            var key = $"wal/{round:D16}";
            var outcomes = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
                await storage.CreateAsync(key, new byte[] { (byte)i, 1, 2 }, default))));
            Assert.Single(outcomes, o => o == LsmWriteOutcome.Applied);
            var winner = Array.IndexOf(outcomes, LsmWriteOutcome.Applied);
            Assert.Equal(new byte[] { (byte)winner, 1, 2 }, await storage.TryReadAsync(key, default));
        }
    }

    [Fact]
    public async Task ReadsListsDeletesAndPuts()
    {
        var storage = new FileSystemLsmStorage(_directory.Path);
        Assert.Null(await storage.TryReadAsync("tables/1.sst", default));
        Assert.Throws<FileNotFoundException>(() => storage.ReadRange("tables/1.sst", 0, new byte[1]));
        foreach (var name in new[] { "p/0003", "p/0001", "q/0001", "p/0002", "pa/0001" })
            Assert.Equal(LsmWriteOutcome.Applied, await storage.CreateAsync(name, new byte[100], default));
        var listed = new List<string>();
        await foreach (var info in storage.ListAsync("p/", default)) listed.Add(info.Key);
        Assert.Equal(new[] { "p/0001", "p/0002", "p/0003" }, listed);
        var buffer = new byte[10];
        storage.ReadRange("p/0001", 90, buffer);
        Assert.Throws<EndOfStreamException>(() => storage.ReadRange("p/0001", 95, buffer));
        await storage.DeleteAsync("p/0002", default);
        await storage.DeleteAsync("p/0002", default);
        await storage.DeleteAsync("missing/0002", default);
        Assert.Null(await storage.TryReadAsync("p/0002", default));
        await storage.PutAsync("readers/r", new byte[] { 1 }, default);
        await storage.PutAsync("readers/r", new byte[] { 2 }, default);
        Assert.Equal(new byte[] { 2 }, await storage.TryReadAsync("readers/r", default));
    }

    [Fact]
    public async Task LeaseIsExclusiveUntilReleasedOrExpired()
    {
        var storage = new FileSystemLsmStorage(_directory.Path);
        var duration = TimeSpan.FromSeconds(30);
        Assert.True(await storage.TryAcquireLeaseAsync("LOCK", "a", duration, default));
        Assert.True(await storage.TryAcquireLeaseAsync("LOCK", "a", duration, default));
        Assert.False(await storage.TryAcquireLeaseAsync("LOCK", "b", duration, default));
        Assert.True(await storage.RenewLeaseAsync("LOCK", "a", duration, default));
        Assert.False(await storage.RenewLeaseAsync("LOCK", "b", duration, default));
        await storage.ReleaseLeaseAsync("LOCK", "b", default);
        Assert.False(await storage.TryAcquireLeaseAsync("LOCK", "b", duration, default));
        await storage.ReleaseLeaseAsync("LOCK", "a", default);
        Assert.True(await storage.TryAcquireLeaseAsync("LOCK", "b", TimeSpan.FromMilliseconds(100), default));
        await Task.Delay(300);
        Assert.False(await storage.RenewLeaseAsync("LOCK", "b", duration, default));
        Assert.True(await storage.TryAcquireLeaseAsync("LOCK", "a", duration, default));
    }
}

/// The shared IKeyValueDB suite on FileSystemLsmStorage with tiny settings.
public class FileSystemLsmKeyValueDBTest : KeyValueDBTestBase, IDisposable
{
    readonly List<TemporaryDirectory> _directories = [];

    public FileSystemLsmKeyValueDBTest(ITestOutputHelper output) : base(output)
    {
    }

    protected override IKeyValueDB NewKeyValueDB()
    {
        var directory = new TemporaryDirectory();
        _directories.Add(directory);
        return LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new FileSystemLsmStorage(directory.Path), AcquireWriterLeaseOnOpen = true, MemtableSize = 4096,
            BlockSize = 512, Level0CompactionTrigger = 2, LevelBaseBytes = 16 * 1024, LevelMultiplier = 4,
            LevelCount = 4, TargetFileSize = 8192, ValueSeparationThreshold = 200,
            WalFlushInterval = TimeSpan.FromMilliseconds(1)
        }).AsTask().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        foreach (var directory in _directories) directory.Dispose();
    }
}
