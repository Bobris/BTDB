using System;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class FileCollectionImportTest
{
    [Fact]
    public void InventoryTypesComeFromExtensionsWithoutNativeHeaders()
    {
        using var files = new InMemoryReplicationFileStorage();
        files.ImportFile(2, "kvi");
        files.ImportFile(3, "trl");
        files.ImportFile(4, "pvl");
        Assert.Equal(KVFileType.KeyIndex, files.GetFileType(2));
        Assert.Equal(KVFileType.TransactionLog, files.GetFileType(3));
        Assert.Equal(KVFileType.PureValues, files.GetFileType(4));
    }

    [Fact]
    public void ImportPreservesExactIdsInAnyOrderAndRejectsCollisions()
    {
        using var files = new InMemoryReplicationFileStorage();
        var high = files.ImportFile(100, "pvl");
        var writer = new MemWriter(high.GetAppenderWriter());
        writer.WriteBlock("preserved"u8);
        writer.Flush();
        high.HardFlush();
        Assert.Equal(3u, files.ImportFile(3, "trl").Index);
        Assert.Equal(2u, files.ImportFile(2, "kvi").Index);
        Assert.Throws<InvalidOperationException>(() => files.ImportFile(100, "other-extension"));
        Assert.Throws<InvalidOperationException>(() => files.ImportFile(100, "pvl"));
        Assert.Throws<ArgumentOutOfRangeException>(() => files.ImportFile(0, "invalid"));
        var bytes = new byte[9];
        high.RandomRead(bytes, 0, false);
        Assert.Equal("preserved"u8.ToArray(), bytes);
        Assert.Equal(5u, files.AddFile("trl", FileIdParity.Odd).Index);
        Assert.Equal(102u, files.AddFile("pvl", FileIdParity.Even).Index);
        Assert.Equal(103u, files.AddFile("legacy").Index);
        Assert.Equal(6u, files.GetCount());
    }

    [Fact]
    public async Task ConcurrentImportAndAllocationNeverOpenTheSameIdTwice()
    {
        using var files = new InMemoryReplicationFileStorage();
        var imported = await Task.WhenAll(Enumerable.Range(0, 32).Select(i => Task.Run(() =>
        {
            try
            {
                // Allocation may claim this ID first, but at most one operation can create it.
                return i % 2 == 0 ? files.ImportFile(1, "import" + i) : files.AddFile("new");
            }
            catch (InvalidOperationException) { return null; }
        })));
        var successful = imported.Where(f => f != null).ToArray();
        Assert.Equal(successful.Length, successful.Select(f => f!.Index).Distinct().Count());
        Assert.Single(successful, f => f!.Index == 1);
        Assert.Equal((uint)successful.Length, files.GetCount());
    }
}
