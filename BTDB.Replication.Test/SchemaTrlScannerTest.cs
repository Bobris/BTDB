using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;

namespace BTDB.Replication.Test;

public class SchemaTrlScannerTest
{
    [Fact]
    public async Task WrongDatabaseIdentityCannotTriggerDetachment()
    {
        using var node = await Node.Create(false);
        await node.Write(1, 1);
        var scanner = new SchemaTrlScanner(node.Capture.Completed, Guid.Empty);
        await node.Write(1, 9);
        using var reader = node.Reader();
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            scanner.ContainsSchemaAsync(reader, node.Capture.Completed, default).AsTask());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task NativeRollbackLargePayloadAndRotationDoNotHideSchema(bool tiny, bool legacyEven)
    {
        using var node = await Node.Create(tiny, legacyEven);
        await node.Write(1, 1);
        var scanner = new SchemaTrlScanner(node.Capture.Completed, node.Db.FileCollection.Guid);
        using var reader = node.Reader();
        reader.ReadChunkSize = 17;
        await node.Write(2, 2, rollback: true);
        await node.Write(2, 2, size: tiny ? 700 : 300_000);
        Assert.False(await scanner.ContainsSchemaAsync(reader, node.Capture.Completed, default));
        await node.Write(2, 3); // Unchanged committed cursor, even though a write API supplied it explicitly.
        await node.Write(3, 3);
        Assert.True(await scanner.ContainsSchemaAsync(reader, node.Capture.Completed, default));
    }
}
