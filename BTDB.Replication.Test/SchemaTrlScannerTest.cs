using System;
using System.IO;
using System.Threading;
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

    [Fact]
    public async Task CancelledScanResumesAfterLastCompleteTransaction()
    {
        using var node = await Node.Create(false);
        await node.Write(1, 1);
        var start = node.Capture.Completed;
        for (ulong id = 2; id <= 6; id++) await node.Write(id, (byte)id, size: 300_000);
        using var reader = node.Reader();
        reader.ReadChunkSize = 4096;
        var reads = 0;
        reader.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        Assert.False(await new SchemaTrlScanner(start, node.Db.FileCollection.Guid)
            .ContainsSchemaAsync(reader, node.Capture.Completed, default));
        var fullScan = reads;
        using var cancellation = new CancellationTokenSource();
        var scanner = new SchemaTrlScanner(start, node.Db.FileCollection.Guid);
        reads = 0;
        reader.BeforeRead = (_, _, _) =>
        {
            if (++reads == fullScan * 3 / 4) cancellation.Cancel();
            return ValueTask.CompletedTask;
        };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            scanner.ContainsSchemaAsync(reader, node.Capture.Completed, cancellation.Token).AsTask());
        reads = 0;
        reader.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        Assert.False(await scanner.ContainsSchemaAsync(reader, node.Capture.Completed, default));
        Assert.True(reads < fullScan / 2, $"resumed scan read {reads} of {fullScan} ranges");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReservedOddIdIsSkippedByLeaderLineage(bool legacyEven)
    {
        using var node = await Node.Create(legacyEven: legacyEven, reservedOdd: true);
        // Start at the restored tail header, before the rotation that skips reserved ID 3.
        var scanner = new SchemaTrlScanner(new(legacyEven ? 2u : 1u, 0), node.Db.FileCollection.Guid);
        using var reader = node.Reader();
        for (ulong id = 1; id <= 3; id++) await node.Write(id, (byte)id);
        Assert.True(node.Capture.Completed.FileId >= 5);
        Assert.False(await scanner.ContainsSchemaAsync(reader, node.Capture.Completed, default));
        await node.Write(3, 4);
        Assert.True(await scanner.ContainsSchemaAsync(reader, node.Capture.Completed, default));
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
