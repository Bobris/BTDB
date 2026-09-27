using System.Threading.Tasks;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;

namespace BTDB.Replication.Test;

public class RetainingLeaderTrlReaderTest
{
    [Fact]
    public async Task ComparisonReusesLeaderBytesFetchedBySchemaScanUntilReleased()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        var start = follower.Capture.Acknowledged;
        for (ulong id = 1; id <= 3; id++)
        {
            await leader.Write(id, (byte)id, size: 300_000);
            await follower.Write(id, (byte)id, size: 300_000);
        }
        using var inner = leader.Reader();
        var reads = 0;
        inner.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        var reader = new RetainingLeaderTrlReader(inner);
        var end = leader.Capture.Completed;
        Assert.False(await new SchemaTrlScanner(start, leader.Db.FileCollection.Guid).ContainsSchemaAsync(reader, end, default));
        var scanReads = reads;
        Assert.True(scanReads > 0);
        var comparer = new TrlPrefixComparer(follower.Files.GetFile, follower.Capture, start, acknowledge: false);
        Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(reader, end));
        Assert.Equal(scanReads, reads); // The comparison crossed no additional peer round trips.
        reader.Release(end);
        var fresh = new TrlPrefixComparer(follower.Files.GetFile, follower.Capture, start, acknowledge: false);
        Assert.Equal(TrlCompareResult.Matched, await fresh.CompareAsync(reader, end));
        Assert.True(reads > scanReads);
    }

    [Fact]
    public async Task RetentionIsBoundedAndLaterReadsGoToTheLeader()
    {
        using var leader = await Node.Create(false);
        await leader.Write(1, 1, size: 300_000);
        using var inner = leader.Reader();
        var reads = 0;
        inner.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        var reader = new RetainingLeaderTrlReader(inner, capacity: 1000);
        var buffer = new byte[800];
        var fileId = leader.Capture.Completed.FileId;
        Assert.Equal(800, await reader.ReadAsync(fileId, 0, buffer, default));
        Assert.Equal(800, await reader.ReadAsync(fileId, 800, buffer, default)); // Over capacity: not retained.
        Assert.Equal(2, reads);
        Assert.Equal(300, await reader.ReadAsync(fileId, 500, buffer, default)); // Served from the first chunk.
        Assert.Equal(800, await reader.ReadAsync(fileId, 800, buffer, default));
        Assert.Equal(3, reads);
    }
}
