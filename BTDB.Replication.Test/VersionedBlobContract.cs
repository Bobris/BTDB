using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace BTDB.Replication.Test;

/// <summary>The <see cref="IVersionedBlob"/> contract every implementation passes, starting from a missing blob.</summary>
internal static class VersionedBlobContract
{
    const VersionedBlobWriteOutcome Applied = VersionedBlobWriteOutcome.Applied;
    const VersionedBlobWriteOutcome Rejected = VersionedBlobWriteOutcome.Rejected;

    public static async Task VerifyAsync(IVersionedBlob blob)
    {
        Assert.Null(await blob.ReadAsync(default));
        Assert.Equal(Rejected, (await blob.WriteAsync("unknown", new byte[] { 1 }, default)).Outcome);
        Assert.Null(await blob.ReadAsync(default));

        var input = new byte[] { 1, 2, 3 };
        var created = await blob.WriteAsync(null, input, default);
        Assert.Equal(Applied, created.Outcome);
        Assert.NotNull(created.Version);
        input[0] = 9; // the blob keeps its own copy
        Assert.Equal(Rejected, (await blob.WriteAsync(null, new byte[] { 7 }, default)).Outcome);
        var read = await blob.ReadAsync(default);
        Assert.Equal(created.Version, read!.Version);
        Assert.Equal(new byte[] { 1, 2, 3 }, read.Content.ToArray());

        var replaced = await blob.WriteAsync(created.Version, new byte[] { 4, 5 }, default);
        Assert.Equal(Applied, replaced.Outcome);
        Assert.NotEqual(created.Version, replaced.Version);
        Assert.Equal(Rejected, (await blob.WriteAsync(created.Version, new byte[] { 6 }, default)).Outcome);
        read = await blob.ReadAsync(default);
        Assert.Equal(replaced.Version, read!.Version);
        Assert.Equal(new byte[] { 4, 5 }, read.Content.ToArray());

        var emptied = await blob.WriteAsync(replaced.Version, ReadOnlyMemory<byte>.Empty, default);
        Assert.Equal(Applied, emptied.Outcome);
        read = await blob.ReadAsync(default);
        Assert.Equal(emptied.Version, read!.Version);
        Assert.True(read.Content.IsEmpty);

        var large = Enumerable.Range(0, 3 * 1024 * 1024 + 17).Select(i => (byte)(i * 31)).ToArray();
        var grown = await blob.WriteAsync(emptied.Version, large, default);
        Assert.Equal(Applied, grown.Outcome);
        read = await blob.ReadAsync(default);
        Assert.Equal(grown.Version, read!.Version);
        Assert.True(read.Content.Span.SequenceEqual(large));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await blob.ReadAsync(canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await blob.WriteAsync(grown.Version, new byte[] { 8 }, canceled.Token));
        Assert.Equal(grown.Version, (await blob.ReadAsync(default))!.Version);
    }
}
