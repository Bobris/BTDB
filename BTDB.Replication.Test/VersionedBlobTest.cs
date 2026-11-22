using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace BTDB.Replication.Test;

public class VersionedBlobTest : IDisposable
{
    readonly string _root = Path.Combine(Path.GetTempPath(), "btdb-vblob-" + Guid.NewGuid().ToString("N"));

    string BlobPath => Path.Combine(_root, "nested", "metadata.bin");

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, true);
    }

    [Fact]
    public Task InMemoryFollowsTheContract() => VersionedBlobContract.VerifyAsync(new InMemoryVersionedBlob());

    [Fact]
    public async Task FileSystemFollowsTheContract()
    {
        using var blob = new FileSystemVersionedBlob(BlobPath, flushToDisk: false);
        await VersionedBlobContract.VerifyAsync(blob);
    }

    [Theory]
    [InlineData((int)VersionedBlobFault.Reject, VersionedBlobWriteOutcome.Rejected, false)]
    [InlineData((int)VersionedBlobFault.AmbiguousNotApplied, VersionedBlobWriteOutcome.Ambiguous, false)]
    [InlineData((int)VersionedBlobFault.AmbiguousApplied, VersionedBlobWriteOutcome.Ambiguous, true)]
    public async Task InMemoryFaultsAffectOnlyTheNextWrite(int fault, VersionedBlobWriteOutcome outcome, bool applied)
    {
        // The fault enum is internal, so the public test method takes its value.
        var blob = new InMemoryVersionedBlob();
        var created = await blob.WriteAsync(null, new byte[] { 1 }, default);
        blob.NextWriteFault = (VersionedBlobFault)fault;
        Assert.Equal(new VersionedBlobWriteResult(outcome), await blob.WriteAsync(created.Version, new byte[] { 2 }, default));
        Assert.Equal(VersionedBlobFault.None, blob.NextWriteFault);
        var read = await blob.ReadAsync(default);
        Assert.Equal(applied ? new byte[] { 2 } : new byte[] { 1 }, read!.Content.ToArray());
        Assert.Equal(applied, read.Version != created.Version);
        // The reconciliation a caller does: read, then write against what it read.
        Assert.Equal(VersionedBlobWriteOutcome.Applied,
            (await blob.WriteAsync(read.Version, new byte[] { 3 }, default)).Outcome);
    }

    [Fact]
    public async Task SecondFileInstanceOverTheSamePathFailsUntilTheFirstIsDisposed()
    {
        var first = new FileSystemVersionedBlob(BlobPath, flushToDisk: false);
        Assert.Throws<IOException>(() => new FileSystemVersionedBlob(BlobPath, flushToDisk: false));
        first.Dispose();
        using var second = new FileSystemVersionedBlob(BlobPath, flushToDisk: false);
        Assert.Null(await second.ReadAsync(default));
    }

    [Fact]
    public async Task FileContentSurvivesReopenWithAFreshVersion()
    {
        string old;
        using (var blob = new FileSystemVersionedBlob(BlobPath, flushToDisk: false))
            old = (await blob.WriteAsync(null, new byte[] { 1, 2 }, default)).Version!;
        using var reopened = new FileSystemVersionedBlob(BlobPath, flushToDisk: false);
        var read = await reopened.ReadAsync(default);
        Assert.Equal(new byte[] { 1, 2 }, read!.Content.ToArray());
        Assert.NotEqual(old, read.Version);
        Assert.Equal(VersionedBlobWriteOutcome.Rejected, (await reopened.WriteAsync(old, new byte[] { 3 }, default)).Outcome);
        Assert.Equal(VersionedBlobWriteOutcome.Rejected, (await reopened.WriteAsync(null, new byte[] { 3 }, default)).Outcome);
        Assert.Equal(VersionedBlobWriteOutcome.Applied,
            (await reopened.WriteAsync(read.Version, new byte[] { 3 }, default)).Outcome);
    }

    [Fact]
    public async Task StagedFileOfACrashedWriteIsDiscardedOnOpen()
    {
        using (var blob = new FileSystemVersionedBlob(BlobPath, flushToDisk: false))
            await blob.WriteAsync(null, new byte[] { 1 }, default);
        // A crash after staging and before the replace leaves only the staged file.
        await File.WriteAllBytesAsync(BlobPath + ".tmp", new byte[] { 9, 9 });
        using var reopened = new FileSystemVersionedBlob(BlobPath, flushToDisk: false);
        Assert.False(File.Exists(BlobPath + ".tmp"));
        var read = await reopened.ReadAsync(default);
        Assert.Equal(new byte[] { 1 }, read!.Content.ToArray());
        Assert.Equal(VersionedBlobWriteOutcome.Applied,
            (await reopened.WriteAsync(read.Version, new byte[] { 2 }, default)).Outcome);
    }

    [Fact]
    public async Task StagedFileOfACrashedFirstWriteLeavesTheBlobMissing()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(BlobPath)!);
        await File.WriteAllBytesAsync(BlobPath + ".tmp", new byte[] { 9 });
        using var blob = new FileSystemVersionedBlob(BlobPath, flushToDisk: false);
        Assert.Null(await blob.ReadAsync(default));
        Assert.Equal(VersionedBlobWriteOutcome.Applied, (await blob.WriteAsync(null, new byte[] { 1 }, default)).Outcome);
    }

    [Fact]
    public async Task DisposedFileInstanceThrows()
    {
        var blob = new FileSystemVersionedBlob(BlobPath, flushToDisk: false);
        blob.Dispose();
        blob.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await blob.ReadAsync(default));
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await blob.WriteAsync(null, new byte[] { 1 }, default));
    }
}
