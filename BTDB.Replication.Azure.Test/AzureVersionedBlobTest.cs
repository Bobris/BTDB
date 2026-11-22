using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Core.Pipeline;
using BTDB.Replication.Test;
using Xunit;

namespace BTDB.Replication.Azure.Test;

public class AzureVersionedBlobTest(BlobStorageFixture fixture) : IClassFixture<BlobStorageFixture>
{
    /// <summary>Loses the response of the next successful upload after Azure applied it.</summary>
    sealed class LoseNextUpload : HttpPipelineSynchronousPolicy
    {
        public bool Armed;

        public override void OnReceivedResponse(HttpMessage message)
        {
            if (!Armed || message.Request.Method != RequestMethod.Put || message.Response.Status is < 200 or >= 300)
                return;
            Armed = false;
            throw new IOException("Lost upload response after Azure applied it.");
        }
    }

    [Fact]
    public async Task FollowsTheContract()
    {
        var container = await fixture.ContainerAsync();
        await VersionedBlobContract.VerifyAsync(new AzureVersionedBlob(container.GetBlobClient("metadata/layer2.bin")));
    }

    [Fact]
    public async Task LostUploadResponseIsAmbiguousAndReconciledByReading()
    {
        var faults = new LoseNextUpload();
        var container = await fixture.ContainerAsync(faults);
        var blob = new AzureVersionedBlob(container.GetBlobClient("layer2.bin"));
        var created = await blob.WriteAsync(null, new byte[] { 1 }, default);
        Assert.Equal(VersionedBlobWriteOutcome.Applied, created.Outcome);
        faults.Armed = true;
        Assert.Equal(VersionedBlobWriteResult.Ambiguous, await blob.WriteAsync(created.Version, new byte[] { 2 }, default));
        // The write landed: resending it against the old version is rejected, and a read shows it.
        Assert.Equal(VersionedBlobWriteOutcome.Rejected,
            (await blob.WriteAsync(created.Version, new byte[] { 2 }, default)).Outcome);
        var read = await blob.ReadAsync(default);
        Assert.NotEqual(created.Version, read!.Version);
        Assert.Equal(new byte[] { 2 }, read.Content.ToArray());
    }

    [Fact]
    public async Task MissingContainerFailsInsteadOfReadingAsMissingBlob()
    {
        var blob = new AzureVersionedBlob(fixture.Connect("btdb-test-missing-" + Guid.NewGuid().ToString("N"))
            .GetBlobClient("layer2.bin"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await blob.ReadAsync(default));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await blob.WriteAsync(null, new byte[] { 1 }, default));
    }
}
