using Xunit;

namespace BTDB.AzureStorage.Test;

public class AzureBlobStorageBackendTests
{
    [Theory]
    [InlineData(null, null, "00000001.trl", "00000001.trl")]
    [InlineData("", null, "00000001.trl", "00000001.trl")]
    [InlineData("tenant-a", "tenant-a/", "00000001.trl", "tenant-a/00000001.trl")]
    [InlineData("/tenant-a/cache//", "tenant-a/cache/", "00000001.trl", "tenant-a/cache/00000001.trl")]
    [InlineData(@"tenant-a\cache", "tenant-a/cache/", "00000001.trl", "tenant-a/cache/00000001.trl")]
    public void AzureBlobStorageBackendNormalizesAndPrefixesNames(string? prefix, string? expectedListPrefix,
        string name,
        string expectedBlobName)
    {
        var pathPrefix = AzureBlobStorageBackend.NormalizePathPrefix(prefix);
        var listPrefix = pathPrefix.Length == 0 ? null : pathPrefix + "/";

        Assert.Equal(expectedListPrefix, listPrefix);
        Assert.Equal(expectedBlobName, AzureBlobStorageBackend.PrefixPath(name, pathPrefix));
        Assert.True(AzureBlobStorageBackend.TryStripPathPrefix(expectedBlobName, listPrefix, out var strippedName));
        Assert.Equal(name, strippedName);
    }

    [Fact]
    public void AzureBlobStorageBackendDoesNotStripDifferentPrefix()
    {
        var pathPrefix = AzureBlobStorageBackend.NormalizePathPrefix("tenant-a/cache");
        var listPrefix = pathPrefix + "/";

        Assert.False(AzureBlobStorageBackend.TryStripPathPrefix("tenant-b/cache/00000001.trl", listPrefix,
            out var strippedName));
        Assert.Equal("", strippedName);
    }

    [Fact]
    public void CalculateCommittedBlockSizeKeepsBlockCountWithinAzureLimit()
    {
        const long fourGigabytes = 4L * 1024 * 1024 * 1024;

        var blockSize = AzureBlobStorageBackend.TransactionLogBlockSize;

        Assert.True((fourGigabytes + blockSize - 1) / blockSize <= AzureBlobStorageBackend.MaxCommittedBlockCount);
    }
}
