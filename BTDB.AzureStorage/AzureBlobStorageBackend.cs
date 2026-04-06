using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace BTDB.AzureStorage;

public sealed class AzureBlobStorageBackend : IBlobStorageBackend
{
    internal const int MaxCommittedBlockCount = 50_000;
    internal const int TransactionLogBlockSize = 128 * 1024;
    readonly BlobContainerClient _blobContainerClient;
    readonly string _pathPrefix;
    readonly string? _listPrefix;

    public AzureBlobStorageBackend(BlobContainerClient blobContainerClient, string? pathPrefix = null)
    {
        _blobContainerClient = blobContainerClient;
        _pathPrefix = NormalizePathPrefix(pathPrefix);
        _listPrefix = _pathPrefix.Length == 0 ? null : _pathPrefix + "/";
    }

    public async IAsyncEnumerable<BlobFileInfo> ListFilesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        await foreach (var blob in _blobContainerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None,
                           _listPrefix, cancellationToken))
        {
            if (TryStripPathPrefix(blob.Name, _listPrefix, out var name))
            {
                yield return new BlobFileInfo(name, blob.Properties.ContentLength ?? 0);
            }
        }
    }

    public Task DownloadToAsync(string name, string localPath, CancellationToken cancellationToken = default)
    {
        return _blobContainerClient.GetBlobClient(PrefixPath(name, _pathPrefix))
            .DownloadToAsync(localPath, cancellationToken: cancellationToken);
    }

    public Task UploadBlockBlobAsync(string name, string localPath, CancellationToken cancellationToken = default)
    {
        return _blobContainerClient.GetBlobClient(PrefixPath(name, _pathPrefix))
            .UploadAsync(localPath, overwrite: true, cancellationToken);
    }

    public async Task AppendBlockBlobAsync(string name, long targetLength, BlobDataReader reader,
        CancellationToken cancellationToken = default)
    {
        var blockBlobClient = _blobContainerClient.GetBlockBlobClient(PrefixPath(name, _pathPrefix));
        if (targetLength == 0)
        {
            await blockBlobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken);
            return;
        }

        var committedBlocks = await GetCommittedBlocksAsync(blockBlobClient, cancellationToken);
        var targetBlockCount =
            checked((int)((targetLength + TransactionLogBlockSize - 1L) / TransactionLogBlockSize));
        var reusablePrefixCount = CountReusableCommittedPrefix(committedBlocks, targetLength, TransactionLogBlockSize);
        if (reusablePrefixCount == targetBlockCount && committedBlocks.Count == targetBlockCount) return;

        var committedBlockIds = committedBlocks.Take(reusablePrefixCount).Select(static block => block.Name).ToList();
        var buffer = ArrayPool<byte>.Shared.Rent(TransactionLogBlockSize);
        try
        {
            for (var blockIndex = reusablePrefixCount; blockIndex < targetBlockCount; blockIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var blockOffset = (long)blockIndex * TransactionLogBlockSize;
                var blockLength = checked((int)Math.Min(TransactionLogBlockSize, targetLength - blockOffset));
                reader(blockOffset, buffer.AsSpan(0, blockLength));
                await using var contentStream = new MemoryStream(buffer, 0, blockLength, writable: false);
                var blockId = CreateBlockId(blockIndex);
                await blockBlobClient.StageBlockAsync(blockId, contentStream, cancellationToken: cancellationToken);
                committedBlockIds.Add(blockId);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        await blockBlobClient.CommitBlockListAsync(committedBlockIds, cancellationToken: cancellationToken);
    }

    public Task DeleteIfExistsAsync(string name, CancellationToken cancellationToken = default)
    {
        return _blobContainerClient.DeleteBlobIfExistsAsync(PrefixPath(name, _pathPrefix),
            cancellationToken: cancellationToken);
    }

    internal static string NormalizePathPrefix(string? prefix)
    {
        if (string.IsNullOrEmpty(prefix)) return "";
        return string.Join("/", prefix.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries));
    }

    internal static string PrefixPath(string name, string pathPrefix)
    {
        return pathPrefix.Length == 0 ? name : pathPrefix + "/" + name;
    }

    internal static bool TryStripPathPrefix(string name, string? listPrefix, out string strippedName)
    {
        if (listPrefix is null)
        {
            strippedName = name;
            return true;
        }

        if (!name.StartsWith(listPrefix, StringComparison.Ordinal))
        {
            strippedName = "";
            return false;
        }

        strippedName = name[listPrefix.Length..];
        return strippedName.Length != 0;
    }

    internal static int CountReusableCommittedPrefix(IReadOnlyList<BlobBlock> committedBlocks, long targetLength,
        int blockSize)
    {
        var targetBlockCount = checked((int)((targetLength + blockSize - 1L) / blockSize));
        var reusableBlockCount = Math.Min(committedBlocks.Count, targetBlockCount);

        for (var blockIndex = 0; blockIndex < reusableBlockCount; blockIndex++)
        {
            var expectedLength = Math.Min(blockSize, targetLength - (long)blockIndex * blockSize);
            if (committedBlocks[blockIndex].SizeLong != expectedLength) return blockIndex;
            if (committedBlocks[blockIndex].Name != CreateBlockId(blockIndex)) return blockIndex;
        }

        return reusableBlockCount;
    }

    static async Task<List<BlobBlock>> GetCommittedBlocksAsync(BlockBlobClient blockBlobClient,
        CancellationToken cancellationToken)
    {
        try
        {
            var blockList = await blockBlobClient.GetBlockListAsync(BlockListTypes.Committed,
                cancellationToken: cancellationToken);
            return blockList.Value.CommittedBlocks.ToList();
        }
        catch (RequestFailedException exception) when (exception.Status == 404)
        {
            return [];
        }
    }

    internal static string CreateBlockId(int blockIndex)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, blockIndex);
        return Convert.ToBase64String(bytes);
    }
}
