using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.AzureStorage;

namespace BTDB.AzureStorage.Test;

sealed class InMemoryBlobStorageBackend : IBlobStorageBackend
{
    readonly object _lock = new();
    readonly Dictionary<string, StoredBlob> _blobs = new();

    public async IAsyncEnumerable<BlobFileInfo> ListFilesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        KeyValuePair<string, StoredBlob>[] snapshot;
        lock (_lock)
        {
            snapshot = _blobs.ToArray();
        }

        foreach (var blob in snapshot)
        {
            yield return new BlobFileInfo(blob.Key, blob.Value.Content.Length);
            await Task.Yield();
        }
    }

    public async Task DownloadToAsync(string name, string localPath, CancellationToken cancellationToken = default)
    {
        byte[] content;
        lock (_lock)
        {
            content = _blobs[name].Content.ToArray();
        }

        Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
        await File.WriteAllBytesAsync(localPath, content, cancellationToken);
    }

    public async Task UploadBlockBlobAsync(string name, string localPath, CancellationToken cancellationToken = default)
    {
        var bytes = await File.ReadAllBytesAsync(localPath, cancellationToken);
        lock (_lock)
        {
            _blobs[name] = new StoredBlob(bytes);
        }
    }

    public Task AppendBlockBlobAsync(string name, long targetLength, BlobDataReader reader,
        CancellationToken cancellationToken = default)
    {
        if (targetLength == 0)
        {
            lock (_lock)
            {
                _blobs.Remove(name);
            }

            return Task.CompletedTask;
        }

        var content = new byte[checked((int)targetLength)];
        reader(0, content);

        lock (_lock)
        {
            _blobs[name] = new StoredBlob(content);
        }

        return Task.CompletedTask;
    }

    public Task DeleteIfExistsAsync(string name, CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            _blobs.Remove(name);
        }

        return Task.CompletedTask;
    }

    public void SeedBlockBlob(string name, ReadOnlySpan<byte> content)
    {
        lock (_lock)
        {
            _blobs[name] = new StoredBlob(content.ToArray());
        }
    }

    public bool Exists(string name)
    {
        lock (_lock)
        {
            return _blobs.ContainsKey(name);
        }
    }

    public byte[] GetBlobContent(string name)
    {
        lock (_lock)
        {
            return _blobs[name].Content.ToArray();
        }
    }

    sealed record StoredBlob(byte[] Content);
}
