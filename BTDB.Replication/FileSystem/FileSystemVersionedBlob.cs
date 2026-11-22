using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>
/// A versioned blob in one local file, for development and tests of a single node; the drop-in counterpart of
/// <c>AzureVersionedBlob</c>. One instance owns the file through an exclusive <c>{path}.lock</c> file held until
/// disposal, so a second instance over the same path fails to open, in this process or another: two instances would
/// each keep their own version and lose each other's updates.
/// The version exists only in memory and is never stored: a fresh random value when the instance opens an existing
/// file and after every write. Every writer goes through this instance, so a version from an earlier instance is
/// simply unknown. A write goes to <c>{path}.tmp</c>, is flushed and atomically replaces the file, so a crash leaves
/// either the old or the new content. Writes are never Ambiguous: they apply or throw.
/// </summary>
/// <param name="path">The blob's file; its directory is created when missing.</param>
/// <param name="flushToDisk">False skips the disk flush (fsync) of every write, for fast development and tests;
/// writes then survive a crash of the process but not of the machine.</param>
public sealed class FileSystemVersionedBlob : IVersionedBlob, IDisposable
{
    readonly Lock _lock = new();
    readonly string _path, _staged;
    readonly bool _flushToDisk;
    readonly FileStream _lockFile;
    string? _version;
    bool _disposed;

    public FileSystemVersionedBlob(string path, bool flushToDisk = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        _path = Path.GetFullPath(path);
        _staged = _path + ".tmp";
        _flushToDisk = flushToDisk;
        var directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
        try
        {
            _lockFile = new(_path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException error)
        {
            throw new IOException($"File '{_path}' is used by another instance; a versioned blob file has one owner.",
                error);
        }
        try
        {
            // Only the owner writes, so a staged file is the leftover of a write that crashed before replacing the blob.
            File.Delete(_staged);
            if (File.Exists(_path)) _version = NewVersion();
        }
        catch
        {
            _lockFile.Dispose();
            throw;
        }
    }

    static string NewVersion() => Guid.NewGuid().ToString("N");

    public ValueTask<VersionedBlobContent?> ReadAsync(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return ValueTask.FromResult(_version == null ? null
                : new VersionedBlobContent(_version, File.ReadAllBytes(_path)));
        }
    }

    public ValueTask<VersionedBlobWriteResult> WriteAsync(string? expectedVersion, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (expectedVersion != _version) return ValueTask.FromResult(VersionedBlobWriteResult.Rejected);
            try
            {
                using (var stream = new FileStream(_staged, FileMode.CreateNew, FileAccess.Write))
                {
                    stream.Write(content.Span);
                    stream.Flush(_flushToDisk);
                }
                File.Move(_staged, _path, true);
            }
            catch
            {
                try { File.Delete(_staged); } catch { }
                throw;
            }
            _version = NewVersion();
            return ValueTask.FromResult(new VersionedBlobWriteResult(VersionedBlobWriteOutcome.Applied, _version));
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _lockFile.Dispose();
        }
    }
}
