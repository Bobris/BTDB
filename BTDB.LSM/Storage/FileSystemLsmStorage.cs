using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.LSM.Storage;

/// ILsmStorage on a local directory, for development, tests across processes and benchmarks (ImplementationPlan.md,
/// section 4). It is a store, not a cache, and it does not fsync, so it is not crash-durable. Keys map to relative
/// paths. Creates write a temporary file and then hard-link it into place (Unix) or move it without replacing
/// (Windows); both are atomic create-if-absent across processes. Leases are records guarded by a short exclusive file lock and use wall-clock UTC time, so every
/// process sharing a directory must run on the same machine.
public sealed partial class FileSystemLsmStorage : ILsmStorage
{
    const string TemporaryPrefix = ".tmp-";
    readonly string _root;
    readonly TimeProvider _timeProvider;

    public FileSystemLsmStorage(string rootDirectory, TimeProvider? timeProvider = null)
    {
        _root = Path.GetFullPath(rootDirectory);
        _timeProvider = timeProvider ?? TimeProvider.System;
        Directory.CreateDirectory(_root);
    }

    public string RootDirectory => _root;

    string PathOf(string key)
    {
        if (key.Length == 0 || key.Contains("..") || Path.IsPathRooted(key))
            throw new ArgumentException("Invalid object key: " + key, nameof(key));
        return Path.Combine(_root, key.Replace('/', Path.DirectorySeparatorChar));
    }

    string TemporaryPathFor(string path) =>
        Path.Combine(Path.GetDirectoryName(path)!, TemporaryPrefix + Guid.NewGuid().ToString("N"));

    string WriteTemporary(string path, ReadOnlySpan<byte> content)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var temporary = TemporaryPathFor(path);
        using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            stream.Write(content);
        return temporary;
    }

    public ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        var path = PathOf(key);
        if (File.Exists(path)) return ValueTask.FromResult(LsmWriteOutcome.Rejected);
        var temporary = WriteTemporary(path, content.Span);
        try
        {
            return ValueTask.FromResult(MoveIfAbsent(temporary, path)
                ? LsmWriteOutcome.Applied
                : LsmWriteOutcome.Rejected);
        }
        finally
        {
            File.Delete(temporary);
        }
    }

    const int ErrorExists = 17; // EEXIST on Linux and macOS

    [LibraryImport("libc", EntryPoint = "link", StringMarshalling = StringMarshalling.Utf8, SetLastError = true)]
    private static partial int Link(string existingPath, string newPath);

    /// Moves temporary to path unless path exists; false when it does. File.Move without overwrite checks and renames
    /// in two steps on Unix, so two processes could both succeed; link(2) fails atomically with EEXIST.
    static bool MoveIfAbsent(string temporary, string path)
    {
        if (OperatingSystem.IsWindows())
        {
            try
            {
                File.Move(temporary, path, false);
                return true;
            }
            catch (IOException) when (File.Exists(path))
            {
                return false;
            }
        }

        if (Link(temporary, path) == 0) return true;
        var error = Marshal.GetLastPInvokeError();
        if (error == ErrorExists) return false;
        throw new IOException($"Creating {path} failed with errno {error}.");
    }

    /// Opens for reading while allowing other processes to replace or delete the file (on Windows too).
    static FileStream OpenShared(string path) => new(path, FileMode.Open, FileAccess.Read,
        FileShare.ReadWrite | FileShare.Delete, 4096, FileOptions.Asynchronous);

    public async ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation)
    {
        try
        {
            await using var stream = OpenShared(PathOf(key));
            var content = new byte[stream.Length];
            await stream.ReadExactlyAsync(content, cancellation);
            return content;
        }
        catch (Exception error) when (error is FileNotFoundException or DirectoryNotFoundException)
        {
            return null;
        }
    }

    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        ReadRange(key, offset, destination.Span);
        return ValueTask.CompletedTask;
    }

    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
        try
        {
            using var handle = File.OpenHandle(PathOf(key), FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            var done = 0;
            while (done < destination.Length)
            {
                var read = RandomAccess.Read(handle, destination[done..], offset + done);
                if (read == 0) throw new EndOfStreamException($"Range beyond the end of {key}");
                done += read;
            }
        }
        catch (DirectoryNotFoundException error)
        {
            throw new FileNotFoundException("Object does not exist: " + key, key, error);
        }
    }

    public async IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        // Enumerate only the directory the prefix points into; keys are listed in ordinal order like object stores.
        var slash = prefix.LastIndexOf('/');
        var directory = slash < 0 ? _root : PathOf(prefix[..slash]);
        if (!Directory.Exists(directory)) yield break;
        var found = new List<LsmObjectInfo>();
        foreach (var file in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories))
        {
            if (Path.GetFileName(file).StartsWith(TemporaryPrefix, StringComparison.Ordinal)) continue;
            var key = Path.GetRelativePath(_root, file).Replace(Path.DirectorySeparatorChar, '/');
            if (!key.StartsWith(prefix, StringComparison.Ordinal) || key.EndsWith(LeaseLockSuffix, StringComparison.Ordinal)) continue;
            long length;
            try
            {
                length = new FileInfo(file).Length;
            }
            catch (FileNotFoundException)
            {
                continue;
            }

            found.Add(new(key, length));
        }

        foreach (var info in found.OrderBy(i => i.Key, StringComparer.Ordinal))
        {
            cancellation.ThrowIfCancellationRequested();
            yield return info;
        }

        await Task.CompletedTask;
    }

    /// Deletes the object; a file still open elsewhere without FileShare.Delete (Windows only) throws, and the
    /// database retries the deletion later.
    public ValueTask DeleteAsync(string key, CancellationToken cancellation)
    {
        try
        {
            File.Delete(PathOf(key));
        }
        catch (DirectoryNotFoundException)
        {
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask PutAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        var path = PathOf(key);
        File.Move(WriteTemporary(path, content.Span), path, true);
        return ValueTask.CompletedTask;
    }

    const string LeaseLockSuffix = ".lease-lock";

    /// Runs action while holding an exclusive lock on the lease's lock file, shared by every process using the
    /// directory, and passes it the current lease record (holder and expiry) or null.
    async ValueTask<T> WithLease<T>(string key, Func<(string Holder, DateTimeOffset Expires)?, Action<string?>, T> action,
        CancellationToken cancellation)
    {
        var path = PathOf(key);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        while (true)
        {
            cancellation.ThrowIfCancellationRequested();
            FileStream guard;
            try
            {
                guard = new(path + LeaseLockSuffix, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                await Task.Delay(5, cancellation);
                continue;
            }

            using (guard)
            {
                (string, DateTimeOffset)? current = null;
                if (File.Exists(path))
                {
                    byte[] record;
                    await using (var stream = OpenShared(path))
                    {
                        record = new byte[stream.Length];
                        await stream.ReadExactlyAsync(record, cancellation);
                    }

                    var parts = Encoding.UTF8.GetString(record).Split(' ');
                    if (parts.Length == 2 && long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture,
                            out var ticks))
                        current = (parts[0], new DateTimeOffset(ticks, TimeSpan.Zero));
                }

                return action(current, holder =>
                {
                    if (holder == null) File.Delete(path);
                    else File.Move(WriteTemporary(path, Encoding.UTF8.GetBytes(holder)), path, true);
                });
            }
        }
    }

    string Record(string leaseId, TimeSpan duration) =>
        leaseId + " " + (_timeProvider.GetUtcNow() + duration).UtcTicks.ToString(CultureInfo.InvariantCulture);

    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation) =>
        WithLease(key, (current, write) =>
        {
            if (current is { } lease && lease.Holder != leaseId && lease.Expires > _timeProvider.GetUtcNow())
                return false;
            write(Record(leaseId, duration));
            return true;
        }, cancellation);

    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation) =>
        WithLease(key, (current, write) =>
        {
            if (current is not { } lease || lease.Holder != leaseId || lease.Expires <= _timeProvider.GetUtcNow())
                return false;
            write(Record(leaseId, duration));
            return true;
        }, cancellation);

    public async ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) =>
        await WithLease(key, (current, write) =>
        {
            if (current is { } lease && lease.Holder == leaseId) write(null);
            return true;
        }, cancellation);
}
