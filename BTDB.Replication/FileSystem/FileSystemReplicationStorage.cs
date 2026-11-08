using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication;

/// <summary>
/// One database's canonical TRLs and immutable PVL/KVI files in a local directory, for development and tests of a
/// single node; the drop-in counterpart of <c>AzureReplicationStorage</c> with the same conditional semantics. Each
/// database needs its own directory, which one process owns while any instance on it is undisposed. Bind a selected
/// inventory and optional leadership authority for restore and maintenance; bindings share the directory and are
/// disposed with the root instance. Operations complete synchronously and durably (flushed to disk).
/// </summary>
public sealed class FileSystemReplicationStorage : IBindableReplicationStorage, IDisposable
{
    readonly FileSystemObjectStore _store;
    readonly bool _ownsStore;
    readonly CanonicalTrlInventory? _canonical;
    readonly LeaseAuthority? _authority;
    readonly TimeProvider _time;
    // Newer versions of listed PVL/KVI objects verified to hold the same content (see ReadAsync).
    readonly ConcurrentDictionary<(uint, KVFileType), string> _followed = new();
    const int CopyBufferSize = 1024 * 1024;

    /// <param name="directory">This database's own directory; created when missing.</param>
    /// <param name="timeProvider">UTC source of deletion deadlines.</param>
    /// <param name="flushToDisk">False skips the disk flush (fsync) of every write, for fast development and tests;
    /// writes then survive a crash of the process but not of the machine.</param>
    public FileSystemReplicationStorage(string directory, TimeProvider? timeProvider = null, bool flushToDisk = true)
    {
        _store = new(directory, flushToDisk);
        _ownsStore = true;
        _time = timeProvider ?? TimeProvider.System;
    }

    FileSystemReplicationStorage(FileSystemReplicationStorage storage, CanonicalTrlInventory inventory,
        LeaseAuthority? authority)
    {
        _store = storage._store;
        _time = storage._time;
        _canonical = inventory;
        _authority = authority;
    }

    /// <summary>Create a distinct session view. Omitting authority permits restore only, not maintenance.</summary>
    public FileSystemReplicationStorage Bind(CanonicalTrlInventory inventory, LeaseAuthority? authority = null)
    {
        ArgumentNullException.ThrowIfNull(inventory);
        if (!inventory.IsFrom(this)) throw new ArgumentException("Inventory belongs to another storage instance.", nameof(inventory));
        return new(this, inventory, authority);
    }

    IReplicationStorage IBindableReplicationStorage.Bind(CanonicalTrlInventory inventory, LeaseAuthority? authority) =>
        Bind(inventory, authority);

    CanonicalTrlInventory Inventory => _canonical ??
        throw new InvalidOperationException("Bind a selected canonical inventory before restoring files.");

    void RequireAuthority(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (_authority is not { IsValid: true }) throw new InvalidOperationException("Remote maintenance requires live authority.");
    }

    public ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation)
    {
        TrlFileName.FileIdFromKey(key);
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            return ValueTask.FromResult(current == null ? null
                : new TrlObjectState(current.Version, checked((uint)current.Length), current.Sha256));
        }
    }

    public ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        TrlFileName.FileIdFromKey(key);
        cancellation.ThrowIfCancellationRequested();
        if (destination.IsEmpty) return ValueTask.CompletedTask;
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            if (current == null || current.Version != token || offset + (long)destination.Length > current.Length)
                throw new IOException("Selected canonical TRL version is no longer available.");
            _store.Read(current, offset, destination.Span);
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation)
    {
        TrlFileName.Validate(new(write.Key, write.FileId));
        if (write.ExpectedToken == null && write.ExpectedLength != 0)
            throw new ArgumentException("A new TRL cannot have a previous length.");
        if (write.Length < write.ExpectedLength) throw new ArgumentException("Canonical TRLs cannot shrink.");
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(write.Key);
            if (write.ExpectedToken is { } token
                    ? current == null || current.Version != token || current.Length != write.ExpectedLength
                    : current != null)
                return ValueTask.FromResult(new TrlWriteResult(TrlWriteOutcome.Rejected));
            // The committed prefix stays in place; a crash before the metadata switch leaves only ignored extra bytes.
            var data = _store.Append(current?.DataFile, write.ExpectedLength, write.AppendLength,
                (offset, destination) => write.ReadAppend(checked((uint)offset), destination));
            // Every write replaces the metadata: only the write that seals the TRL records its checksum.
            var next = new FileSystemObjectStore.Metadata(FileSystemObjectStore.NewVersion(), data, write.Length,
                write.Sha256);
            _store.Put(write.Key, next);
            return ValueTask.FromResult(new TrlWriteResult(TrlWriteOutcome.Applied,
                new(next.Version, write.Length, write.Sha256)));
        }
    }

    // Native files use a nonzero decimal ID and .trl, .pvl or .kvi.
    static bool TryParseFileName(string name, out uint id, out KVFileType type)
    {
        var dot = name.LastIndexOf('.');
        type = dot <= 0 ? KVFileType.Unknown : name[dot..] switch
        {
            ".pvl" => KVFileType.PureValues, ".kvi" => KVFileType.KeyIndex, ".trl" => KVFileType.TransactionLog,
            _ => KVFileType.Unknown
        };
        id = 0;
        return type != KVFileType.Unknown &&
               uint.TryParse(name.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out id) && id != 0;
    }

    static string Key(uint id, KVFileType type) => id.ToString(CultureInfo.InvariantCulture) + type switch
    {
        KVFileType.PureValues => ".pvl", KVFileType.KeyIndex => ".kvi", KVFileType.TransactionLog => ".trl",
        _ => throw new ArgumentOutOfRangeException(nameof(type))
    };

    List<(string Key, FileSystemObjectStore.Metadata Metadata)> ListNative(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock) return _store.List("").Where(o => TryParseFileName(o.Key, out _, out _)).ToList();
    }

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var file in Inventory.EnumerateAsync(cancellation).ConfigureAwait(false)) yield return file;
        foreach (var (key, metadata) in ListNative(cancellation))
        {
            TryParseFileName(key, out var id, out var type);
            if (type == KVFileType.TransactionLog) continue;
            yield return new(id, type, (ulong)metadata.Length, metadata.Version, true, metadata.Sha256);
        }
    }

    public async IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync(
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        foreach (var (key, metadata) in ListNative(cancellation))
        {
            TryParseFileName(key, out var id, out var type);
            yield return new(key, id, type, metadata.Version, false, metadata.DeleteAfter);
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async IAsyncEnumerable<TrlHead> EnumerateTrlsAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        foreach (var (key, metadata) in ListNative(cancellation))
        {
            TryParseFileName(key, out var id, out var type);
            if (type != KVFileType.TransactionLog) continue;
            if (TrlFileName.FileIdFromKey(key) != id) throw new InvalidDataException("Invalid TRL object name.");
            yield return new(id, key, new(metadata.Version, checked((uint)metadata.Length), metadata.Sha256));
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    public ValueTask<TrlSuccessor> ResolveRecoveryRootAsync(TrlSuccessor genesis, CancellationToken cancellation)
    {
        TrlHead? first = null;
        var hasCheckpoint = false;
        foreach (var (key, metadata) in ListNative(cancellation))
        {
            TryParseFileName(key, out var id, out var type);
            hasCheckpoint |= type == KVFileType.KeyIndex;
            if (type == KVFileType.TransactionLog && (first == null || id < first.FileId))
                first = new(id, TrlFileName.Key(id), new(metadata.Version, checked((uint)metadata.Length)));
        }
        if (first == null && hasCheckpoint) throw new IOException("A published checkpoint has no retained TRL history.");
        return ValueTask.FromResult(first == null ? genesis : new TrlSuccessor(first.Key, first.FileId));
    }

    public ValueTask<RemoteMaintenanceFile> ScheduleDeletionAsync(RemoteMaintenanceFile file, TimeSpan delay,
        CancellationToken cancellation)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(delay.Ticks);
        TrlFileName.ValidateKey(file.Key);
        RequireAuthority(cancellation);
        lock (_store.Lock)
        {
            var current = _store.Get(file.Key);
            if (current == null || current.Version != file.Version) return ValueTask.FromResult(file);
            if (current.DeleteAfter is { } existing) return ValueTask.FromResult(file with { DeleteAfter = existing });
            var deadline = _time.GetUtcNow() + delay;
            var next = current with { Version = FileSystemObjectStore.NewVersion(), DeleteAfter = deadline };
            _store.Put(file.Key, next);
            return ValueTask.FromResult(file with { Version = next.Version, DeleteAfter = deadline });
        }
    }

    public ValueTask CancelDeletionAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
    {
        TrlFileName.ValidateKey(file.Key);
        RequireAuthority(cancellation);
        lock (_store.Lock)
        {
            var current = _store.Get(file.Key);
            if (current == null || current.Version != file.Version)
                throw new IOException("Retained file disappeared or changed during protection.");
            // Rewriting an unmarked file would only change the version that restores and validations read by.
            if (current.DeleteAfter != null)
                _store.Put(file.Key, current with { Version = FileSystemObjectStore.NewVersion(), DeleteAfter = null });
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
    {
        TrlFileName.ValidateKey(file.Key);
        RequireAuthority(cancellation);
        if (file.DeleteAfter is not { } due || _time.GetUtcNow() < due) return ValueTask.CompletedTask;
        lock (_store.Lock)
        {
            var current = _store.Get(file.Key);
            if (current != null && current.Version == file.Version && current.DeleteAfter is { } deadline &&
                _time.GetUtcNow() >= deadline)
                _store.Remove(file.Key, current);
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> ProtectPureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation)
    {
        RequireAuthority(cancellation);
        var key = Key(id, KVFileType.PureValues);
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            if (current == null) return ValueTask.FromResult(false);
            if ((ulong)current.Length != source.Length || current.Sha256 == null) throw new RemoteFileConflictException();
            // Clearing a mark changes the version, so an older delete of the marked version cannot match.
            if (current.DeleteAfter != null)
                _store.Put(key, current with { Version = FileSystemObjectStore.NewVersion(), DeleteAfter = null });
            return ValueTask.FromResult(true);
        }
    }

    public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation)
    {
        if (file.FileType == KVFileType.TransactionLog)
            return await Inventory.ReadAsync(file, offset, buffer, cancellation).ConfigureAwait(false);
        if (offset > file.Length) throw new ArgumentOutOfRangeException(nameof(offset));
        var count = (int)Math.Min((ulong)buffer.Length, file.Length - offset);
        cancellation.ThrowIfCancellationRequested();
        if (count == 0) return 0;
        var identity = (file.FileId, file.FileType);
        lock (_store.Lock)
        {
            var current = _store.Get(Key(file.FileId, file.FileType)) ??
                          throw new IOException("Selected checkpoint file is no longer available.");
            if (current.Version != _followed.GetValueOrDefault(identity, file.Version))
            {
                // The content is immutable; a deletion mark or its removal changes only metadata and thus the version.
                // The same length and whole-file SHA-256 identify the listed bytes.
                if (file.Sha256 == null || (ulong)current.Length != file.Length ||
                    !string.Equals(current.Sha256, file.Sha256, StringComparison.OrdinalIgnoreCase))
                    throw new IOException("Selected checkpoint file is no longer available.");
                _followed[identity] = current.Version;
            }
            _store.Read(current, (long)offset, buffer.Span[..count]);
        }
        return count;
    }

    public ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation)
    {
        using var staged = new StagedFile(_store, _authority, cancellation);
        var buffer = new byte[(int)Math.Min(source.Length, CopyBufferSize)];
        for (ulong offset = 0; offset < source.Length;)
        {
            var count = (int)Math.Min((ulong)buffer.Length, source.Length - offset);
            source.File.RandomRead(buffer.AsSpan(0, count), offset, false);
            staged.Write(buffer.AsSpan(0, count), offset);
            offset += (uint)count;
        }
        staged.Commit(Key(id, KVFileType.PureValues));
        return ValueTask.CompletedTask;
    }

    public async ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map,
        CancellationToken cancellation)
    {
        var first = snapshot.Sources.Where(s => s.FileType == KVFileType.TransactionLog).Select(s => s.FileId)
            .Append(snapshot.TransactionLogFileId).Min();
        // Native KVI references define its closure. Protect retained TRLs from old deletion marks before publication.
        var required = snapshot.Sources.Where(s => s.FileType == KVFileType.TransactionLog).Select(s => s.FileId)
            .Append(snapshot.TransactionLogFileId).ToHashSet();
        await foreach (var file in EnumerateMaintenanceAsync(cancellation).ConfigureAwait(false))
        {
            if (file.FileType != KVFileType.TransactionLog) continue;
            required.Remove(file.FileId);
            if (file.FileId >= first && file.DeleteAfter != null)
                await CancelDeletionAsync(file, cancellation).ConfigureAwait(false);
        }
        if (required.Count != 0) throw new FileNotFoundException("A required checkpoint TRL is missing.");
        using var staged = new StagedFile(_store, _authority, cancellation);
        using (var writer = new PositionLessStreamWriter(staged, () => { })) snapshot.WriteTo(writer, 0, map, cancellation);
        staged.Commit(Key(id, KVFileType.KeyIndex));
    }

    /// <summary>Sequential content of a new immutable file, staged outside the lock and created only if absent. An
    /// existing file with the same length and SHA-256 is this retried identity; different content fences.</summary>
    sealed class StagedFile : IPositionLessStream
    {
        readonly FileSystemObjectStore _store;
        readonly LeaseAuthority? _authority;
        readonly CancellationToken _cancellation;
        readonly string _path;
        readonly FileStream _stream;
        readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        ulong _length;

        public StagedFile(FileSystemObjectStore store, LeaseAuthority? authority, CancellationToken cancellation)
        {
            _store = store;
            _authority = authority;
            _cancellation = cancellation;
            RequireAuthority();
            _path = store.Stage();
            _stream = new(_path, FileMode.CreateNew, FileAccess.Write);
        }

        void RequireAuthority()
        {
            _cancellation.ThrowIfCancellationRequested();
            if (_authority is not { IsValid: true })
                throw new InvalidOperationException("Checkpoint publication requires live authority.");
        }

        public void Write(ReadOnlySpan<byte> data, ulong position)
        {
            RequireAuthority();
            if (position != _length) throw new NotSupportedException("Checkpoint output must be sequential.");
            _hash.AppendData(data);
            _stream.Write(data);
            _length += (uint)data.Length;
        }

        public void Commit(string key)
        {
            _store.Flush(_stream);
            _stream.Dispose();
            var sha = Convert.ToHexString(_hash.GetHashAndReset());
            RequireAuthority();
            lock (_store.Lock)
            {
                if (_store.Get(key) is { } existing)
                {
                    if ((ulong)existing.Length == _length &&
                        string.Equals(existing.Sha256, sha, StringComparison.OrdinalIgnoreCase)) return;
                    _authority?.Fence();
                    throw new RemoteFileConflictException();
                }
                _store.Put(key, new(FileSystemObjectStore.NewVersion(), _store.Install(_path), (long)_length, sha));
            }
        }

        public int Read(Span<byte> data, ulong position) => throw new NotSupportedException();
        public void Flush() { }
        public void HardFlush() { }
        public ulong GetSize() => _length;
        public void SetSize(ulong size) { if (size != 0 || _length != 0) throw new NotSupportedException(); }

        public void Dispose()
        {
            _stream.Dispose();
            _hash.Dispose();
            File.Delete(_path);
        }
    }

    /// <summary>Releases the directory once every instance on it is disposed. Bindings share the root's directory.</summary>
    public void Dispose()
    {
        if (_ownsStore) _store.Dispose();
    }
}
