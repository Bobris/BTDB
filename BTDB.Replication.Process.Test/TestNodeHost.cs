using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test;

namespace BTDB.Replication.ProcessTests;

internal sealed record NodeStatus(string Role, ulong EventId, int Value, int Applied, uint CompletedFile,
    uint CompletedOffset, uint AcknowledgedFile, uint AcknowledgedOffset);

internal sealed class TestNodeHost(string endpoint, IReplicationStorage canonical) : IReplicationNodeHost, IAsyncDisposable
{
    readonly InMemoryReplicationFileStorage _files = new();
    readonly TransactionLogCapture _capture = new();
    readonly SemaphoreSlim _writer = new(1);
    readonly object _progressLock = new();
    readonly PublicationGate _storage = new(canonical);
    ReplicationFileSet? _collection;
    BTreeKeyValueDB? _database;
    LeaderTrlProgress? _progress;
    string _session = "";
    int _applied;
    public static TrlSuccessor Genesis => new("genesis/1", 1);

    public async ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation)
    {
        // A failed attempt owns and disposes its opened resources before the coordinator retries.
        _database?.Dispose();
        _database = null;
        if (_collection != null) { await _collection.DisposeAsync(); _collection = null; }
        TransactionLogPosition restored = default;
        IFileCollection files;
        if (await _storage.ReadAsync(Genesis.Key, cancellation) == null)
            files = new LocalReplicatedCollection(_files);
        else
        {
            var inventory = await CanonicalTrlInventory.DiscoverAsync(_storage, Genesis, cancellation);
            _collection = new(_files, inventory);
            await _collection.InitializeAsync(cancellation);
            files = _collection;
            restored = new(inventory.Tail.FileId, inventory.Tail.State.Length);
        }
        _database = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = _capture, CompactorScheduler = null,
            Compression = new NoCompressionStrategy()
        }, cancellation);
        return [new("main", _database, _capture, _storage, Genesis, restored, id => $"{_session}/{id}")];
    }

    public LeaderCandidate CreateCandidate()
    {
        _session = Guid.NewGuid().ToString("N");
        return new("cluster", endpoint, _session, 1, ["main"], endpoint, Guid.NewGuid().ToString("N"));
    }
    public LeaderTrlProgress? GetProgress(string database) { lock (_progressLock) return _progress; }
    public void RequestRestart(string reason) => Console.Error.WriteLine("RESTART " + reason);
    public ReplicationNodeRole Role { get; private set; } = ReplicationNodeRole.Restoring;
    public void ReportStatus(ReplicationNodeRole role) => Role = role;
    public void DatabaseRemoved(string database) { }
    public ValueTask<ulong> CaptureInitializationCursorAsync(string database, CancellationToken cancellation) => ValueTask.FromResult(0ul);
    public ValueTask PrepareSchemaAsync(ActivationDatabase database, LeaseAuthority authority, CancellationToken cancellation)
    {
        UpdateProgress();
        return ValueTask.CompletedTask;
    }

    public async Task ApplyAsync(ulong id, byte value)
    {
        await _writer.WaitAsync();
        try
        {
            var db = _database ?? throw new IOException("Database is not restored.");
            using (var read = db.StartReadOnlyTransaction())
                if (id != read.GetCommitUlong() + 1) throw new InvalidOperationException("Test input must be consecutive.");
            using (var transaction = await db.StartWritingTransaction(id))
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([checked((byte)id)], [value]);
                transaction.Commit();
            }
            Interlocked.Increment(ref _applied);
            UpdateProgress();
        }
        finally { _writer.Release(); }
    }

    void UpdateProgress()
    {
        var end = _capture.Completed;
        if (end.FileId == 0) return;
        using var read = _database!.StartReadOnlyTransaction();
        lock (_progressLock) _progress = new(read.GetCommitUlong(), end.FileId, end.Offset);
    }

    public NodeStatus Status()
    {
        var db = _database;
        if (db == null) return new(Role.ToString(), 0, -1, _applied, 0, 0, 0, 0);
        using var read = db.StartReadOnlyTransaction();
        var id = read.GetCommitUlong();
        using var cursor = read.CreateCursor();
        Span<byte> buffer = stackalloc byte[1];
        var value = id != 0 && cursor.FindExactKey([checked((byte)id)]) ? cursor.GetValueSpan(ref buffer)[0] : -1;
        var completed = _capture.Completed;
        var acknowledged = _capture.Acknowledged;
        return new(Role.ToString(), id, value, _applied, completed.FileId, completed.Offset,
            acknowledged.FileId, acknowledged.Offset);
    }
    public void PausePublication() => _storage.Paused = true;

    public async ValueTask DisposeAsync()
    {
        _database?.Dispose();
        if (_collection != null) await _collection.DisposeAsync();
        _files.Dispose();
        _writer.Dispose();
    }

    sealed class PublicationGate(IReplicationStorage inner) : IReplicationStorage
    {
        public IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation) => inner.EnumerateAsync(cancellation);
        public ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation) => inner.ReadAsync(file, offset, buffer, cancellation);
        public ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) => inner.EnsurePureValuesAsync(id, source, cancellation);
        public ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map, CancellationToken cancellation) => inner.PublishKeyIndexAsync(id, snapshot, map, cancellation);
        public IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync(CancellationToken cancellation) => inner.EnumerateMaintenanceAsync(cancellation);
        public ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation) => inner.DeleteAsync(file, cancellation);
        public ValueTask<bool> ProtectPureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) => inner.ProtectPureValuesAsync(id, source, cancellation);

        public volatile bool Paused;
        public ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation) => inner.ReadAsync(key, cancellation);
        public ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination, CancellationToken cancellation) =>
            inner.ReadRangeAsync(key, token, offset, destination, cancellation);
        public async ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation)
        {
            if (Paused) await Task.Delay(Timeout.InfiniteTimeSpan, cancellation);
            return await inner.WriteAsync(write, cancellation);
        }
    }
}
