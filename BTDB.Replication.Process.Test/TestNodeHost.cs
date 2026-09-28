using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Azure.Storage.Blobs;
using BTDB.Replication.Azure;
using BTDB.Replication.Test;

namespace BTDB.Replication.ProcessTests;

// Compared is the last cut the follower matched with its leader (not local TRL retention).
internal sealed record NodeStatus(string Role, ulong EventId, int Value, int Applied, uint CompletedFile,
    uint CompletedOffset, uint ComparedFile, uint ComparedOffset, bool Detached = false);

internal sealed class TestNodeHost(string endpoint, BlobContainerClient container, string dataDirectory,
    ulong generation, string[] names, bool objects = false) : IReplicationNodeHost, IAsyncDisposable
{
    // One application database: its own local directory, capture, Blob prefix and publication gate.
    sealed class Database
    {
        public Database(string name, BlobContainerClient container, string directory)
        {
            Name = name;
            Files = new(directory);
            Canonical = new(container, name);
            Storage = new(Canonical);
        }

        public readonly string Name;
        public readonly OnDiskReplicationFileStorage Files;
        public readonly TransactionLogCapture Capture = new();
        public readonly AzureReplicationStorage Canonical;
        public readonly PublicationGate Storage;
        public readonly SemaphoreSlim Writer = new(1);
        public readonly object ProgressLock = new();
        public ReplicationFileSet? Collection;
        public BTreeKeyValueDB? Db;
        public ObjectDB? Objects;
        public bool RelationsReady;
        public LeaderTrlProgress? Progress;
        public int Applied;
    }

    readonly Dictionary<string, Database> _databases = names.ToDictionary(name => name,
        name => new Database(name, container, names.Length == 1 ? dataDirectory : Path.Combine(dataDirectory, name)));
    string _session = "";
    public static TrlSuccessor Genesis => new("1.trl", 1);

    public async ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation)
    {
        var restored = new List<ActivationDatabase>();
        foreach (var name in names) restored.Add(await RestoreAsync(_databases[name], cancellation));
        return restored;
    }

    async ValueTask<ActivationDatabase> RestoreAsync(Database database, CancellationToken cancellation)
    {
        // A failed attempt owns and disposes its opened resources before the coordinator retries.
        database.Objects?.Dispose();
        database.Objects = null;
        database.RelationsReady = false;
        database.Db?.Dispose();
        database.Db = null;
        if (database.Collection != null) { await database.Collection.DisposeAsync(); database.Collection = null; }
        var restored = false;
        IFileCollection files;
        if (await database.Storage.ResolveRecoveryRootAsync(Genesis, cancellation) == Genesis &&
            await database.Storage.ReadAsync(Genesis.Key, cancellation) == null)
            files = new LocalReplicatedCollection(database.Files);
        else
        {
            var inventory = await CanonicalTrlInventory.DiscoverAsync(database.Canonical, Genesis, cancellation);
            database.Collection = new(database.Files, database.Canonical.Bind(inventory));
            await database.Collection.InitializeAsync(cancellation);
            files = database.Collection;
            restored = true; // Published history exists; use the recovered boundary after native replay.
        }
        database.Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = database.Capture, CompactorScheduler = null,
            Compression = new NoCompressionStrategy(), RequireExplicitTransactions = objects
        }, cancellation);
        if (objects)
        {
            // Relations are registered only by InitializeRelations: under leader authority in PrepareSchemaAsync, or
            // on a follower whose restored schema already matches (a read-only check that writes nothing).
            database.Objects = new ObjectDB();
            database.Objects.Open(database.Db, false, new DBOptions().WithoutAutoRegistrationOfRelations());
        }
        if (restored)
        {
            // Until its first local transaction the restored cut is this node's complete local work.
            var position = database.Db.ReplicationRestoredPosition;
            using var read = database.Db.StartReadOnlyTransaction();
            lock (database.ProgressLock) database.Progress = new(read.GetCommitUlong(), position.FileId, position.Offset);
        }
        return new(database.Name, database.Db, database.Capture, database.Storage, Genesis,
            restored ? database.Db.ReplicationRestoredPosition : default, id => $"{id}.trl");
    }

    public LeaderCandidate CreateCandidate()
    {
        _session = Guid.NewGuid().ToString("N");
        return new("cluster", endpoint, _session, generation, names, endpoint, Guid.NewGuid().ToString("N"));
    }
    volatile PreparedHandoff? _preparedUpgrade;
    public PreparedHandoff? PreparedUpgrade => _preparedUpgrade;
    // The test decides when this higher-generation node is ready to take over (compatibility and inputs prepared).
    public void PrepareUpgrade() => _preparedUpgrade ??= new(generation, Guid.NewGuid().ToString());
    public LeaderTrlProgress? GetProgress(string database)
    {
        var state = _databases[database];
        lock (state.ProgressLock) return state.Progress;
    }
    public void RequestRestart(string reason) => Console.Error.WriteLine("RESTART " + reason);
    public void RequestFatalRestart(string reason)
    {
        Console.Error.WriteLine("FATAL " + reason);
        Environment.Exit(75); // Test-host fatal policy: no await of the deliberately stuck publication task.
    }
    public ReplicationNodeRole Role { get; private set; } = ReplicationNodeRole.Restoring;
    public void ReportStatus(ReplicationNodeRole role) => Role = role;
    public void DatabaseRemoved(string database) { }
    public ValueTask<ulong> CaptureInitializationCursorAsync(string database, CancellationToken cancellation) => ValueTask.FromResult(0ul);
    public async ValueTask PrepareSchemaAsync(ActivationDatabase database, LeaseAuthority authority, CancellationToken cancellation)
    {
        var state = _databases[database.Name];
        if (state.Objects != null)
        {
            // Genesis or an upgrade: persist this generation's schemas and indexes in one non-application writer.
            if (!authority.IsValid) throw new IOException("Leader authority ended before schema preparation.");
            await state.Writer.WaitAsync(cancellation);
            try
            {
                await state.Objects.InitializeRelations(OrderApplication.Relations(generation));
                state.RelationsReady = true;
            }
            finally { state.Writer.Release(); }
        }
        UpdateProgress(state);
    }

    // One virtual batch of consecutive order events, finished before progress is reported.
    public async Task ApplyOrdersAsync(string name, ulong from, int count)
    {
        var database = _databases[name];
        await database.Writer.WaitAsync();
        try
        {
            var db = database.Objects ?? throw new IOException("Database is not restored.");
            if (!database.RelationsReady)
            {
                await db.InitializeRelations(OrderApplication.Relations(generation));
                database.RelationsReady = true;
            }
            using (var read = database.Db!.StartReadOnlyTransaction())
                if (from != read.GetCommitUlong() + 1) throw new InvalidOperationException("Test input must be consecutive.");
            try
            {
                for (var id = from; id < from + (ulong)count; id++) await OrderApplication.ApplyAsync(db, generation, id);
            }
            finally { db.FinishTransactionBatchAfterCurrentTransaction(); }
            Interlocked.Add(ref database.Applied, count);
            UpdateProgress(database);
        }
        finally { database.Writer.Release(); }
    }

    public async Task<OrderSummary> OrdersAsync(string name)
    {
        var database = _databases[name];
        await database.Writer.WaitAsync();
        try
        {
            var db = database.Objects ?? throw new IOException("Database is not restored.");
            if (!database.RelationsReady)
            {
                await db.InitializeRelations(OrderApplication.Relations(generation));
                database.RelationsReady = true;
            }
            return OrderApplication.Summary(db, generation);
        }
        finally { database.Writer.Release(); }
    }

    // A positive size writes that many KiB of the value byte, to give a restore something to download.
    public async Task ApplyAsync(string name, ulong id, byte value, int kilobytes = 0)
    {
        var database = _databases[name];
        await database.Writer.WaitAsync();
        try
        {
            var db = database.Db ?? throw new IOException("Database is not restored.");
            using (var read = db.StartReadOnlyTransaction())
                if (id != read.GetCommitUlong() + 1) throw new InvalidOperationException("Test input must be consecutive.");
            using (var transaction = await db.StartWritingTransaction(id))
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([checked((byte)id)],
                    kilobytes <= 0 ? [value] : Enumerable.Repeat(value, kilobytes * 1024).ToArray());
                transaction.Commit();
            }
            Interlocked.Increment(ref database.Applied);
            UpdateProgress(database);
        }
        finally { database.Writer.Release(); }
    }

    static void UpdateProgress(Database database)
    {
        var end = database.Capture.Completed;
        if (end.FileId == 0) return;
        using var read = database.Db!.StartReadOnlyTransaction();
        lock (database.ProgressLock) database.Progress = new(read.GetCommitUlong(), end.FileId, end.Offset);
    }

    public NodeStatus Status(ReplicationStatus replication, string name)
    {
        var database = _databases[name];
        var db = database.Db;
        if (db == null) return new(Role.ToString(), 0, -1, database.Applied, 0, 0, 0, 0);
        using var read = db.StartReadOnlyTransaction();
        var id = read.GetCommitUlong();
        using var cursor = read.CreateCursor();
        Span<byte> buffer = stackalloc byte[1];
        var value = id != 0 && cursor.FindExactKey([checked((byte)id)]) ? cursor.GetValueSpan(ref buffer)[0] : -1;
        var completed = database.Capture.Completed;
        var replicated = replication.Current.Databases.FirstOrDefault(d => d.Name == name);
        var compared = replicated?.Compared;
        return new(Role.ToString(), id, value, database.Applied, completed.FileId, completed.Offset,
            compared?.TrlFileId ?? 0, compared?.TrlPosition ?? 0, replicated?.Detached ?? false);
    }
    public void PausePublication()
    {
        foreach (var database in _databases.Values) database.Storage.Paused = true;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var database in _databases.Values)
        {
            database.Objects?.Dispose();
            database.Db?.Dispose();
            if (database.Collection != null) await database.Collection.DisposeAsync();
            database.Files.Dispose();
            database.Writer.Dispose();
        }
    }

    sealed class PublicationGate(IReplicationStorage inner) : IReplicationStorage
    {
        public IAsyncEnumerable<TrlHead> EnumerateTrlsAsync(CancellationToken cancellation) => inner.EnumerateTrlsAsync(cancellation);
        public ValueTask<TrlSuccessor> ResolveRecoveryRootAsync(TrlSuccessor genesis, CancellationToken cancellation) => inner.ResolveRecoveryRootAsync(genesis, cancellation);
        public ValueTask<RemoteMaintenanceFile> ScheduleDeletionAsync(RemoteMaintenanceFile file, TimeSpan delay, CancellationToken cancellation) => inner.ScheduleDeletionAsync(file, delay, cancellation);
        public ValueTask CancelDeletionAsync(RemoteMaintenanceFile file, CancellationToken cancellation) => inner.CancelDeletionAsync(file, cancellation);
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
