using System;
using System.IO;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BTDB.KVDBLayer;
using BTDB.Replication;

namespace DBBenchmark.Replication;

/// Local commit cost with replication disabled (standalone file collections) and enabled (ReplicationFileSet over
/// node-local replication storage with TransactionLogCapture and explicit event transactions). Nothing consumes the
/// capture here: publication and comparison run on other lanes and are measured by the replication measurements.
/// Every commit appends to the TRL and nothing compacts, so a fixed invocation count bounds the database to about
/// 45 x 10,000 commits (1.8 GB at 4 KiB values) instead of letting the pilot grow it until disk or memory runs out.
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 5, iterationCount: 40, invocationCount: 10_000)]
public class ReplicationCommitBenchmark
{
    public enum Storage
    {
        StandaloneMemory,
        StandaloneFile,
        // The memory-mapped standalone collection: the like-for-like baseline of OnDiskReplicationFileStorage.
        StandaloneMapped,
        ReplicatedMemory,
        ReplicatedMapped
    }

    [Params(Storage.StandaloneMemory, Storage.StandaloneFile, Storage.StandaloneMapped, Storage.ReplicatedMemory,
        Storage.ReplicatedMapped)]
    public Storage Setup;

    [Params(64, 4096)] public int ValueSize;

    string? _directory;
    IDisposable? _storage;
    BenchmarkReplicationStorage? _remote;
    ReplicationFileSet? _files;
    BTreeKeyValueDB _db = null!;
    readonly byte[] _key = new byte[8];
    byte[] _value = [];
    ulong _eventId;

    bool Replicated => Setup is Storage.ReplicatedMemory or Storage.ReplicatedMapped;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _value = new byte[ValueSize];
        Random.Shared.NextBytes(_value);
        if (Setup is not (Storage.StandaloneMemory or Storage.ReplicatedMemory))
        {
            _directory = Path.Combine(Path.GetTempPath(), "btdb-replication-commit-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
        }
        if (!Replicated)
        {
            IFileCollection files = Setup switch
            {
                Storage.StandaloneMemory => new InMemoryFileCollection(),
                Storage.StandaloneFile => new OnDiskFileCollection(_directory!),
                _ => new OnDiskMemoryMappedFileCollection(_directory!)
            };
            _storage = files;
            _db = new(new KeyValueDBOptions
            {
                FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null
            });
            return;
        }
        IReplicationFileStorage local = Setup == Storage.ReplicatedMapped
            ? new OnDiskReplicationFileStorage(_directory!) : new InMemoryReplicationFileStorage();
        _storage = local;
        _remote = new();
        _files = new(local, _remote);
        await _files.InitializeAsync();
        _db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = _files, TransactionLogCapture = new(), RequireExplicitTransactions = true,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null
        });
    }

    [Benchmark]
    public async Task Commit()
    {
        var id = ++_eventId;
        BitConverter.TryWriteBytes(_key, id);
        using var transaction = Replicated ? await _db.StartWritingTransaction(id) : await _db.StartWritingTransaction();
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue(_key, _value);
        transaction.Commit();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try
        {
            _db.Dispose();
            if (_files != null) await _files.DisposeAsync();
            _storage?.Dispose();
            _remote?.Dispose();
        }
        finally
        {
            if (_directory != null) Directory.Delete(_directory, true);
        }
    }
}
