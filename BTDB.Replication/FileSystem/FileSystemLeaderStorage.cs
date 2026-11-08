using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>
/// The leader document and its finite lease in a local directory, for development and tests of a single node; the
/// drop-in counterpart of <c>AzureLeaderStorage</c>. The document is <c>leader.json</c>, created from
/// the initial JSON when missing, and every write is conditional on its version and the current lease.
/// One process owns the directory, so the lease lives in memory: a new process finds it free, while within the process
/// an unrenewed lease blocks a new acquisition until it expires after its duration, as in Azure.
/// </summary>
/// <param name="directory">The cluster's leader directory; created when missing.</param>
/// <param name="duration">Lease duration; 15 seconds matches the Azure recommendation.</param>
/// <param name="initialJson">The initial cluster JSON, as for <c>AzureLeaderStorage</c>.</param>
/// <param name="timeProvider">Monotonic timestamps for lease expiry.</param>
/// <param name="flushToDisk">False skips the disk flush (fsync) of every write, for fast development and tests;
/// writes then survive a crash of the process but not of the machine.</param>
public sealed class FileSystemLeaderStorage(string directory, TimeSpan duration, string initialJson,
    TimeProvider? timeProvider = null, bool flushToDisk = true) : IReplicationLeaderStorage, IDisposable
{
    const string Key = "leader.json";
    readonly FileSystemObjectStore _store = new(directory, flushToDisk);
    readonly TimeSpan _duration = duration > TimeSpan.Zero ? duration
        : throw new ArgumentOutOfRangeException(nameof(duration), "The lease duration must be positive.");
    readonly string _initialJson = initialJson ?? throw new ArgumentNullException(nameof(initialJson));
    readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    long Expiry() => _time.GetTimestamp() + (long)(_duration.TotalSeconds * _time.TimestampFrequency);

    public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            Initialize();
            if (_store.LeaseHandle != null && _time.GetTimestamp() < _store.LeaseExpiry)
                return ValueTask.FromResult<LeaseGrant?>(null);
            var handle = Guid.NewGuid().ToString();
            _store.LeaseHandle = handle;
            _store.LeaseExpiry = Expiry();
            return ValueTask.FromResult<LeaseGrant?>(new(handle, _duration));
        }
    }

    /// <summary>Like an Azure lease, an expired lease that nobody acquired since can be renewed.</summary>
    public ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            if (handle != _store.LeaseHandle) return ValueTask.FromResult<TimeSpan?>(null);
            _store.LeaseExpiry = Expiry();
            return ValueTask.FromResult<TimeSpan?>(_duration);
        }
    }

    public ValueTask TransferAsync(string currentHandle, string proposedHandle, CancellationToken cancellation)
    {
        ArgumentException.ThrowIfNullOrEmpty(proposedHandle);
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            if (currentHandle != _store.LeaseHandle || _time.GetTimestamp() >= _store.LeaseExpiry)
                throw new IOException("Lease transfer lost ownership.");
            // Like Azure lease Change: the remaining duration is kept and the target confirms it by renewal.
            _store.LeaseHandle = proposedHandle;
        }
        return ValueTask.CompletedTask;
    }

    void Initialize()
    {
        if (_store.Get(Key) == null)
            _store.Put(Key, new(FileSystemObjectStore.NewVersion(), _store.WriteData(Encoding.UTF8.GetBytes(_initialJson)),
                Encoding.UTF8.GetByteCount(_initialJson)));
    }

    public ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            Initialize();
            var current = _store.Get(Key)!;
            return ValueTask.FromResult(new LeaderRecord(current.Version, Encoding.UTF8.GetString(_store.ReadAll(current))));
        }
    }

    public ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string expectedToken, string json,
        CancellationToken cancellation)
    {
        ArgumentNullException.ThrowIfNull(json);
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(Key);
            if (leaseHandle != _store.LeaseHandle || current == null || current.Version != expectedToken)
                return ValueTask.FromResult(LeaderWriteOutcome.Rejected);
            var content = Encoding.UTF8.GetBytes(json);
            _store.Put(Key, new(FileSystemObjectStore.NewVersion(), _store.WriteData(content), content.Length));
            _store.DeleteData(current.DataFile);
            return ValueTask.FromResult(LeaderWriteOutcome.Applied);
        }
    }

    /// <summary>Releases the directory once every instance on it is disposed.</summary>
    public void Dispose() => _store.Dispose();
}
