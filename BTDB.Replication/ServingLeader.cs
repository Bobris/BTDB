using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>The active leader's side of the peer protocol for one selected session: authenticates follower
/// connections, answers polls with progress, grants and inline TRL bytes, and serves range reads.</summary>
internal sealed class ServingLeader(ReplicationPeerIdentity identity, LeaseAuthority authority,
    IReadOnlyList<ActivationDatabase> databases, IReadOnlyList<CanonicalTrlPublisher> publishers,
    IReplicationNodeHost host, IReplicationScheduler scheduler, ulong generation)
{
    readonly object _lock = new();
    readonly IReplicationNodeHost _host = host;
    readonly ulong _generation = generation;
    readonly byte[] _apiKey = Encoding.UTF8.GetBytes(identity.ApiKey);
    readonly ConfirmationGrants _grants = new(scheduler, authority);
    readonly Dictionary<string, LeaderTrlReader> _readers = databases.ToDictionary(d => d.Name,
        d => new LeaderTrlReader(d.Database, d.Capture, authority, d.RestoredBase), StringComparer.Ordinal);
    readonly Dictionary<string, (CanonicalTrlPublisher Publisher, TransactionLogCapture Capture)> _databases = databases
        .Select((d, i) => (d.Name, publishers[i], d.Capture))
        .ToDictionary(p => p.Name, p => (p.Item2, p.Capture), StringComparer.Ordinal);
    bool _closed;
    PreparedHandoff? _handoff;
    public PreparedHandoff? Handoff { get { lock (_lock) return _handoff; } }
    public bool IsDrained { get { lock (_lock) return _grants.IsDrained; } }

    public bool Authenticate(string apiKey)
    {
        lock (_lock) return !_closed && authority.IsValid && ApiKeyMatches(apiKey);
    }

    public IReplicationPeerSession Connect(ReplicationPeerIdentity requested)
    {
        lock (_lock)
        {
            RequireActive();
            if (requested.ClusterId != identity.ClusterId || requested.Term != identity.Term ||
                requested.SessionId != identity.SessionId || requested.Endpoint != identity.Endpoint ||
                !ApiKeyMatches(requested.ApiKey))
                throw new IOException("Peer authentication or leader session is invalid.");
            return new Connection(this);
        }
    }

    // Every peer request authenticates; compare without allocating for ordinary key lengths.
    bool ApiKeyMatches(string requested)
    {
        var length = Encoding.UTF8.GetMaxByteCount(requested.Length);
        var rented = length > 256 ? ArrayPool<byte>.Shared.Rent(length) : null;
        try
        {
            Span<byte> buffer = rented ?? stackalloc byte[256];
            var count = Encoding.UTF8.GetBytes(requested, buffer);
            return CryptographicOperations.FixedTimeEquals(buffer[..count], _apiKey);
        }
        finally { if (rented != null) ArrayPool<byte>.Shared.Return(rented); }
    }

    void RequireActive()
    {
        if (_closed || !authority.IsValid) throw new IOException("Leader session is no longer active.");
    }

    public void Close()
    {
        lock (_lock)
        {
            _closed = true;
            foreach (var reader in _readers.Values) reader.Close();
        }
    }

    sealed class Connection(ServingLeader leader) : IReplicationPeerSession
    {
        bool _closed;
        ServingLeader Owner => leader;
        public void Dispose() { lock (leader._lock) _closed = true; }
        void Check() { if (_closed) throw new IOException("Peer connection is closed."); leader.RequireActive(); }
        public async ValueTask<ReplicationPeerPoll> PollAsync(IReadOnlyList<ReplicationPeerPollRequest> databases,
            long challenge, TimeSpan duration, int inlineBudget, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            var answers = new ReplicationPeerDatabaseProgress[databases.Count];
            bool granted;
            lock (leader._lock)
            {
                Check();
                for (var i = 0; i < answers.Length; i++)
                {
                    var database = databases[i].Database;
                    if (!leader._databases.TryGetValue(database, out var served)) throw new IOException("Unknown peer database.");
                    var published = served.Publisher.PublishedPosition;
                    var progress = leader._host.GetProgress(database);
                    // After progress: every schema commit through the advertised cut is already recorded.
                    var schema = served.Capture.NonApplicationCommitted;
                    answers[i] = new(database, progress, published.FileId == 0 ? null : published,
                        Schema: schema.FileId == 0 ? null : schema);
                }
                // One grant covers every database in this poll.
                granted = leader._grants.TryIssue(duration);
            }
            // Inline bytes are read outside the session lock, like range reads; each read rechecks authority.
            var budget = Math.Min(inlineBudget, ReplicationPeerPoll.MaximumInlineBytes);
            for (var i = 0; i < answers.Length && budget > 0; i++)
            {
                if (answers[i].Progress is not { } progress) continue;
                var chunks = await leader._readers[answers[i].Database].ReadInlineAsync(databases[i].From,
                    progress.Position, budget, cancellation).ConfigureAwait(false);
                if (chunks.Count == 0) continue;
                foreach (var chunk in chunks) budget -= chunk.Bytes.Length;
                answers[i] = answers[i] with { Chunks = chunks };
            }
            lock (leader._lock) Check();
            return new(challenge, granted, answers);
        }
        public ValueTask OfferHandoffAsync(PreparedHandoff offer, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            lock (leader._lock)
            {
                Check();
                if (!Guid.TryParse(offer.TransferId, out _)) throw new ArgumentException("Transfer ID must be a UUID.");
                if (offer.ApplicationGeneration > leader._generation &&
                    (leader._handoff == null || offer.ApplicationGeneration > leader._handoff.ApplicationGeneration))
                {
                    leader._handoff = offer;
                    leader._grants.BeginDrain();
                }
            }
            return ValueTask.CompletedTask;
        }
        public ILeaderTrlReader Reader(string database)
        {
            lock (leader._lock)
            {
                Check();
                if (!leader._readers.ContainsKey(database)) throw new IOException("Unknown peer database.");
                return new ReaderProxy(this, database);
            }
        }
        sealed class ReaderProxy(Connection connection, string database) : ILeaderTrlReader
        {
            // Disk reads run outside the session lock so concurrent peer reads, polls and fencing never queue
            // behind them. The reader rechecks closure and authority after reading; so does this proxy.
            public async ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
            {
                LeaderTrlReader reader;
                lock (connection.Owner._lock)
                {
                    connection.Check();
                    reader = connection.Owner._readers[database];
                }
                var count = await reader.ReadAsync(fileId, offset, destination, cancellation).ConfigureAwait(false);
                lock (connection.Owner._lock) connection.Check();
                return count;
            }
        }
    }
}
