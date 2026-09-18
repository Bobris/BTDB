using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using BTDB.Allocators;
using BTDB.BTreeLib;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication.Test.Simulation;

internal sealed record ApplicationInput(ulong EventId, byte Key, byte[] Value);

// The application driver owns input and execution. No replication coordinator or handler runner is implemented here.
internal sealed class NodeFixture : IDisposable
{
    readonly MallocAllocator _allocator = new();
    bool _disposed;
    readonly Dictionary<string, List<ApplicationInput>> _committed = [];
    readonly Dictionary<string, List<ApplicationInput>> _visible = [];
    readonly Dictionary<string, BTreeKeyValueDB> _databases = [];
    readonly Dictionary<string, InMemoryFileCollection> _files = [];

    public NodeFixture(DeterministicScheduler scheduler, string name, ulong seed, IEnumerable<string> databases,
        Func<string, Guid> databaseIdentity)
    {
        Scope = scheduler.CreateScope(name);
        Random = new SeededRandom(seed);
        SessionId = $"{name}-{Random.NextUInt64():x16}";
        foreach (var database in databases)
        {
            var files = new InMemoryFileCollection();
            SeedNativeHeader(files, databaseIdentity(database));
            _files.Add(database, files);
            _databases.Add(database, new(new KeyValueDBOptions
            {
                FileCollection = files, CompactorScheduler = null, Allocator = _allocator,
                Compression = new NoCompressionStrategy(), RequireExplicitTransactions = true,
                UseOddTransactionLogIds = true, FileSplitSize = 1024
            }));
            _committed.Add(database, []);
            _visible.Add(database, []);
        }
    }

    public DeterministicScheduler.Scope Scope { get; }
    public SeededRandom Random { get; }
    public string SessionId { get; }
    public BTreeKeyValueDB Database(string database) => _databases[database];

    internal static void SeedNativeHeader(InMemoryFileCollection files, Guid identity)
    {
        // Existing BTDB3 empty TRL header. Seeding a known native fixture avoids the core's Guid.NewGuid()
        // fallback without introducing a production API or changing core behavior during M0.
        var file = files.AddFile("trl", FileIdParity.Odd);
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock("BTDB3"u8);
        writer.WriteGuid(identity);
        writer.WriteUInt8((byte)KVFileType.TransactionLog);
        writer.WriteVInt64(1); // generation
        writer.WriteVInt32(0); // no previous file
        writer.Flush();
        file.HardFlush();
    }

    public string NativeFileHash(string database)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var file in _files[database].Enumerate().OrderBy(f => f.Index))
        {
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, true);
            hash.AppendData(bytes);
        }
        return Convert.ToHexString(hash.GetHashAndReset());
    }

    public void Apply(string database, ApplicationInput input, bool rollback = false, bool inBatch = false)
    {
        ObjectDisposedException.ThrowIf(Scope.Stopped, this);
        var pending = _databases[database].StartWritingTransaction(input.EventId, inBatch);
        if (!pending.IsCompletedSuccessfully)
            throw new InvalidOperationException("Fixture writer unexpectedly queued; schedule application work serially.");
        using (var transaction = pending.Result)
        {
            using var cursor = transaction.CreateCursor();
            cursor.CreateOrUpdateKeyValue([input.Key], input.Value);
            if (!rollback)
            {
                transaction.Commit();
                _committed[database].Add(input with { Value = input.Value.ToArray() });
            }
        }
        // Rolling back a mutated virtual writer publishes its previously committed prefix during native rebuild.
        if (!inBatch || rollback) _visible[database] = [.. _committed[database]];
    }

    public void PublishBatch(string database)
    {
        _databases[database].FinishTransactionBatchAfterCurrentTransaction();
        _visible[database] = [.. _committed[database]];
    }

    public void CheckLocalState()
    {
        if (Scope.Stopped) return;
        foreach (var (name, database) in _databases)
        {
            // Independent dictionary replay of application inputs, compared with the actual native readable root.
            var expected = new Dictionary<byte, byte[]>();
            foreach (var input in _visible[name]) expected[input.Key] = input.Value;
            // Opening a normal reader publishes a pending memory batch. Observe the published root directly
            // so invariant checks do not perturb the system under test or retain a historical root between steps.
            var root = (IRootNode)database.ReferenceAndGetLastCommitted();
            try
            {
                var expectedCursor = _visible[name].LastOrDefault()?.EventId ?? 0;
                if (root.CommitUlong != expectedCursor || root.GetCount() != expected.Count)
                    throw new InvalidOperationException($"I2/I4 local cursor or key count differs: {SessionId}/{name}.");
                var cursor = root.CreateCursor();
                foreach (var (key, value) in expected)
                {
                    Span<byte> buffer = default;
                    if (!cursor.FindExact([key]) ||
                        !database.ReadValueSpan(cursor.GetValue(), ref buffer, false).SequenceEqual(value))
                        throw new InvalidOperationException($"I2 local value differs: {SessionId}/{name}/{key}.");
                }
            }
            finally
            {
                database.DereferenceRootNodeInternal(root);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Scope.Dispose();
        foreach (var database in _databases.Values) database.Dispose();
        foreach (var files in _files.Values) files.Dispose();
        var stats = _allocator.GetStats();
        if (stats.AllocCount != stats.DeallocCount || stats.AllocSize != stats.DeallocSize)
            throw new InvalidOperationException("I12 node leaked native allocations.");
    }
}

internal sealed class ClusterFixture : IDisposable
{
    readonly List<NodeFixture> _nodes = [];
    readonly SeededRandom _random;
    readonly List<ApplicationInput> _inputs = [];
    readonly ulong _seed;
    public DeterministicScheduler Scheduler { get; }
    public SimulatedBlobStore Storage { get; }
    public HistoryOracle Oracle { get; }
    public int Checks { get; private set; }

    public ClusterFixture(ulong seed, Action<string>? failureSink = null)
    {
        Scheduler = new(seed, failureSink);
        _seed = seed;
        _random = new(seed);
        Storage = new(Scheduler);
        Oracle = new(Storage);
        Scheduler.CheckInvariants = () =>
        {
            Oracle.Check();
            foreach (var node in _nodes) node.CheckLocalState();
            Checks++;
        };
    }

    public NodeFixture AddNode(string name, params string[] databases)
    {
        var node = new NodeFixture(Scheduler, name, _random.NextUInt64(), databases,
            database => new Guid(SHA256.HashData(Encoding.UTF8.GetBytes($"{_seed:x16}/{database}")).AsSpan(0, 16)));
        _nodes.Add(node);
        return node;
    }

    public ApplicationInput AddInput(byte key, ReadOnlySpan<byte> value)
    {
        var input = new ApplicationInput((ulong)_inputs.Count + 1, key, value.ToArray());
        _inputs.Add(input);
        return input with { Value = input.Value.ToArray() };
    }

    public void Dispose()
    {
        foreach (var node in _nodes) node.Dispose();
    }
}
