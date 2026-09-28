using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication;

namespace DBBenchmark.Replication;

/// Whole-pipeline replication measurements that BenchmarkDotNet's per-operation model does not fit: steady-state
/// commits with concurrent canonical publication, follower comparison round trips, and cold/warm restore. The remote
/// is in memory with an optional fixed per-request latency, so results are local CPU/disk floors plus a crude latency
/// model, never provider evidence.
static class ReplicationMeasurements
{
    sealed class Options
    {
        public int Transactions = 200_000;
        public int ValueSize = 256;
        public int? RestoreValueSize;
        public int LatencyMs;
        public int PollMs = 50;
        public int PollTransactions = 100;
        public int DatasetMb = 1024;
        public bool OnDisk = true;
        public int SplitMb = 64;
        public int Readers;
        public int Downloads = 4;
        public string? Account;
        public string Container = "btdb-bench";
        public string? ReusePrefix;
        // The coordinator's per-poll InlineBudget; the leader honours up to ReplicationPeerPoll.MaximumInlineBytes.
        public int InlineKb = 1024;
        public int TrlSoftMb, TrlHardMb;
        public int Seconds = 600;
        public int Databases = 4;
    }

    public static async Task RunAsync(string[] args)
    {
        var what = args.Length > 0 ? args[0] : "all";
        var options = Parse(args.Skip(1).ToArray());
        CultureInfo.DefaultThreadCurrentCulture = CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        // PVL size; TRLs use it too unless --trl-soft-mb/--trl-hard-mb set their own limits.
        Node.FileSplitSize = (uint)Math.Min((long)options.SplitMb * 1024 * 1024, int.MaxValue);
        if (options.TrlSoftMb != 0)
            Node.TrlLimits = new((uint)options.TrlSoftMb * 1024 * 1024,
                (uint)Math.Min((long)Math.Max(options.TrlHardMb, options.TrlSoftMb) * 1024 * 1024, uint.MaxValue - 1));
        Console.WriteLine($"PVL split {Node.FileSplitSize / 1048576.0:F0} MiB, TRL " +
                          (Node.TrlLimits is { } limits ? $"soft {limits.Soft / 1048576} MiB, hard {limits.Hard / 1048576} MiB" : "split size"));
        Console.WriteLine($"{RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}, " +
                          $"{Environment.ProcessorCount} CPUs, {RuntimeInformation.FrameworkDescription}, " +
                          $"server GC {System.Runtime.GCSettings.IsServerGC}");
        if (what is "pipeline" or "all") await PipelineAsync(options);
        if (what is "compare" or "all") await CompareAsync(options);
        if (what is "restore" or "all") await RestoreAsync(options);
        if (what is "clock") await ClockMeasurement.RunAsync(options.Seconds);
        if (what is "multi") await MultiPublishAsync(options);
    }

    static Options Parse(string[] args)
    {
        var options = new Options();
        for (var i = 0; i < args.Length; i++)
        {
            int Next() => int.Parse(args[++i], CultureInfo.InvariantCulture);
            switch (args[i])
            {
                case "--transactions": options.Transactions = Next(); break;
                case "--value": options.ValueSize = Next(); options.RestoreValueSize = options.ValueSize; break;
                case "--key-bytes": Node.KeyBytes = Math.Max(8, Next()); break;
                case "--latency-ms": options.LatencyMs = Next(); break;
                case "--poll-ms": options.PollMs = Next(); break;
                case "--poll-transactions": options.PollTransactions = Next(); break;
                case "--dataset-mb": options.DatasetMb = Next(); break;
                case "--memory": options.OnDisk = false; break;
                case "--seconds": options.Seconds = Next(); break;
                case "--databases": options.Databases = Next(); break;
                case "--split-mb": options.SplitMb = Next(); break;
                case "--readers": options.Readers = Next(); break;
                case "--downloads": options.Downloads = Next(); break;
                case "--account": options.Account = args[++i]; break;
                case "--container": options.Container = args[++i]; break;
                case "--reuse-prefix": options.ReusePrefix = args[++i]; break;
                case "--dir": Node.BaseDirectory = args[++i]; break;
                case "--inline-kb": options.InlineKb = Next(); break;
                case "--trl-soft-mb": options.TrlSoftMb = Next(); break;
                case "--trl-hard-mb": options.TrlHardMb = Next(); break;
                default: throw new ArgumentException($"Unknown option {args[i]}.");
            }
        }
        return options;
    }

    sealed class Node : IAsyncDisposable
    {
        // TRL and PVL size; the database default (2 GiB) would keep a whole run in one TRL.
        public static uint FileSplitSize = 64 * 1024 * 1024;
        public static FixedTrlLimits? TrlLimits;

        public readonly string? Directory;
        public readonly IDisposable Storage;
        public readonly IReplicationFileStorage? Local;
        public readonly ReplicationFileSet? Files;
        public readonly TransactionLogCapture? Capture;
        public readonly BTreeKeyValueDB Db;

        Node(string? directory, IDisposable storage, IReplicationFileStorage? local, ReplicationFileSet? files,
            TransactionLogCapture? capture, BTreeKeyValueDB db)
        {
            Directory = directory;
            Storage = storage;
            Local = local;
            Files = files;
            Capture = capture;
            Db = db;
        }

        // Keys are a big-endian ID followed by pseudo-random bytes up to KeyBytes, which KVI prefix compression cannot
        // shrink: longer keys give the KVI its real share of the database.
        public static int KeyBytes = 8;

        public static byte[] Key(ulong id)
        {
            var key = new byte[KeyBytes];
            BinaryPrimitives.WriteUInt64BigEndian(key, id);
            var state = id * 0x9E3779B97F4A7C15UL;
            for (var i = 8; i < key.Length; i++)
            {
                state ^= state >> 29;
                state *= 0xBF58476D1CE4E5B9UL;
                key[i] = (byte)(state >> 56);
            }
            return key;
        }

        // Local storage location, e.g. a VM's temporary or NVMe disk.
        public static string BaseDirectory = Path.GetTempPath();

        public static string NewDirectory()
        {
            var directory = Path.Combine(BaseDirectory, "btdb-replication-" + Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(directory);
            return directory;
        }

        public static Node Standalone(bool onDisk, bool mapped = false)
        {
            var directory = onDisk ? NewDirectory() : null;
            IFileCollection files = !onDisk ? new InMemoryFileCollection()
                : mapped ? new OnDiskMemoryMappedFileCollection(directory!) : new OnDiskFileCollection(directory!);
            return new(directory, files, null, null, null, new(new KeyValueDBOptions
            {
                FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
                FileSplitSize = FileSplitSize, TransactionLogSizeStrategy = TrlLimits
            }));
        }

        /// A follower starts from the leader's published history, which fixes the shared native header identity.
        public static async Task<Node> Restore(bool onDisk, BenchmarkReplicationStorage remote)
        {
            var inventory = await CanonicalTrlInventory.DiscoverAsync(remote, new(TrlFileName.Key(1), 1));
            return await Replicated(onDisk, remote.Bind(inventory));
        }

        public static async Task<Node> Replicated(bool onDisk, IRemoteFileCollection remote, string? directory = null)
        {
            if (onDisk) directory ??= NewDirectory();
            IReplicationFileStorage local = onDisk ? new OnDiskReplicationFileStorage(directory!) : new InMemoryReplicationFileStorage();
            var files = new ReplicationFileSet(local, remote);
            await files.InitializeAsync();
            var capture = new TransactionLogCapture();
            var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = files, TransactionLogCapture = capture, RequireExplicitTransactions = true,
                Compression = new NoCompressionStrategy(), CompactorScheduler = null, FileSplitSize = FileSplitSize, TransactionLogSizeStrategy = TrlLimits
            });
            return new(directory, local, local, files, capture, db);
        }

        public async Task WriteAsync(ulong id, ReadOnlyMemory<byte> value, ulong? keyId = null)
        {
            var key = Key(keyId ?? id);
            using var transaction = Capture != null ? await Db.StartWritingTransaction(id) : await Db.StartWritingTransaction();
            using var cursor = transaction.CreateCursor();
            cursor.CreateOrUpdateKeyValue(key, value.Span);
            transaction.Commit();
        }

        public long LocalTrlBytes(uint fromFileId)
        {
            if (Local == null) return 0;
            long total = 0;
            foreach (var file in Local.Enumerate())
                if (Local.GetFileType(file.Index) == KVFileType.TransactionLog && file.Index >= fromFileId)
                    total += (long)file.GetSize();
            return total;
        }

        public async ValueTask DisposeAsync(bool keepDirectory)
        {
            Db.Dispose();
            if (Files != null) await Files.DisposeAsync();
            Storage.Dispose();
            if (!keepDirectory && Directory != null) System.IO.Directory.Delete(Directory, true);
        }

        public ValueTask DisposeAsync() => DisposeAsync(false);
    }

    static string Micros(long ticks) => (ticks * 1_000_000.0 / Stopwatch.Frequency).ToString("F1", CultureInfo.InvariantCulture);

    static string Percentiles(long[] ticks)
    {
        Array.Sort(ticks);
        long At(double p) => ticks[Math.Min(ticks.Length - 1, (int)(p * ticks.Length))];
        return $"p50 {Micros(At(0.5))} µs, p99 {Micros(At(0.99))} µs, p99.9 {Micros(At(0.999))} µs, max {Micros(ticks[^1])} µs";
    }

    static string Mb(long bytes) => (bytes / 1024.0 / 1024.0).ToString("F1", CultureInfo.InvariantCulture) + " MiB";

    // StandaloneMapped (OnDiskMemoryMappedFileCollection) is the like-for-like baseline of OnDiskReplicationFileStorage.
    enum PipelineMode { Standalone, StandaloneMapped, CaptureOnly, Publish }

    /// Sequential single-key commits while a publisher runs the coordinator's cadence: publish the latest complete
    /// prefix, then wait one poll interval. Optional reader tasks run read-only point lookups of already committed keys
    /// the whole time, as application queries do. Allocation totals include the publisher and the readers.
    static async Task PipelineAsync(Options options)
    {
        Console.WriteLine();
        Console.WriteLine($"## Commit pipeline: {options.Transactions} transactions, {options.ValueSize} B values, " +
                          $"{(options.OnDisk ? "on-disk" : "in-memory")} local storage, {options.Readers} concurrent readers, poll {options.PollMs} ms, remote latency {options.LatencyMs} ms");
        foreach (var mode in new[] { PipelineMode.Standalone, PipelineMode.StandaloneMapped, PipelineMode.CaptureOnly, PipelineMode.Publish })
        {
            // A short warm-up keeps JIT tiering out of the percentiles.
            await PipelineRunAsync(options, mode, Math.Min(20_000, options.Transactions), false);
            await PipelineRunAsync(options, mode, options.Transactions, true);
        }
    }

    static async Task PipelineRunAsync(Options options, PipelineMode mode, int transactions, bool report)
    {
        using var remote = new BenchmarkReplicationStorage(TimeSpan.FromMilliseconds(options.LatencyMs));
        await using var node = mode is PipelineMode.Standalone or PipelineMode.StandaloneMapped
            ? Node.Standalone(options.OnDisk, mode == PipelineMode.StandaloneMapped)
            : await Node.Replicated(options.OnDisk, remote);
        var value = new byte[options.ValueSize];
        Random.Shared.NextBytes(value);
        var latencies = new long[transactions];
        using var stop = new CancellationTokenSource();
        CanonicalTrlPublisher? publisher = null;
        Task? publication = null;
        long maxLag = 0, lagSamples = 0, lagTotal = 0;
        if (mode == PipelineMode.Publish)
        {
            publisher = new(node.Db, node.Capture!, remote, StopwatchScheduler.Authority(), 1, TrlFileName.Key);
            publication = Task.Run(async () =>
            {
                while (true)
                {
                    var completed = node.Capture!.Completed;
                    var published = publisher.PublishedPosition;
                    if (completed.FileId != 0)
                    {
                        var lag = completed.FileId == published.FileId ? completed.Offset - published.Offset : completed.Offset;
                        maxLag = Math.Max(maxLag, lag);
                        lagTotal += lag;
                        lagSamples++;
                    }
                    if (stop.IsCancellationRequested && published >= completed) return;
                    var result = await publisher.PublishNextAsync(true);
                    if (result is not (TrlPublishResult.Published or TrlPublishResult.Idle))
                        throw new InvalidOperationException($"Publication ended with {result}.");
                    if (!stop.IsCancellationRequested) await Task.Delay(options.PollMs);
                }
            });
        }
        long committed = 0, reads = 0;
        var readers = Enumerable.Range(0, options.Readers).Select(seed => Task.Run(() =>
        {
            var random = new Random(seed);
            var buffer = new byte[options.ValueSize].AsSpan();
            long count = 0;
            while (!stop.IsCancellationRequested)
            {
                var last = Volatile.Read(ref committed);
                if (last == 0) { Thread.Yield(); continue; }
                using var transaction = node.Db.StartReadOnlyTransaction();
                using var cursor = transaction.CreateCursor();
                for (var i = 0; i < 16; i++)
                {
                    var key = Node.Key((ulong)random.NextInt64(1, last + 1));
                    if (cursor.Find(key, 0) != FindResult.Exact) throw new InvalidOperationException("A committed key is missing.");
                    if (cursor.GetValueSpan(ref buffer).Length != options.ValueSize) throw new InvalidOperationException("Value length differs.");
                    count++;
                }
            }
            Interlocked.Add(ref reads, count);
        })).ToArray();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        var gen0 = GC.CollectionCount(0);
        var gen2 = GC.CollectionCount(2);
        var allocated = GC.GetTotalAllocatedBytes(true);
        var started = Stopwatch.GetTimestamp();
        for (var i = 0; i < transactions; i++)
        {
            var begin = Stopwatch.GetTimestamp();
            await node.WriteAsync((ulong)i + 1, value);
            latencies[i] = Stopwatch.GetTimestamp() - begin;
            Volatile.Write(ref committed, i + 1);
        }
        var written = Stopwatch.GetTimestamp();
        stop.Cancel();
        await Task.WhenAll(readers);
        if (publication != null) await publication;
        var drained = Stopwatch.GetTimestamp();
        allocated = GC.GetTotalAllocatedBytes(true) - allocated;
        publisher?.Dispose();
        if (!report) return;
        var seconds = (written - started) / (double)Stopwatch.Frequency;
        Console.WriteLine($"- {mode}: {transactions / seconds:F0} commits/s, {Percentiles(latencies)}, " +
                          $"{allocated / transactions} B allocated/commit, GC gen0 {GC.CollectionCount(0) - gen0} gen2 {GC.CollectionCount(2) - gen2}" +
                          (options.Readers == 0 ? "" : $", {reads / seconds:F0} reads/s"));
        if (mode == PipelineMode.CaptureOnly)
            Console.WriteLine($"  local TRL retained without a consumer: {Mb(node.LocalTrlBytes(node.Capture!.Acknowledged.FileId))}");
        if (mode == PipelineMode.Publish)
            Console.WriteLine($"  {(allocated - remote.UploadedBytes) / transactions} B allocated/commit excluding the in-memory remote copy; " +
                              $"{remote.TrlWrites} TRL writes ({transactions / (double)Math.Max(1, remote.TrlWrites):F0} transactions each), " +
                              $"uploaded {Mb(remote.UploadedBytes)}, lag mean {Mb(lagTotal / Math.Max(1, lagSamples))} max {Mb(maxLag)}, " +
                              $"drain after last commit {(drained - written) * 1000.0 / Stopwatch.Frequency:F0} ms, " +
                              $"local TRL retained by capture {Mb(node.LocalTrlBytes(node.Capture!.Acknowledged.FileId))}");
    }

    sealed record FixedTrlLimits(uint Soft, uint Hard) : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint transactionLogFileId) => new(Soft, Hard);
    }

    /// Counts peer round trips: every leader range read is one HTTP request in production.
    sealed class CountingReader(ILeaderTrlReader inner) : ILeaderTrlReader
    {
        public long Reads, Bytes;

        public async ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
        {
            Reads++;
            var read = await inner.ReadAsync(fileId, offset, destination, cancellation).ConfigureAwait(false);
            Bytes += read;
            return read;
        }
    }

    /// Follower comparison in steady state: leader and follower apply the same transactions and the follower compares
    /// after every PollTransactions, once by range reads only and once with inline poll bytes as the coordinator does.
    /// Then one whole-history comparison measures raw comparison throughput.
    static async Task CompareAsync(Options options)
    {
        Console.WriteLine();
        Console.WriteLine($"## Follower comparison: {options.Transactions} transactions, {options.ValueSize} B values, " +
                          $"a poll every {options.PollTransactions} transactions, inline budget {options.InlineKb} KiB, remote latency {options.LatencyMs} ms per peer request");
        foreach (var inline in new[] { false, true })
            await CompareRunAsync(options, inline);
    }

    static async Task CompareRunAsync(Options options, bool inline)
    {
        using var remote = new BenchmarkReplicationStorage();
        await using var leader = await Node.Replicated(false, remote);
        var value = new byte[options.ValueSize];
        Random.Shared.NextBytes(value);
        await leader.WriteAsync(1, value);
        var authority = StopwatchScheduler.Authority();
        using (var genesis = new CanonicalTrlPublisher(leader.Db, leader.Capture!, remote, authority, 1, TrlFileName.Key))
            if (await genesis.PublishNextAsync() != TrlPublishResult.Published) throw new InvalidOperationException("Genesis was not published.");
        await using var follower = await Node.Restore(false, remote);
        var leaderReader = new LeaderTrlReader(leader.Db, leader.Capture!, authority);
        var counting = new CountingReader(leaderReader);
        var retaining = new RetainingLeaderTrlReader(counting);
        var comparer = new TrlPrefixComparer(follower.Db.FileCollection.GetFile, follower.Capture!, follower.Db.ReplicationRestoredPosition);
        var latency = TimeSpan.FromMilliseconds(options.LatencyMs);
        long polls = 0, compareTicks = 0, inlineBytes = 0;
        var allocated = GC.GetTotalAllocatedBytes(true);
        for (var id = 2; id <= options.Transactions; id++)
        {
            await leader.WriteAsync((ulong)id, value);
            await follower.WriteAsync((ulong)id, value);
            if (id % options.PollTransactions != 0 && id != options.Transactions) continue;
            var begin = Stopwatch.GetTimestamp();
            var end = leader.Capture!.Completed;
            polls++;
            var readsBefore = counting.Reads;
            if (inline)
            {
                var budget = options.InlineKb * 1024;
                var from = retaining.Available >= budget ? retaining.ContiguousEnd(comparer.Position) : default;
                var chunks = await leaderReader.ReadInlineAsync(from, end, budget, default);
                foreach (var chunk in chunks) inlineBytes += chunk.Bytes.Length;
                if (chunks.Count != 0) retaining.Retain(from, chunks);
            }
            var result = await comparer.CompareAsync(inline ? retaining : counting, end);
            if (inline) retaining.Release(comparer.Position);
            if (result != TrlCompareResult.Matched) throw new InvalidOperationException($"Comparison ended with {result}.");
            // One modeled round trip for the poll itself and one for every range read it needed.
            if (latency > TimeSpan.Zero) await Task.Delay(latency * (1 + (int)(counting.Reads - readsBefore)));
            compareTicks += Stopwatch.GetTimestamp() - begin;
        }
        allocated = GC.GetTotalAllocatedBytes(true) - allocated;
        var total = leader.Capture!.Completed;
        Console.WriteLine($"- {(inline ? "inline poll bytes" : "range reads only")}: {polls} polls, {counting.Reads} range reads " +
                          $"({counting.Reads / (double)polls:F2} per poll), inline {Mb(inlineBytes)}, range {Mb(counting.Bytes)}, " +
                          $"comparison {compareTicks * 1000.0 / Stopwatch.Frequency / polls:F3} ms per poll, " +
                          $"{allocated / options.Transactions} B allocated/transaction (both nodes)");
        if (inline) return;
        var whole = new TrlPrefixComparer(follower.Db.FileCollection.GetFile, follower.Capture!, follower.Db.ReplicationRestoredPosition);
        var wholeReader = new CountingReader(leaderReader);
        var started = Stopwatch.GetTimestamp();
        allocated = GC.GetTotalAllocatedBytes(true);
        if (await whole.CompareAsync(wholeReader, total) != TrlCompareResult.Matched) throw new InvalidOperationException("History differs.");
        var seconds = (Stopwatch.GetTimestamp() - started) / (double)Stopwatch.Frequency;
        allocated = GC.GetTotalAllocatedBytes(true) - allocated;
        Console.WriteLine($"- whole history {Mb(wholeReader.Bytes)}: {wholeReader.Bytes / 1024.0 / 1024.0 / seconds:F0} MiB/s in-process, " +
                          $"{wholeReader.Reads} range reads, {allocated} B allocated");
    }

    /// Counts restore transfers: every ReadAsync is one range request to the remote.
    sealed class CountingRemote(IRemoteFileCollection inner) : IRemoteFileCollection
    {
        public long Requests, Bytes;

        public IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation) => inner.EnumerateAsync(cancellation);

        public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation)
        {
            Interlocked.Increment(ref Requests);
            var read = await inner.ReadAsync(file, offset, buffer, cancellation).ConfigureAwait(false);
            Interlocked.Add(ref Bytes, read);
            return read;
        }
    }

    // Like the coordinator, retry an unresolved (Pending) write, e.g. after a transient provider failure.
    static async Task PublishAsync(CanonicalTrlPublisher publisher)
    {
        for (var attempt = 1; ; attempt++)
        {
            TrlPublishResult result;
            try { result = await publisher.PublishNextAsync(true); }
            catch (IOException error) when (attempt < 10)
            {
                Console.WriteLine($"  publication retry after {error.Message}");
                await Task.Delay(1000);
                continue;
            }
            if (result is TrlPublishResult.Published or TrlPublishResult.Idle) return;
            if (result != TrlPublishResult.Pending || attempt == 10) throw new InvalidOperationException($"Publication ended with {result}.");
            Console.WriteLine("  publication pending, retrying");
            await Task.Delay(1000);
        }
    }

    /// Aggregate TRL publication of one and then --databases leader databases writing and publishing concurrently, each
    /// to its own prefix of the Azure account (or in memory): every database has its own serialized canonical lane.
    static async Task MultiPublishAsync(Options options)
    {
        var valueSize = options.RestoreValueSize ?? 4096;
        var values = checked((int)((long)options.DatasetMb * 1024 * 1024 / valueSize));
        var container = options.Account == null ? null : new Azure.Storage.Blobs.BlobServiceClient(
                new Uri($"https://{options.Account}.blob.core.windows.net"), new Azure.Identity.DefaultAzureCredential())
            .GetBlobContainerClient(options.Container);
        if (container != null) await container.CreateIfNotExistsAsync();
        Console.WriteLine();
        Console.WriteLine($"## Multi-database publication: {values} x {valueSize} B values per database, " +
                          (container == null ? "in-memory remote" : $"Azure Blob {options.Account}/{options.Container}"));
        foreach (var count in new[] { 1, options.Databases }.Distinct())
        {
            var run = "multi-" + DateTime.UtcNow.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture) + "-" +
                      Guid.NewGuid().ToString("N")[..6];
            var started = Stopwatch.GetTimestamp();
            var bytes = await Task.WhenAll(Enumerable.Range(0, count).Select(i => Task.Run(async () =>
            {
                using var memory = new BenchmarkReplicationStorage(TimeSpan.FromMilliseconds(options.LatencyMs));
                IReplicationStorage remote = container == null ? memory
                    : new BTDB.Replication.Azure.AzureReplicationStorage(container, $"{run}-{i}");
                using var empty = new BenchmarkReplicationStorage();
                await using var leader = await Node.Replicated(options.OnDisk, empty);
                using var publisher = new CanonicalTrlPublisher(leader.Db, leader.Capture!, remote,
                    StopwatchScheduler.Authority(), 1, TrlFileName.Key);
                var value = new byte[valueSize];
                new Random(i).NextBytes(value);
                for (ulong id = 1; id <= (ulong)values; id++)
                {
                    await leader.WriteAsync(id, value);
                    if (id % 4096 == 0) await PublishAsync(publisher);
                }
                await PublishAsync(publisher);
                return leader.LocalTrlBytes(0);
            })));
            var seconds = (Stopwatch.GetTimestamp() - started) / (double)Stopwatch.Frequency;
            Console.WriteLine($"- {count} database(s): {Mb(bytes.Sum())} written and published in {seconds:F1} s, " +
                              $"{bytes.Sum() / 1048576.0 / seconds:F0} MiB/s aggregate, {bytes.Sum() / 1048576.0 / seconds / count:F0} MiB/s per database");
        }
    }

    /// A remote to publish to and restore from: in memory, or Azure Blob Storage with the production adapter.
    sealed record RestoreRemote(string Description, IReplicationStorage Storage,
        Func<CanonicalTrlInventory, IRemoteFileCollection> Bind,
        Func<CanonicalTrlInventory, LeaseAuthority, IReplicationStorage> BindForMaintenance,
        Func<Task<string>> Describe);

    static async Task RestoreAsync(Options options)
    {
        if (options.Account == null)
        {
            using var memory = new BenchmarkReplicationStorage(TimeSpan.FromMilliseconds(options.LatencyMs));
            await RestoreAsync(options, new($"in-memory remote, latency {options.LatencyMs} ms", memory, memory.Bind,
                (_, _) => memory,
                () => Task.FromResult($"TRL {Mb(memory.TrlBytes)}, PVL {Mb(memory.ImmutableBytes - memory.KviBytes)}, KVI {Mb(memory.KviBytes)}")));
            return;
        }
        var service = new Azure.Storage.Blobs.BlobServiceClient(new Uri($"https://{options.Account}.blob.core.windows.net"),
            new Azure.Identity.DefaultAzureCredential());
        var container = service.GetBlobContainerClient(options.Container);
        await container.CreateIfNotExistsAsync();
        // Unique per run: machines started in the same second must never share a canonical namespace.
        var prefix = options.ReusePrefix ?? "restore-" + DateTime.UtcNow.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture) +
            "-" + Environment.MachineName + "-" + Guid.NewGuid().ToString("N")[..6];
        var azure = new BTDB.Replication.Azure.AzureReplicationStorage(container, prefix);
        await RestoreAsync(options, new($"Azure Blob {options.Account}/{options.Container}/{prefix}", azure,
            inventory => azure.Bind(inventory), (inventory, authority) => azure.Bind(inventory, authority),
            async () =>
            {
                long trl = 0, pvl = 0, kvi = 0;
                await foreach (var blob in container.GetBlobsAsync(Azure.Storage.Blobs.Models.BlobTraits.None, Azure.Storage.Blobs.Models.BlobStates.None, prefix + "/", CancellationToken.None))
                    if (blob.Name.EndsWith(".trl", StringComparison.Ordinal)) trl += blob.Properties.ContentLength ?? 0;
                    else if (blob.Name.EndsWith(".kvi", StringComparison.Ordinal)) kvi += blob.Properties.ContentLength ?? 0;
                    else pvl += blob.Properties.ContentLength ?? 0;
                return $"TRL {Mb(trl)}, PVL {Mb(pvl)}, KVI {Mb(kvi)}";
            }));
    }

    /// Build a leader with a published checkpoint (KVI + PVLs) and a TRL tail after it, then restore it on a new node
    /// with an empty cache (cold) and again from the restored node's cache (warm).
    static async Task RestoreAsync(Options options, RestoreRemote remote)
    {
        var valueSize = options.RestoreValueSize ?? 4096;
        var values = checked((int)((long)options.DatasetMb * 1024 * 1024 / valueSize));
        Console.WriteLine();
        Console.WriteLine($"## Restore: {values} x {valueSize} B values with {Node.KeyBytes} B keys ({options.DatasetMb} MiB) with 50% overwritten, compacted and checkpointed, plus a 10% overwrite TRL tail, " +
                          $"{(options.OnDisk ? "on-disk" : "in-memory")} local storage, {remote.Description}, {options.Downloads} concurrent downloads");
        var genesis = new TrlSuccessor(TrlFileName.Key(1), 1);
        var buildStarted = Stopwatch.GetTimestamp();
        using var empty = new BenchmarkReplicationStorage();
        // --reuse-prefix restores a dataset an earlier run with the same parameters published.
        if (options.ReusePrefix == null)
        await using (var leader = await Node.Replicated(options.OnDisk, empty))
        {
            var authority = StopwatchScheduler.Authority();
            using var publisher = new CanonicalTrlPublisher(leader.Db, leader.Capture!, remote.Storage, authority, 1, TrlFileName.Key);
            var value = new byte[valueSize];
            ulong id = 0;
            var random = new Random(42);
            var total = values + values / 2 + values / 10;
            // Keys 1..values, then random overwrites leave partly wasted TRLs that local compaction moves into PVLs.
            async Task WriteBatchAsync(int count, bool overwrite = false)
            {
                for (var i = 0; i < count; i++)
                {
                    random.NextBytes(value.AsSpan(0, 16));
                    ++id;
                    await leader.WriteAsync(id, value, overwrite ? (ulong)random.Next(1, values + 1) : id);
                    if (id % 4096 == 0) await PublishAsync(publisher);
                    if (id % (ulong)Math.Max(1, total / 10) == 0)
                        Console.WriteLine($"  built {id * 100 / (ulong)total} % in {(Stopwatch.GetTimestamp() - buildStarted) / (double)Stopwatch.Frequency:F0} s");
                }
                await PublishAsync(publisher);
            }
            var phase = Stopwatch.GetTimestamp();
            void Phase(string name, long bytes = 0)
            {
                var seconds = (Stopwatch.GetTimestamp() - phase) / (double)Stopwatch.Frequency;
                Console.WriteLine($"  {name}: {seconds:F1} s" + (bytes == 0 ? "" : $", {bytes / 1024.0 / 1024.0 / seconds:F0} MiB/s"));
                phase = Stopwatch.GetTimestamp();
            }
            await WriteBatchAsync(values);
            await WriteBatchAsync(values / 2, true);
            Phase("write and publish TRL", leader.LocalTrlBytes(0));
            // Local compaction moves live values out of wasted TRLs into PVLs, which the checkpoint publishes.
            while (await leader.Db.Compact(CancellationToken.None)) { }
            Phase("local compaction");
            using (var snapshot = leader.Db.CaptureKeyIndexSnapshot())
            {
                var inventory = await CanonicalTrlInventory.DiscoverAsync(remote.Storage, genesis);
                var checkpoint = new CheckpointPublisher(leader.Files!, publisher, remote.BindForMaintenance(inventory, authority));
                if (await checkpoint.PublishAsync(snapshot, default, true) != CheckpointPublishResult.Published)
                    throw new InvalidOperationException("Checkpoint publication failed.");
                Phase("checkpoint (PVL + KVI) publication",
                    (long)snapshot.Sources.Where(s => s.FileType != KVFileType.TransactionLog).Sum(s => (decimal)s.Length));
            }
            await WriteBatchAsync(values / 10, true);
            Phase("tail write and publish");
        }
        Console.WriteLine($"- remote: {await remote.Describe()}; built in {(Stopwatch.GetTimestamp() - buildStarted) / (double)Stopwatch.Frequency:F1} s");
        var directory = options.OnDisk ? Node.NewDirectory() : null;
        try
        {
            foreach (var warm in new[] { false, true })
            {
                if (warm && !options.OnDisk)
                {
                    Console.WriteLine("- warm restore needs on-disk local storage");
                    break;
                }
                var started = Stopwatch.GetTimestamp();
                var inventory = await CanonicalTrlInventory.DiscoverAsync(remote.Storage, genesis);
                var discovered = Stopwatch.GetTimestamp();
                var local = options.OnDisk ? (IReplicationFileStorage)new OnDiskReplicationFileStorage(directory!) : new InMemoryReplicationFileStorage();
                var counting = new CountingRemote(remote.Bind(inventory));
                var files = new ReplicationFileSet(local, counting, options.Downloads);
                await files.InitializeAsync();
                var initialized = Stopwatch.GetTimestamp();
                var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
                {
                    FileCollection = files, TransactionLogCapture = new(), RequireExplicitTransactions = true,
                    Compression = new NoCompressionStrategy(), CompactorScheduler = null, FileSplitSize = Node.FileSplitSize,
                    TransactionLogSizeStrategy = Node.TrlLimits
                });
                var opened = Stopwatch.GetTimestamp();
                using (var transaction = db.StartReadOnlyTransaction())
                    if (transaction.GetKeyValueCount() != values)
                        throw new InvalidOperationException("Restored key count differs.");
                db.Dispose();
                await files.DisposeAsync();
                local.Dispose();
                double Seconds(long from, long to) => (to - from) / (double)Stopwatch.Frequency;
                var openSeconds = Seconds(initialized, opened);
                Console.WriteLine($"- {(warm ? "warm" : "cold")}: total {Seconds(started, opened):F2} s (discovery {Seconds(started, discovered):F2} s, " +
                                  $"file set initialization {Seconds(discovered, initialized):F2} s, open with downloads and replay {openSeconds:F2} s), " +
                                  $"downloaded {Mb(counting.Bytes)} in {counting.Requests} requests ({counting.Bytes / 1024.0 / 1024.0 / Seconds(started, opened):F0} MiB/s overall)");
            }
        }
        finally
        {
            if (directory != null) Directory.Delete(directory, true);
        }
    }
}
