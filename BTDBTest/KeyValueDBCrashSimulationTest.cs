using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

/// A crash at any file creation, hard flush or removal of a workload with commits, compactions and key index writes
/// leaves each file with what was hard flushed plus any part of what was written after. The database then opens to
/// the state after one of its commits, and a commit made after that open survives the next one.
public class KeyValueDBCrashSimulationTest
{
    sealed class RecordingCollection(IFileCollection? inner = null) : IFileCollection
    {
        public readonly IFileCollection Inner = inner ?? new InMemoryFileCollection();
        readonly Dictionary<uint, string> _hints = new();
        readonly Dictionary<uint, ulong> _flushed = new();
        public int Operations;
        public int CrashAt = -1;
        public List<(string Name, byte[] Bytes, ulong Flushed)>? Snapshot;

        // Each operation is counted, and a crash snapshot taken, atomically with its effect, as other threads run too.
        void Operation(Action effect)
        {
            lock (this)
            {
                OperationLocked();
                effect();
            }
        }

        void OperationLocked()
        {
            if (Operations++ != CrashAt) return;
            Snapshot = Inner.Enumerate().Select(f =>
            {
                var bytes = new byte[f.GetSize()];
                f.RandomRead(bytes, 0, false);
                return (f.Index.ToString("D8") + "." + _hints[f.Index], bytes, _flushed.GetValueOrDefault(f.Index));
            }).ToList();
        }

        /// Files already in the folder are durable as they are.
        public RecordingCollection AdoptExisting(string folder)
        {
            foreach (var path in System.IO.Directory.GetFiles(folder))
            {
                var index = uint.Parse(Path.GetFileNameWithoutExtension(path));
                _hints[index] = Path.GetExtension(path)[1..];
                _flushed[index] = (ulong)new FileInfo(path).Length;
            }

            return this;
        }

        public IFileCollectionFile AddFile(string humanHint)
        {
            IFileCollectionFile file = null!;
            Operation(() =>
            {
                file = Inner.AddFile(humanHint);
                _hints[file.Index] = humanHint;
            });
            return new File(this, file);
        }

        public uint GetCount() => Inner.GetCount();
        public IFileCollectionFile GetFile(uint index) => Inner.GetFile(index) is { } f ? new File(this, f) : null!;
        public IEnumerable<IFileCollectionFile> Enumerate() => Inner.Enumerate().Select(f => new File(this, f));
        public void ConcurrentTemporaryTruncate(uint index, uint offset) => Inner.ConcurrentTemporaryTruncate(index, offset);
        public void Dispose() => Inner.Dispose();

        sealed class File(RecordingCollection owner, IFileCollectionFile inner) : IFileCollectionFile
        {
            public uint Index => inner.Index;
            public IMemReader GetExclusiveReader() => inner.GetExclusiveReader();
            public void AdvisePrefetch() => inner.AdvisePrefetch();
            public void RandomRead(Span<byte> data, ulong position, bool doNotCache) =>
                inner.RandomRead(data, position, doNotCache);
            public IMemWriter GetAppenderWriter() => inner.GetAppenderWriter();
            public IMemWriter GetExclusiveAppenderWriter() => inner.GetExclusiveAppenderWriter();
            public ulong GetSize() => inner.GetSize();

            void Flushed(Action flush) => owner.Operation(() =>
            {
                flush();
                owner._flushed[inner.Index] = inner.GetSize();
            });

            public void HardFlush() => Flushed(inner.HardFlush);
            public void HardFlushTruncateSwitchToReadOnlyMode() => Flushed(inner.HardFlushTruncateSwitchToReadOnlyMode);
            public void HardFlushTruncateSwitchToDisposedMode() => Flushed(inner.HardFlushTruncateSwitchToDisposedMode);

            public void Remove() => owner.Operation(inner.Remove);
        }
    }

    static string Short(string state) => string.Join(";", state.Split(';', StringSplitOptions.RemoveEmptyEntries)
        .Select(p => p.Split('=')).Select(p => $"{p[0]}={p[1][..Math.Min(4, p[1].Length)]}x{p[1].Length / 2}"));

    sealed class SmallLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint transactionLogFileId) => new(1024, 1300);
    }

    static KeyValueDBOptions Options(IFileCollection files, bool split = false) => new()
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
        FileSplitSize = 4096, TransactionLogSizeStrategy = split ? new SmallLogs() : null
    };

    static string Dump(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var parts = new List<string>();
        if (cursor.FindFirstKey([]))
            do
            {
                var key = Span<byte>.Empty;
                var value = Span<byte>.Empty;
                parts.Add(Convert.ToHexString(cursor.GetKeySpan(ref key)) + "=" +
                          Convert.ToHexString(cursor.GetValueSpan(ref value)));
            } while (cursor.FindNextKey([]));
        return string.Join(";", parts);
    }

    // Commits, compactions and key index writes; returns the state after every commit and the file operations done
    // when it returned.
    static async Task<(List<string> States, List<int> Done)> Workload(RecordingCollection files, int seed,
        bool durable, bool batches = false, bool split = false)
    {
        var random = new Random(seed);
        var states = new List<string> { "" };
        var done = new List<int> { 0 };
        // Every commit's state, also of batched commits that are not published yet.
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using var db = new BTreeKeyValueDB(Options(files, split));
        db.DurableTransactions = durable;
        for (var t = 0; t < 30; t++)
        {
            using (var tr = await db.StartWritingTransaction(batches && random.Next(3) != 0))
            {
                using var cursor = tr.CreateCursor();
                for (var op = split ? random.Next(1, 5) : 1; op > 0; op--)
                {
                    var key = new[] { (byte)random.Next(10) };
                    if (random.Next(4) == 0 && cursor.FindExactKey(key))
                    {
                        cursor.EraseCurrent();
                        model.Remove(Convert.ToHexString(key));
                    }
                    else
                    {
                        var value = Enumerable.Repeat((byte)t, random.Next(3) == 0 ? 900 : 4).ToArray();
                        cursor.CreateOrUpdateKeyValue(key, value);
                        model[Convert.ToHexString(key)] = Convert.ToHexString(value);
                    }
                }

                if (batches && random.Next(4) == 0) db.FinishTransactionBatchAfterCurrentTransaction();
                tr.Commit();
            }

            states.Add(string.Join(";", model.Select(p => p.Key + "=" + p.Value)));
            done.Add(files.Operations);
            if (t % 7 == 6) await db.Compact(CancellationToken.None);
            if (t % 11 == 10) db.CreateKvi(CancellationToken.None);
        }

        return (states, done);
    }

    /// With durable transactions a crash also keeps every commit that returned before it.
    [Theory]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    [InlineData(3, true)]
    public async Task EveryCrashOpensToACommittedStateAndKeepsLaterCommits(int seed, bool durable) =>
        await CrashEverywhere(seed, durable, false);

    /// Batched transactions publish later, but each commit is in the log: a crash keeps a prefix of the commits.
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task EveryCrashOfBatchedCommitsOpensToACommittedState(int seed) => await CrashEverywhere(seed, false, true);

    static void WriteSnapshot(RecordingCollection files, string directory, Random random)
    {
        Directory.CreateDirectory(directory);
        foreach (var (name, bytes, flushed) in files.Snapshot!)
            System.IO.File.WriteAllBytes(Path.Combine(directory, name),
                bytes[..(int)((ulong)random.NextInt64((long)flushed, bytes.Length + 1L))]);
    }

    static void Copy(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.GetFiles(from)) System.IO.File.Copy(file, Path.Combine(to, Path.GetFileName(file)));
    }

    static async Task<string> OpenAndCommit(IFileCollection files)
    {
        using var db = new BTreeKeyValueDB(Options(files));
        var opened = Dump(db);
        using var tr = await db.StartWritingTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([200], [1]);
        tr.Commit();
        return opened;
    }

    /// A second crash during the open that recovers from the first one, at any of its file operations, still opens
    /// to what the first crash left, with or without the commit made after that open.
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task CrashDuringRecoveryKeepsWhatTheFirstCrashLeft(int seed)
    {
        int total;
        using (var counting = new RecordingCollection())
        {
            await Workload(counting, seed, false, false);
            total = counting.Operations;
        }

        var recoveries = 0;
        var random = new Random(seed);
        var root = Path.Combine(Path.GetTempPath(), "btdb-crash2-" + Guid.NewGuid().ToString("N"));
        try
        {
            for (var crash = 0; crash < total; crash++)
            {
                using var files = new RecordingCollection { CrashAt = crash };
                await Workload(files, seed, false, false);
                var first = Path.Combine(root, $"{crash}");
                WriteSnapshot(files, first, random);
                var probe = first + "-probe";
                Copy(first, probe);
                string left;
                int recoveryOperations;
                using (var recording = new RecordingCollection(new OnDiskFileCollection(probe)).AdoptExisting(probe))
                {
                    left = await OpenAndCommit(recording);
                    recoveryOperations = recording.Operations;
                }

                for (var second = 0; second < recoveryOperations; second++, recoveries++)
                {
                    var during = $"{first}-{second}";
                    Copy(first, during);
                    using (var recording = new RecordingCollection(new OnDiskFileCollection(during)) { CrashAt = second }
                               .AdoptExisting(during))
                    {
                        await OpenAndCommit(recording);
                        var after = during + "-after";
                        WriteSnapshot(recording, after, random);
                        using var db = new BTreeKeyValueDB(Options(new OnDiskFileCollection(after)));
                        var opened = Dump(db);
                        Assert.True(opened == left || opened == (left.Length == 0 ? "C8=01" : left + ";C8=01"),
                            $"Seed {seed} crash {crash}, then {second} of {recoveryOperations} during recovery: opened " +
                            $"{Short(opened)} instead of {Short(left)}");
                    }
                }
            }
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }

        Assert.True(recoveries > total, "Recovery should do file operations to crash in.");
    }

    /// With small log size limits a transaction is split between files, so a crash can cut it at a file boundary.
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task EveryCrashOfSplitTransactionsOpensToACommittedState(int seed) =>
        await CrashEverywhere(seed, true, false, true);

    /// A crash at a random file operation while compaction and CreateKvi run beside a writer opens to a committed
    /// state that takes a later commit, and with durable transactions keeps every commit that returned before it.
    [Theory]
    [InlineData(1, true)]
    [InlineData(2, true)]
    [InlineData(3, true)]
    [InlineData(4, true)]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(3, false)]
    [InlineData(4, false)]
    public async Task CrashWhileCompactingBesideAWriterKeepsDurableCommits(int seed, bool durable)
    {
        var random = new Random(seed);
        for (var round = 0; round < 25; round++)
        {
            using var files = new RecordingCollection { CrashAt = random.Next(10, 120) };
            var states = new List<string> { "" };
            var done = new List<int> { 0 };
            var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
            using (var db = new BTreeKeyValueDB(Options(files)))
            {
                db.DurableTransactions = durable;
                using var stop = new CancellationTokenSource();
                var compactor = Task.Run(async () =>
                {
                    while (!stop.IsCancellationRequested)
                    {
                        await db.Compact(CancellationToken.None);
                        if (Random.Shared.Next(3) == 0) db.CreateKvi(CancellationToken.None);
                    }
                });
                for (var t = 0; t < 40 && files.Snapshot == null; t++)
                {
                    using (var tr = await db.StartWritingTransaction())
                    {
                        using var cursor = tr.CreateCursor();
                        var key = new[] { (byte)random.Next(10) };
                        var value = Enumerable.Repeat((byte)t, random.Next(2) == 0 ? 900 : 4).ToArray();
                        cursor.CreateOrUpdateKeyValue(key, value);
                        model[Convert.ToHexString(key)] = Convert.ToHexString(value);
                        tr.Commit();
                    }

                    states.Add(string.Join(";", model.Select(p => p.Key + "=" + p.Value)));
                    lock (files) done.Add(files.Operations);
                }

                await stop.CancelAsync();
                await compactor;
            }

            if (files.Snapshot == null) continue;
            var directory = Path.Combine(Path.GetTempPath(), "btdb-crash-concurrent-" + Guid.NewGuid().ToString("N"));
            try
            {
                WriteSnapshot(files, directory, random);
                var opened = await OpenAndCommit(new OnDiskFileCollection(directory));
                var acknowledged = durable ? done.FindLastIndex(d => d <= files.CrashAt) : 0;
                Assert.True(Enumerable.Range(acknowledged, states.Count - acknowledged).Any(i => states[i] == opened),
                    $"Seed {seed} round {round} crash {files.CrashAt}: opened {Short(opened)}, not a commit at or after " +
                    $"{acknowledged}, which returned before the crash");
                using var again = new BTreeKeyValueDB(Options(new OnDiskFileCollection(directory)));
                Assert.True((opened.Length == 0 ? "C8=01" : opened + ";C8=01") == Dump(again),
                    $"Seed {seed} round {round}: the commit after the reopen is lost");
            }
            finally
            {
                if (Directory.Exists(directory)) Directory.Delete(directory, true);
            }
        }
    }

    sealed class WarningLogger : IKeyValueDBLogger
    {
        public string? LastWarning;
        public void ReportTransactionLeak(IKeyValueDBTransaction transaction) { }
        public void CompactionStart(ulong totalWaste) { }
        public void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory) { }
        public void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration,
            ulong beforeCompressionSize) { }
        public void TransactionLogCreated(uint fileId) { }
        public void FileMarkedForDelete(uint fileId) { }
        public void LogWarning(string message) => LastWarning = message;
    }

    /// While a writing transaction holds the transaction log, CreateKvi cannot flush it, so it writes a key index only
    /// when the last commit is already durable, and otherwise skips it with a warning.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CreateKviBesideAWriterIndexesOnlyDurableCommits(bool durable)
    {
        using var files = new RecordingCollection();
        var logger = new WarningLogger();
        using var db = new BTreeKeyValueDB(Options(files));
        db.Logger = logger;
        db.DurableTransactions = durable;
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([1], [1]);
            tr.Commit();
        }

        using var writer = await db.StartWritingTransaction();
        using (var cursor = writer.CreateCursor()) cursor.CreateOrUpdateKeyValue([2], [2]);
        var indexes = files.Inner.Enumerate().Count();
        db.CreateKvi(CancellationToken.None);
        Assert.Equal(indexes + (durable ? 1 : 0), files.Inner.Enumerate().Count());
        Assert.Equal(!durable, logger.LastWarning?.StartsWith("CreateKvi skipped") ?? false);
    }

    async Task CrashEverywhere(int seed, bool durable, bool batches, bool split = false)
    {
        int total;
        List<string> states;
        List<int> done;
        using (var counting = new RecordingCollection())
        {
            (states, done) = await Workload(counting, seed, durable, batches, split);
            total = counting.Operations;
        }

        var random = new Random(seed);
        for (var crash = 0; crash < total; crash++)
        {
            using var files = new RecordingCollection { CrashAt = crash };
            await Workload(files, seed, durable, batches, split);
            var directory = Path.Combine(Path.GetTempPath(), "btdb-crash-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            try
            {
                foreach (var (name, bytes, flushed) in files.Snapshot!)
                    System.IO.File.WriteAllBytes(Path.Combine(directory, name),
                        bytes[..(int)((ulong)random.NextInt64((long)flushed, bytes.Length + 1L))]);
                string opened;
                using (var db = new BTreeKeyValueDB(Options(new OnDiskFileCollection(directory), split)))
                {
                    opened = Dump(db);
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([200], [1]);
                    tr.Commit();
                }

                Assert.True(states.Contains(opened), $"Seed {seed} crash {crash}/{total}: opened {Short(opened)}, " +
                    "which no commit made; files at the crash: " +
                    string.Join(", ", files.Snapshot!.Select(f => $"{f.Name} {f.Bytes.Length} flushed {f.Flushed}")));
                if (durable)
                {
                    var acknowledged = done.FindLastIndex(d => d <= crash);
                    Assert.True(Enumerable.Range(acknowledged, states.Count - acknowledged).Any(i => states[i] == opened),
                        $"Seed {seed} crash {crash}/{total}: lost commit {acknowledged}, which returned before the crash");
                }

                using var again = new BTreeKeyValueDB(Options(new OnDiskFileCollection(directory), split));
                Assert.True((opened.Length == 0 ? "C8=01" : opened + ";C8=01") == Dump(again),
                    $"Seed {seed} crash {crash}/{total}: the commit after the reopen is lost");
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }
    }
}
