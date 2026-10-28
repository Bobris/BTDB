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
    sealed class RecordingCollection : IFileCollection
    {
        public readonly InMemoryFileCollection Inner = new();
        readonly Dictionary<uint, string> _hints = new();
        readonly Dictionary<uint, ulong> _flushed = new();
        public int Operations;
        public int CrashAt = -1;
        public List<(string Name, byte[] Bytes, ulong Flushed)>? Snapshot;

        void Operation()
        {
            if (Operations++ != CrashAt) return;
            Snapshot = Inner.Enumerate().Select(f =>
            {
                var bytes = new byte[f.GetSize()];
                f.RandomRead(bytes, 0, false);
                return (f.Index.ToString("D8") + "." + _hints[f.Index], bytes, _flushed.GetValueOrDefault(f.Index));
            }).ToList();
        }

        public IFileCollectionFile AddFile(string humanHint)
        {
            Operation();
            var file = Inner.AddFile(humanHint);
            _hints[file.Index] = humanHint;
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

            void Flushed(Action flush)
            {
                owner.Operation();
                flush();
                owner._flushed[inner.Index] = inner.GetSize();
            }

            public void HardFlush() => Flushed(inner.HardFlush);
            public void HardFlushTruncateSwitchToReadOnlyMode() => Flushed(inner.HardFlushTruncateSwitchToReadOnlyMode);
            public void HardFlushTruncateSwitchToDisposedMode() => Flushed(inner.HardFlushTruncateSwitchToDisposedMode);

            public void Remove()
            {
                owner.Operation();
                inner.Remove();
            }
        }
    }

    static string Short(string state) => string.Join(";", state.Split(';', StringSplitOptions.RemoveEmptyEntries)
        .Select(p => p.Split('=')).Select(p => $"{p[0]}={p[1][..Math.Min(4, p[1].Length)]}x{p[1].Length / 2}"));

    static KeyValueDBOptions Options(IFileCollection files) => new()
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
        FileSplitSize = 4096
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

    // Commits, compactions and key index writes; returns the state after every commit.
    static async Task<List<string>> Workload(RecordingCollection files, int seed)
    {
        var random = new Random(seed);
        var states = new List<string> { "" };
        using var db = new BTreeKeyValueDB(Options(files));
        for (var t = 0; t < 30; t++)
        {
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                var key = new[] { (byte)random.Next(10) };
                if (random.Next(4) == 0 && cursor.FindExactKey(key)) cursor.EraseCurrent();
                else cursor.CreateOrUpdateKeyValue(key, Enumerable.Repeat((byte)t, random.Next(3) == 0 ? 900 : 4).ToArray());
                tr.Commit();
            }

            states.Add(Dump(db));
            if (t % 7 == 6) await db.Compact(CancellationToken.None);
            if (t % 11 == 10) db.CreateKvi(CancellationToken.None);
        }

        return states;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task EveryCrashOpensToACommittedStateAndKeepsLaterCommits(int seed)
    {
        int total;
        List<string> states;
        using (var counting = new RecordingCollection())
        {
            states = await Workload(counting, seed);
            total = counting.Operations;
        }

        var random = new Random(seed);
        for (var crash = 0; crash < total; crash++)
        {
            using var files = new RecordingCollection { CrashAt = crash };
            await Workload(files, seed);
            var directory = Path.Combine(Path.GetTempPath(), "btdb-crash-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            try
            {
                foreach (var (name, bytes, flushed) in files.Snapshot!)
                    System.IO.File.WriteAllBytes(Path.Combine(directory, name),
                        bytes[..(int)((ulong)random.NextInt64((long)flushed, bytes.Length + 1L))]);
                string opened;
                using (var db = new BTreeKeyValueDB(Options(new OnDiskFileCollection(directory))))
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
                using var again = new BTreeKeyValueDB(Options(new OnDiskFileCollection(directory)));
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
