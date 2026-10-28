using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

/// A crash may cut the transaction log, split into small files, at any byte. For every cut the database opens to the
/// state after one of its committed transactions, never earlier for a later cut, and a commit made after the reopen
/// survives the next open, with the state it was made on.
public class TransactionLogCrashCutTest
{
    static KeyValueDBOptions Options(string directory) => new()
    {
        FileCollection = new OnDiskFileCollection(directory), Compression = new NoCompressionStrategy(),
        CompactorScheduler = null, FileSplitSize = 1024
    };

    static string Describe(SortedDictionary<string, string> state) =>
        string.Join(";", state.Select(p => p.Key + "=" + p.Value));

    static async Task<string> Read(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var state = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (cursor.FindFirstKey([]))
            do
            {
                var key = Span<byte>.Empty;
                var value = Span<byte>.Empty;
                state[Convert.ToHexString(cursor.GetKeySpan(ref key))] = Convert.ToHexString(cursor.GetValueSpan(ref value));
            } while (cursor.FindNextKey([]));
        await Task.CompletedTask;
        return Describe(state);
    }

    public static TheoryData<int> Seeds() => new(Enumerable.Range(2, 1));

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task EveryCutOpensToACommittedStateAndKeepsLaterCommits(int seed)
    {
        var random = new Random(seed);
        var source = Path.Combine(Path.GetTempPath(), "btdb-trl-cut-" + Guid.NewGuid().ToString("N"));
        var work = source + "-work";
        Directory.CreateDirectory(source);
        try
        {
            var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
            var states = new List<string> { Describe(model) };
            using (var db = new BTreeKeyValueDB(Options(source)))
            {
                for (var t = 0; t < 10; t++)
                {
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    for (var op = random.Next(1, 4); op > 0; op--)
                    {
                        var key = new[] { (byte)random.Next(6) };
                        if (random.Next(4) == 0)
                        {
                            if (cursor.FindExactKey(key)) cursor.EraseCurrent();
                            model.Remove(Convert.ToHexString(key));
                        }
                        else
                        {
                            var value = new byte[random.Next(3) == 0 ? random.Next(200, 600) : random.Next(1, 6)];
                            random.NextBytes(value);
                            cursor.CreateOrUpdateKeyValue(key, value);
                            model[Convert.ToHexString(key)] = Convert.ToHexString(value);
                        }
                    }

                    tr.Commit();
                    states.Add(Describe(model));
                }
            }

            foreach (var index in Directory.GetFiles(source, "*.kvi")) File.Delete(index);
            var logs = Directory.GetFiles(source, "*.trl").OrderBy(f => f).Select(f => (Name: Path.GetFileName(f),
                Bytes: File.ReadAllBytes(f))).ToList();
            Assert.True(logs.Count > 1, "The log should span several files.");
            Assert.Equal(states[^1], await ReadAt(source));
            var total = logs.Sum(l => l.Bytes.Length);
            var previous = 0;
            for (var cut = 0; cut <= total; cut++)
            {
                if (Directory.Exists(work)) Directory.Delete(work, true);
                Directory.CreateDirectory(work);
                var left = cut;
                foreach (var (name, bytes) in logs)
                {
                    if (left <= 0) break;
                    File.WriteAllBytes(Path.Combine(work, name), bytes[..Math.Min(left, bytes.Length)]);
                    left -= bytes.Length;
                }

                string opened;
                using (var db = new BTreeKeyValueDB(Options(work)))
                {
                    opened = await Read(db);
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([200], [1]);
                    tr.Commit();
                }

                var reached = states.LastIndexOf(opened);
                Assert.True(reached >= previous, $"Seed {seed} cut {cut}: opened {opened}, not a committed state at or after {previous}");
                previous = reached;
                var expected = opened.Length == 0 ? "C8=01" : opened + ";C8=01";
                Assert.True(expected == await ReadAt(work), $"Seed {seed} cut {cut}: the commit after the reopen is lost");
            }

            Assert.Equal(states.Count - 1, previous);
        }
        finally
        {
            Directory.Delete(source, true);
            if (Directory.Exists(work)) Directory.Delete(work, true);
        }
    }

    static async Task<string> ReadAt(string directory)
    {
        using var db = new BTreeKeyValueDB(Options(directory));
        return await Read(db);
    }
}
