using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

/// Replay stops at transaction log content it cannot apply, as after a crash, and keeps the transactions before it.
public class TransactionLogDamageTest
{
    static KeyValueDBOptions Options(string directory) => new()
    {
        FileCollection = new OnDiskFileCollection(directory), Compression = new NoCompressionStrategy(),
        CompactorScheduler = null
    };

    [Theory]
    [InlineData(1, 0x7F)] // value length -1, read as an inline value
    [InlineData(0, 0x7F)] // key length -1
    [InlineData(0, 0xFF)] // key length that overflows an int
    public async Task DamagedLengthEndsReplayAtThePreviousTransaction(int field, byte damage)
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-damage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
            {
                foreach (var (key, value) in new (byte, byte)[] { (1, 2), (3, 4), (5, 6) })
                {
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([key], [value]);
                    tr.Commit();
                }
            }

            var log = Directory.GetFiles(directory, "*.trl").Single();
            var content = File.ReadAllBytes(log);
            // Key length 1 and value length 1 (VInt 0x81 each), then key 3 and value 4.
            var at = Enumerable.Range(0, content.Length - 3).Single(i =>
                content[i] == 0x81 && content[i + 1] == 0x81 && content[i + 2] == 3 && content[i + 3] == 4);
            content[at + field] = damage;
            File.WriteAllBytes(log, content);

            using var reopened = new BTreeKeyValueDB(Options(directory));
            using var read = reopened.StartReadOnlyTransaction();
            using var check = read.CreateCursor();
            Assert.True(check.FindExactKey([1]));
            Assert.False(check.FindExactKey([3]));
            Assert.False(check.FindExactKey([5]));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    /// A key length far beyond the file, as damage can leave, ends replay like other damage instead of allocating the
    /// length or failing the open with OutOfMemoryException.
    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(1 << 30)]
    public async Task KeyLengthBeyondTheFileEndsReplay(int length)
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-damage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
                foreach (var (key, value) in new (byte, byte)[] { (1, 2), (3, 4), (5, 6) })
                {
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([key], [value]);
                    tr.Commit();
                }

            var log = Directory.GetFiles(directory, "*.trl").Single();
            var content = File.ReadAllBytes(log);
            var at = Enumerable.Range(0, content.Length - 3).Single(i =>
                content[i] == 0x81 && content[i + 1] == 0x81 && content[i + 2] == 3 && content[i + 3] == 4);
            var writer = new BTDB.StreamLayer.MemWriter();
            writer.WriteVInt32(length);
            File.WriteAllBytes(log, [.. content[..at], .. writer.GetSpan().ToArray(), .. content[(at + 1)..]]);

            var before = GC.GetAllocatedBytesForCurrentThread();
            using var reopened = new BTreeKeyValueDB(Options(directory));
            Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 64 << 20);
            using var read = reopened.StartReadOnlyTransaction();
            using var check = read.CreateCursor();
            Assert.True(check.FindExactKey([1]));
            Assert.False(check.FindExactKey([3]));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    /// A crash that cut the log after a complete command of a transaction leaves it unfinished at the end of the log.
    /// The next session writes a new log starting with its own transaction; replay drops the unfinished one there and
    /// keeps the new commits. Before the 2026-10-28 fix replay stopped at that start, losing every later commit.
    [Fact]
    public async Task CommitsAfterACrashInsideATransactionSurviveTheNextOpen()
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-damage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
                foreach (var (key, value) in new (byte, byte)[] { (1, 2), (3, 4), (5, 6) })
                {
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([key], [value]);
                    tr.Commit();
                }

            foreach (var index in Directory.GetFiles(directory, "*.kvi")) File.Delete(index);
            var log = Directory.GetFiles(directory, "*.trl").Single();
            var content = File.ReadAllBytes(log);
            var at = Enumerable.Range(0, content.Length - 3).Single(i =>
                content[i] == 0x81 && content[i + 1] == 0x81 && content[i + 2] == 3 && content[i + 3] == 4);
            File.WriteAllBytes(log, content[..(at + 4)]); // after the command writing key 3, before its commit

            using (var reopened = new BTreeKeyValueDB(Options(directory)))
            {
                using var tr = await reopened.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue([200], [1]);
                tr.Commit();
            }

            using var again = new BTreeKeyValueDB(Options(directory));
            using var read = again.StartReadOnlyTransaction();
            using var check = read.CreateCursor();
            Assert.True(check.FindExactKey([1]));
            Assert.False(check.FindExactKey([3]));
            Assert.True(check.FindExactKey([200]));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    /// A damaged predecessor link that points back at a later log closes the chain into a cycle: opening fails
    /// instead of following it forever.
    [Fact]
    public async Task PredecessorLinkCycleFailsTheOpen()
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-damage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var options = Options(directory);
            options.FileSplitSize = 1024;
            using (var db = new BTreeKeyValueDB(options))
                for (var i = 0; i < 12; i++)
                {
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([(byte)i], new byte[300]);
                    tr.Commit();
                }

            foreach (var index in Directory.GetFiles(directory, "*.kvi")) File.Delete(index);
            var logs = Directory.GetFiles(directory, "*.trl").Order().ToArray();
            Assert.True(logs.Length > 2);
            var first = File.ReadAllBytes(logs[0]);
            // "BTDB3", the Guid, the file type, generation 1 (VInt 0x81), then the predecessor ID 0 (VUInt 0x80).
            Assert.Equal([0x81, 0x80], first[22..24]);
            first[23] = (byte)(0x80 | logs.Length);
            File.WriteAllBytes(logs[0], first);
            await Assert.ThrowsAsync<BTDBException>(() =>
                Task.Run(() => new BTreeKeyValueDB(Options(directory)).Dispose()).WaitAsync(TimeSpan.FromSeconds(20)));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task KeySuffixUpdateWithPrefixLongerThanKeyEndsReplay()
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-damage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
            {
                using (var tr = await db.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([7, 2], [9]);
                    tr.Commit();
                }

                using (var tr = await db.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    Assert.Equal(UpdateKeySuffixResult.Updated, cursor.UpdateKeySuffix([7, 3], 1));
                    tr.Commit();
                }
            }

            var log = Directory.GetFiles(directory, "*.trl").Single();
            var content = File.ReadAllBytes(log);
            // Prefix length 1 and key length 2 (VUInt), then the new key.
            var at = Enumerable.Range(0, content.Length - 3).Single(i =>
                content[i] == 1 && content[i + 1] == 2 && content[i + 2] == 7 && content[i + 3] == 3);
            content[at] = 5;
            File.WriteAllBytes(log, content);

            using var reopened = new BTreeKeyValueDB(Options(directory));
            using var read = reopened.StartReadOnlyTransaction();
            using var check = read.CreateCursor();
            Assert.True(check.FindExactKey([7, 2]));
            Assert.False(check.FindExactKey([7, 3]));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    /// A ulong index the array of ulongs cannot hold, read as uint.MaxValue or 2^31.
    [Theory]
    [InlineData(new byte[] { 0xF0, 0xFF, 0xFF, 0xFF, 0xFF })]
    [InlineData(new byte[] { 0xF0, 0x80, 0x00, 0x00, 0x00 })]
    public async Task UlongIndexBeyondAnyArrayEndsReplay(byte[] index)
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-damage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
            {
                using (var tr = await db.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue([1], [2]);
                    tr.Commit();
                }

                using (var tr = await db.StartWritingTransaction())
                {
                    tr.SetUlong(3, 77);
                    tr.Commit();
                }
            }

            var log = Directory.GetFiles(directory, "*.trl").Single();
            var content = File.ReadAllBytes(log);
            // DeltaUlongs (10), index 3 and delta 77 as VUInts.
            var at = Enumerable.Range(0, content.Length - 2).Single(i =>
                content[i] == 10 && content[i + 1] == 3 && content[i + 2] == 77);
            File.WriteAllBytes(log, [.. content[..(at + 1)], .. index, .. content[(at + 2)..]]);

            using var reopened = new BTreeKeyValueDB(Options(directory));
            using var read = reopened.StartReadOnlyTransaction();
            using var check = read.CreateCursor();
            Assert.True(check.FindExactKey([1]));
            Assert.Equal(0ul, read.GetUlong(3));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task SetUlongRejectsIndexBeyondAnyArray()
    {
        using var db = new InMemoryKeyValueDB();
        using var tr = await db.StartWritingTransaction();
        Assert.Throws<ArgumentOutOfRangeException>(() => tr.SetUlong(uint.MaxValue, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => tr.SetUlong(1u << 31, 1));
    }
}
