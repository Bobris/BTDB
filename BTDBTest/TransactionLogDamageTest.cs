using System;
using System.IO;
using System.Linq;
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
    public void DamagedLengthEndsReplayAtThePreviousTransaction(int field, byte damage)
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-damage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
            {
                foreach (var (key, value) in new (byte, byte)[] { (1, 2), (3, 4), (5, 6) })
                {
                    using var tr = db.StartWritingTransaction().Result;
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
}
