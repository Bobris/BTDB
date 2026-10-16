using System;
using System.Buffers.Binary;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// A writer moves amounts between accounts while readers check, in their snapshots, that the total never changes and
/// that counts and key indexes agree, with flushes, compactions and deletions running in the background.
public class LsmConcurrencyTest
{
    const int Accounts = 2000;
    const long Total = Accounts * 1000L;

    static byte[] Key(int account)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, account);
        return key;
    }

    static byte[] Value(long amount, int padding)
    {
        var value = new byte[8 + padding];
        BinaryPrimitives.WriteInt64LittleEndian(value, amount);
        return value;
    }

    [Theory]
    [InlineData(true, 0)]
    [InlineData(true, 600)]
    [InlineData(false, 0)]
    public async Task SnapshotsStayConsistentUnderBackgroundWork(bool nativeMemtable, int padding)
    {
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), MemtableSize = 32 * 1024, BlockSize = 1024, IndexPartitionSize = 256,
            Level0CompactionTrigger = 2, LevelBaseBytes = 64 * 1024, LevelMultiplier = 4, LevelCount = 5,
            TargetFileSize = 32 * 1024, NativeMemtable = nativeMemtable, ValueSeparationThreshold = 512,
            BlockCache = new LsmBlockCache(256 * 1024), PointDeleteRangeLimit = 4
        });
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < Accounts; i++) cursor.CreateOrUpdateKeyValue(Key(i), Value(1000, padding));
            tr.Commit();
        }

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(4));
        var checks = 0;
        var readers = new Task[4];
        for (var r = 0; r < readers.Length; r++)
        {
            readers[r] = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    using var tr = db.StartReadOnlyTransaction();
                    using var cursor = tr.CreateCursor();
                    long sum = 0;
                    var index = 0;
                    if (cursor.FindFirstKey([]))
                    {
                        do
                        {
                            Assert.Equal(index, cursor.GetKeyIndex());
                            sum += BinaryPrimitives.ReadInt64LittleEndian(cursor.SlowGetValue());
                            index++;
                        } while (cursor.FindNextKey([]));
                    }

                    Assert.Equal(tr.GetKeyValueCount(), index);
                    Assert.Equal(Total, sum);
                    Interlocked.Increment(ref checks);
                }
            });
        }

        var random = new Random(1);
        var writes = 0;
        while (!stop.IsCancellationRequested)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 20; i++)
            {
                var from = random.Next(Accounts);
                var to = random.Next(Accounts);
                Assert.Equal(FindResult.Exact, cursor.Find(Key(from), 0));
                var fromAmount = BinaryPrimitives.ReadInt64LittleEndian(cursor.SlowGetValue());
                cursor.SetValue(Value(fromAmount - 1, padding));
                Assert.Equal(FindResult.Exact, cursor.Find(Key(to), 0));
                var toAmount = BinaryPrimitives.ReadInt64LittleEndian(cursor.SlowGetValue());
                Assert.False(cursor.CreateOrUpdateKeyValue(Key(to), Value(toAmount + 1, padding)));
            }

            tr.Commit();
            writes++;
        }

        await Task.WhenAll(readers);
        Assert.True(checks > 0 && writes > 0, $"checks {checks}, writes {writes}");
        await db.Compact(default);
        using var final = db.StartReadOnlyTransaction();
        Assert.Equal(Accounts, final.GetKeyValueCount());
    }
}
