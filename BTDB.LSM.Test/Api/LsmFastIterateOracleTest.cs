using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// FastIterate and KeyHasPrefix, which the oracle test does not use, from cursors in every state: current, fuzzy after
/// an erase, and fuzzy before a key renamed past their erased key, compared with BTreeKeyValueDB.
public class LsmFastIterateOracleTest
{
    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 200).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task FastIterateMatchesBTreeKeyValueDB(int seed)
    {
        var random = new Random(seed);
        using var expectedDb = new BTreeKeyValueDB(new InMemoryFileCollection());
        using var actualDb = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true, MemtableSize = 2048,
            BlockSize = 128, PointDeleteRangeLimit = seed % 3
        });
        var keys = Enumerable.Range(0, 30).Select(_ => RandomKey(random)).Distinct().ToArray();
        foreach (var db in new IKeyValueDB[] { expectedDb, actualDb })
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            foreach (var key in keys) cursor.CreateOrUpdateKeyValue(key, key);
            tr.Commit();
        }

        if (random.Next(2) == 0) await actualDb.FlushAsync();
        using var e = await expectedDb.StartWritingTransaction();
        using var a = await actualDb.StartWritingTransaction();
        var cursors = Enumerable.Range(0, 3).Select(_ => (E: e.CreateCursor(), A: a.CreateCursor())).ToArray();
        var log = new List<string>();
        try
        {
            for (var step = 0; step < 60; step++)
            {
                var (ce, ca) = cursors[random.Next(cursors.Length)];
                var op = random.Next(10);
                var key = RandomKey(random);
                log.Add($"{step}: op {op} key {Convert.ToHexString(key)}");
                switch (op)
                {
                    case 0:
                        Assert.Equal(ce.Find(key, 0), ca.Find(key, 0));
                        break;
                    case 1:
                        if (ce.IsValid())
                        {
                            ce.EraseCurrent();
                            ca.EraseCurrent();
                        }

                        break;
                    case 2:
                        Assert.Equal(ce.CreateOrUpdateKeyValue(key, key), ca.CreateOrUpdateKeyValue(key, key));
                        break;
                    case 3:
                    {
                        var prefixLen = (uint)random.Next(key.Length + 1);
                        Assert.Equal(ce.UpdateKeySuffix(key, prefixLen), ca.UpdateKeySuffix(key, prefixLen));
                        break;
                    }
                    case 4:
                        Assert.Equal(ce.FindNextKey([]), ca.FindNextKey([]));
                        break;
                    case 5:
                    {
                        var prefix = key[..random.Next(key.Length + 1)];
                        if (ce.IsValid()) Assert.Equal(ce.KeyHasPrefix(prefix), ca.KeyHasPrefix(prefix));
                        break;
                    }
                    default:
                    {
                        var stop = random.Next(1, 8);
                        Assert.Equal(Iterate(ce, stop), Iterate(ca, stop));
                        break;
                    }
                }

                // A fuzzy cursor is invalid in BTreeKeyValueDB but remembers its key in LsmKeyValueDB, as the oracle
                // test allows.
                foreach (var (oe, oa) in cursors)
                {
                    if (!oe.IsValid()) continue;
                    Assert.True(oa.IsValid());
                    Assert.Equal(oe.GetKeyIndex(), oa.GetKeyIndex());
                }
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Seed {seed}:{Environment.NewLine}{string.Join(Environment.NewLine, log.TakeLast(12))}",
                ex);
        }
        finally
        {
            foreach (var (ce, ca) in cursors)
            {
                ce.Dispose();
                ca.Dispose();
            }
        }
    }

    static byte[] RandomKey(Random random)
    {
        var key = new byte[random.Next(0, 3)];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(random.Next(3) * 0x7F);
        return key;
    }

    /// The (index, key) pairs visited until the callback stops after stop keys.
    static string Iterate(IKeyValueDBCursor cursor, int stop)
    {
        var visited = new List<string>();
        Span<byte> buffer = default;
        cursor.FastIterate(ref buffer, (index, key) =>
        {
            visited.Add($"{index}:{Convert.ToHexString(key)}");
            return visited.Count >= stop;
        });
        return string.Join(",", visited);
    }
}
