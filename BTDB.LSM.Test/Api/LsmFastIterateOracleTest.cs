using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Cursor operations the oracle test does not use (FastIterate, FastIterateNoCursor, KeyHasPrefix, storage sizes) and
/// the others from cursors in every state: current, fuzzy after an erase, and fuzzy before a key renamed past their
/// erased key. BTreeKeyValueDB is the reference for LsmKeyValueDB and InMemoryKeyValueDB.
public class LsmFastIterateOracleTest
{
    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 1000).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task CursorOperationsMatchBTreeKeyValueDB(int seed)
    {
        var random = new Random(seed);
        using var expectedDb = new BTreeKeyValueDB(new InMemoryFileCollection());
        using var lsmDb = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true, MemtableSize = 2048,
            BlockSize = 128, PointDeleteRangeLimit = seed % 3
        });
        using var memoryDb = new InMemoryKeyValueDB();
        IKeyValueDB[] dbs = [expectedDb, lsmDb, memoryDb];
        var keys = Enumerable.Range(0, 30).Select(_ => RandomKey(random)).Distinct().ToArray();
        foreach (var db in dbs)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            foreach (var key in keys) cursor.CreateOrUpdateKeyValue(key, key);
            tr.Commit();
        }

        if (random.Next(2) == 0) await lsmDb.FlushAsync();
        var transactions = new List<IKeyValueDBTransaction>();
        foreach (var db in dbs) transactions.Add(await db.StartWritingTransaction());
        var cursors = Enumerable.Range(0, 3).Select(_ => transactions.Select(t => t.CreateCursor()).ToArray()).ToArray();
        var log = new List<string>();
        try
        {
            for (var step = 0; step < 120; step++)
            {
                if (random.Next(25) == 0)
                {
                    // End the transaction, sometimes flushing the LSM memtable into tables, and start another one.
                    var commit = random.Next(3) != 0;
                    log.Add($"{step}: {(commit ? "commit" : "rollback")}");
                    foreach (var other in cursors)
                    foreach (var x in other)
                        x.Dispose();
                    foreach (var t in transactions)
                    {
                        if (commit) t.Commit();
                        t.Dispose();
                    }

                    if (random.Next(2) == 0) await lsmDb.FlushAsync();
                    transactions.Clear();
                    foreach (var db in dbs) transactions.Add(await db.StartWritingTransaction());
                    cursors = Enumerable.Range(0, 3).Select(_ => transactions.Select(t => t.CreateCursor()).ToArray())
                        .ToArray();
                    Assert.All(transactions, t => Assert.Equal(transactions[0].GetKeyValueCount(), t.GetKeyValueCount()));
                    continue;
                }

                var ci = random.Next(cursors.Length);
                var c = cursors[ci];
                var op = random.Next(20);
                var key = RandomKey(random);
                log.Add($"{step}: op {op} cursor {ci} key {Convert.ToHexString(key)}");
                switch (op)
                {
                    case 0:
                        Same(c, x => x.Find(key, 0));
                        break;
                    case 1:
                        if (c[0].IsValid()) Same(c, x => { x.EraseCurrent(); return 0; });
                        break;
                    case 2:
                        Same(c, x => x.CreateOrUpdateKeyValue(key, key));
                        break;
                    case 3:
                    {
                        var prefixLen = (uint)random.Next(key.Length + 1);
                        Same(c, x => x.UpdateKeySuffix(key, prefixLen));
                        break;
                    }
                    case 4:
                    {
                        var prefix = key[..random.Next(key.Length + 1)];
                        Same(c, x => x.FindNextKey(prefix));
                        break;
                    }
                    case 5:
                    {
                        var prefix = key[..random.Next(key.Length + 1)];
                        Same(c, x => x.FindPreviousKey(prefix));
                        break;
                    }
                    case 6:
                    {
                        var prefix = key[..random.Next(key.Length + 1)];
                        if (c[0].IsValid()) Same(c, x => x.KeyHasPrefix(prefix));
                        break;
                    }
                    case 7:
                    {
                        var index = random.Next(-1, 34);
                        Same(c, x => x.FindKeyIndex(index));
                        break;
                    }
                    case 8:
                    {
                        var other = cursors[random.Next(cursors.Length)];
                        if (other != c && c[0].IsValid() && other[0].IsValid())
                            Same(c, x => x.EraseUpTo(other[Array.IndexOf(c, x)]));
                        break;
                    }
                    case 9:
                        if (c[0].IsValid())
                        {
                            var value = new byte[random.Next(4)];
                            random.NextBytes(value);
                            Same(c, x => { x.SetValue(value); return Convert.ToHexString(x.SlowGetValue()); });
                        }

                        break;
                    case 10:
                        if (c[0].IsValid()) Same(c, x => x.GetStorageSizeOfCurrentKey());
                        break;
                    case 11:
                    {
                        var stop = random.Next(1, 8);
                        Same(c, x => IterateNoCursor(x, stop));
                        break;
                    }
                    case 12:
                    {
                        var stop = random.Next(1, 8);
                        Same(c, x => Iterate(x, stop));
                        break;
                    }
                    case 13:
                    {
                        var prefix = key[..random.Next(key.Length + 1)];
                        Same(c, x => x.FindFirstKey(prefix));
                        break;
                    }
                    case 14:
                    {
                        var prefix = key[..random.Next(key.Length + 1)];
                        Same(c, x => x.FindLastKey(prefix));
                        break;
                    }
                    case 15:
                    {
                        var prefix = key[..random.Next(key.Length + 1)];
                        var index = random.Next(-1, 6);
                        Same(c, x => x.FindKeyIndex(prefix, index));
                        break;
                    }
                    case 16:
                        Same(c, x => x.FindExactKey(key));
                        break;
                    case 17:
                        Same(c, x => { x.Invalidate(); return x.IsValid(); });
                        break;
                    case 18:
                        if (c[0].IsValid()) Same(c, x => Convert.ToHexString(x.SlowGetValue()));
                        break;
                    default:
                        Same(c, x => x.GetKeyIndex());
                        break;
                }

                // A fuzzy cursor is invalid in BTreeKeyValueDB but remembers its key in the others, as the oracle
                // test allows.
                foreach (var other in cursors)
                {
                    if (!other[0].IsValid()) continue;
                    var expectedKey = Convert.ToHexString(other[0].SlowGetKey());
                    for (var i = 1; i < other.Length; i++)
                    {
                        Assert.True(other[i].IsValid(), $"{Name(i)} cursor is invalid");
                        Assert.Equal(other[0].GetKeyIndex(), other[i].GetKeyIndex());
                        Assert.Equal(expectedKey, Convert.ToHexString(other[i].SlowGetKey()));
                    }
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
            foreach (var c in cursors)
            foreach (var x in c)
                x.Dispose();
            foreach (var t in transactions) t.Dispose();
        }
    }

    static string Name(int i) => i == 1 ? "LsmKeyValueDB" : "InMemoryKeyValueDB";

    static void Same<T>(IKeyValueDBCursor[] c, Func<IKeyValueDBCursor, T> operation)
    {
        var expected = Result(() => operation(c[0]));
        for (var i = 1; i < c.Length; i++)
        {
            var cursor = c[i];
            var actual = Result(() => operation(cursor));
            Assert.True(expected == actual, $"{Name(i)}: expected {expected}, actual {actual}");
        }
    }

    static string Result<T>(Func<T> operation)
    {
        try
        {
            return operation()?.ToString() ?? "null";
        }
        catch (Exception ex)
        {
            return ex.GetType().Name;
        }
    }

    static byte[] RandomKey(Random random)
    {
        var key = new byte[random.Next(0, 4)];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(random.Next(4) * 0x55);
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

    /// The same for FastIterateNoCursor, which leaves the cursor on the key that stopped it.
    static string IterateNoCursor(IKeyValueDBCursor cursor, int stop)
    {
        var visited = new List<string>();
        Span<byte> buffer = default;
        cursor.FastIterateNoCursor(ref buffer, (index, key) =>
        {
            visited.Add($"{index}:{Convert.ToHexString(key)}");
            return visited.Count >= stop;
        });
        var stopped = visited.Count >= stop && cursor.IsValid()
            ? $"{cursor.GetKeyIndex()}:{Convert.ToHexString(cursor.SlowGetKey())}"
            : "-";
        return string.Join(",", visited) + " stopped " + stopped;
    }
}
