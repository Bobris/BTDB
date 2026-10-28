using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.ReadOnly;
using Xunit;

namespace BTDBTest;

/// Read cursor operations of ReadOnlyKeyValueDB, built from random content with small nodes, compared with
/// BTreeKeyValueDB holding the same content.
public class ReadOnlyKeyValueDBOracleTest
{
    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 400).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void ReadOperationsMatchBTreeKeyValueDB(int seed)
    {
        var random = new Random(seed);
        using var expectedDb = new BTreeKeyValueDB(new InMemoryFileCollection());
        using (var tr = expectedDb.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            var count = random.Next(0, 400);
            for (var i = 0; i < count; i++)
            {
                var key = RandomKey(random);
                var value = new byte[random.Next(0, 6)];
                random.NextBytes(value);
                cursor.CreateOrUpdateKeyValue(key, value);
            }

            tr.Commit();
        }

        byte[] bytes;
        using (var tr = expectedDb.StartReadOnlyTransaction())
        {
            var creator = new ReadOnlyCreator(tr);
            creator.SetMaxChildren((uint)random.Next(2, 8));
            bytes = creator.WriteToArray();
        }
        using var actualDb = ReadOnlyKeyValueDB.Create(bytes);
        using var e = expectedDb.StartReadOnlyTransaction();
        using var a = actualDb.StartReadOnlyTransaction();
        Assert.Equal(e.GetKeyValueCount(), a.GetKeyValueCount());
        var cursors = Enumerable.Range(0, 2).Select(_ => (E: e.CreateCursor(), A: a.CreateCursor())).ToArray();
        var log = new List<string>();
        try
        {
            for (var step = 0; step < 200; step++)
            {
                var (ce, ca) = cursors[random.Next(cursors.Length)];
                var key = RandomKey(random);
                var prefix = key[..random.Next(key.Length + 1)];
                var op = random.Next(11);
                log.Add($"{step}: op {op} key {Convert.ToHexString(key)} prefix {Convert.ToHexString(prefix)}");
                switch (op)
                {
                    case 0:
                        Same(ce, ca, x => x.Find(key, (uint)prefix.Length));
                        break;
                    case 1:
                        Same(ce, ca, x => x.FindFirstKey(prefix));
                        break;
                    case 2:
                        Same(ce, ca, x => x.FindLastKey(prefix));
                        break;
                    case 3:
                        Same(ce, ca, x => x.FindNextKey(prefix));
                        break;
                    case 4:
                        Same(ce, ca, x => x.FindPreviousKey(prefix));
                        break;
                    case 5:
                    {
                        var index = random.Next(-1, (int)e.GetKeyValueCount() + 1);
                        Same(ce, ca, x => x.FindKeyIndex(index));
                        break;
                    }
                    case 6:
                    {
                        var index = random.Next(-1, 6);
                        Same(ce, ca, x => x.FindKeyIndex(prefix, index));
                        break;
                    }
                    case 7:
                        Same(ce, ca, x => x.FindExactKey(key));
                        break;
                    case 8:
                        if (ce.IsValid()) Same(ce, ca, x => x.KeyHasPrefix(prefix));
                        break;
                    case 9:
                    {
                        var stop = random.Next(1, 10);
                        Same(ce, ca, x => Iterate(x, stop));
                        break;
                    }
                    default:
                        Same(ce, ca, x => x.IsValid() ? Convert.ToHexString(x.SlowGetValue()) : "-");
                        break;
                }

                foreach (var (oe, oa) in cursors)
                {
                    Assert.Equal(oe.IsValid(), oa.IsValid());
                    Assert.Equal(oe.GetKeyIndex(), oa.GetKeyIndex());
                    if (oe.IsValid())
                        Assert.Equal(Convert.ToHexString(oe.SlowGetKey()), Convert.ToHexString(oa.SlowGetKey()));
                }
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Seed {seed}:{Environment.NewLine}{string.Join(Environment.NewLine, log.TakeLast(10))}",
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

    static void Same<T>(IKeyValueDBCursor expected, IKeyValueDBCursor actual, Func<IKeyValueDBCursor, T> operation)
    {
        var e = Result(() => operation(expected));
        var a = Result(() => operation(actual));
        Assert.True(e == a, $"expected {e}, actual {a}");
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
        var key = new byte[random.Next(0, 5)];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(random.Next(4) * 0x55);
        return key;
    }

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
