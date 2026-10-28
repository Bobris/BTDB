using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BTDB;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

/// A transaction log with random bytes changed, inserted, removed or cut off opens: replay stops at what it cannot
/// apply, as after a crash, instead of failing the database. The opened database then takes a commit that the next open
/// keeps.
public class TransactionLogFuzzTest
{
    static KeyValueDBOptions Options(string directory) => new()
    {
        FileCollection = new OnDiskFileCollection(directory), Compression = new NoCompressionStrategy(),
        CompactorScheduler = null
    };

    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 40));

    static byte[] Bytes(Random random, int length)
    {
        var result = new byte[length];
        random.NextBytes(result);
        return result;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task MutatedLogOpensAndStaysUsable(int seed)
    {
        var random = new Random(seed);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-trl-fuzz-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
            {
                for (var t = 0; t < 30; t++)
                {
                    using var tr = await db.StartWritingTransaction();
                    using var cursor = tr.CreateCursor();
                    for (var op = random.Next(1, 5); op > 0; op--)
                    {
                        var key = new[] { (byte)random.Next(4), (byte)random.Next(8) };
                        switch (random.Next(6))
                        {
                            case 0:
                                if (cursor.FindExactKey(key)) cursor.EraseCurrent();
                                break;
                            case 1:
                                using (var to = tr.CreateCursor())
                                    if (cursor.FindFirstKey(key[..1]) && to.FindLastKey(key[..1]))
                                        cursor.EraseUpTo(to);
                                break;
                            case 2:
                                if (cursor.FindFirstKey(key[..1]))
                                    cursor.UpdateKeySuffix([key[0], (byte)random.Next(8), 9], 1);
                                break;
                            case 3:
                                tr.SetUlong((uint)random.Next(3), (ulong)random.NextInt64());
                                break;
                            default:
                                cursor.CreateOrUpdateKeyValue(key, Bytes(random, random.Next(3) == 0 ? 3000 : random.Next(8)));
                                break;
                        }
                    }

                    tr.SetCommitUlong((ulong)t);
                    tr.Commit();
                }
            }

            foreach (var index in Directory.GetFiles(directory, "*.kvi")) File.Delete(index);
            var log = Directory.GetFiles(directory, "*.trl").Single();
            var original = File.ReadAllBytes(log);
            for (var round = 0; round < 15; round++)
            {
                var content = original.ToList();
                for (var changes = random.Next(1, 4); changes > 0; changes--)
                {
                    var at = random.Next(content.Count);
                    switch (random.Next(5))
                    {
                        case 0:
                            content[at] = (byte)random.Next(256);
                            break;
                        case 1:
                            content[at] ^= (byte)(1 << random.Next(8));
                            break;
                        case 2:
                            content.Insert(at, (byte)random.Next(256));
                            break;
                        case 3:
                            content.RemoveAt(at);
                            break;
                        default:
                            content.RemoveRange(at, content.Count - at);
                            break;
                    }
                }

                File.WriteAllBytes(log, content.ToArray());
                foreach (var other in Directory.GetFiles(directory).Where(f => f != log && !f.EndsWith(".pvl")))
                    File.Delete(other);
                try
                {
                    using (var reopened = new BTreeKeyValueDB(Options(directory)))
                    {
                        using (var read = reopened.StartReadOnlyTransaction())
                        using (var check = read.CreateCursor())
                        {
                            var count = 0;
                            if (check.FindFirstKey([]))
                                do
                                {
                                    var buffer = Span<byte>.Empty;
                                    check.GetValueSpan(ref buffer);
                                    count++;
                                } while (check.FindNextKey([]));
                            Assert.Equal(read.GetKeyValueCount(), count);
                        }

                        using var tr = await reopened.StartWritingTransaction();
                        using var cursor = tr.CreateCursor();
                        cursor.CreateOrUpdateKeyValue([200], [1]);
                        tr.Commit();
                    }

                    using (var again = new BTreeKeyValueDB(Options(directory)))
                    using (var read = again.StartReadOnlyTransaction())
                    using (var check = read.CreateCursor())
                        Assert.True(check.FindExactKey([200]));
                }
                catch (BTDBException error) when (error.Message == "Compression not supported")
                {
                    // A flag for a compressed key or value, like a log written with compression and opened without:
                    // refusing to open is right, as replay cannot tell this from a configuration mismatch.
                }
                catch (Exception error)
                {
                    Assert.Fail($"Seed {seed} round {round}: {error.GetType().Name}: {error.Message}\n{error.StackTrace}");
                }

                foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
                File.WriteAllBytes(log, original);
            }
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }
}
