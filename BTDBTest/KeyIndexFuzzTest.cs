using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

/// A key index with random bytes changed, inserted, removed or cut off is refused, or the database opens with its keys
/// in order, so that every key it lists is found. With its checksum the damaged index is not used at all, so the
/// database opens with exactly the data committed, replayed from the transaction logs.
public class KeyIndexFuzzTest
{
    static KeyValueDBOptions Options(string directory, bool compressed = false) => new()
    {
        FileCollection = new OnDiskFileCollection(directory),
        Compression = new NoCompressionStrategy(),
        KviCompressionStrategy = new DefaultCompressionKviStrategy(compressed ? 5 : -1, 22, 0),
        CompactorScheduler = null
    };

    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 60));

    // Before the fix keys were loaded as they came, so damage putting one out of order built a tree whose lookups
    // missed keys it listed.
    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedKeyIndexOpensOnlyWithKeysInOrder(int seed)
    {
        var random = new Random(seed);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-kvi-fuzz-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var db = new BTreeKeyValueDB(Options(directory)))
            {
                for (var t = 0; t < 20; t++)
                {
                    using var tr = db.StartWritingTransaction().AsTask().GetAwaiter().GetResult();
                    using var cursor = tr.CreateCursor();
                    for (var op = random.Next(1, 6); op > 0; op--)
                    {
                        var key = new byte[random.Next(1, 6)];
                        random.NextBytes(key);
                        var value = new byte[random.Next(3) == 0 ? 200 : random.Next(8)];
                        random.NextBytes(value);
                        cursor.CreateOrUpdateKeyValue(key, value);
                    }

                    tr.SetCommitUlong((ulong)t);
                    tr.Commit();
                }

                db.CreateKvi(CancellationToken.None);
            }

            var index = Directory.GetFiles(directory, "*.kvi").Single();
            var original = File.ReadAllBytes(index);
            for (var round = 0; round < 20; round++)
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

                var files = Directory.GetFiles(directory).ToDictionary(f => f, File.ReadAllBytes);
                File.WriteAllBytes(index, content.ToArray());
                try
                {
                    using var reopened = new BTreeKeyValueDB(Options(directory));
                    using var read = reopened.StartReadOnlyTransaction();
                    using var cursor = read.CreateCursor();
                    var keys = new List<byte[]>();
                    if (cursor.FindFirstKey([]))
                        do keys.Add(cursor.SlowGetKey()); while (cursor.FindNextKey([]));
                    for (var i = 1; i < keys.Count; i++)
                        Assert.True(keys[i - 1].AsSpan().SequenceCompareTo(keys[i]) < 0,
                            $"Seed {seed} round {round}: key {i} is out of order");
                    foreach (var key in keys)
                        Assert.True(cursor.FindExactKey(key), $"Seed {seed} round {round}: a listed key is not found");
                }
                catch (Exception error) when (error is BTDBException or InvalidDataException or EndOfStreamException)
                {
                }

                foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
                foreach (var (path, bytes) in files) File.WriteAllBytes(path, bytes);
                File.WriteAllBytes(index, original);
            }
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    // Before the checksum, damage keeping the keys in order, as in a key's last byte or a value's position, loaded as
    // other keys and values.
    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedKeyIndexNeverLoadsOtherContent(int seed) => NeverLoadsOtherContent(seed, false);

    /// Compressed, damage hits the Brotli stream instead.
    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedCompressedKeyIndexNeverLoadsOtherContent(int seed) => NeverLoadsOtherContent(seed, true);

    static void NeverLoadsOtherContent(int seed, bool compressed)
    {
        var random = new Random(seed);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-kvi-sum-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
            using (var db = new BTreeKeyValueDB(Options(directory, compressed)))
            {
                for (var t = 0; t < 20; t++)
                {
                    using var tr = db.StartWritingTransaction().AsTask().GetAwaiter().GetResult();
                    using var cursor = tr.CreateCursor();
                    for (var op = random.Next(1, 6); op > 0; op--)
                    {
                        var key = new byte[random.Next(1, 6)];
                        random.NextBytes(key);
                        var value = new byte[random.Next(3) == 0 ? 200 : random.Next(8)];
                        random.NextBytes(value);
                        cursor.CreateOrUpdateKeyValue(key, value);
                        expected[Convert.ToHexString(key)] = Convert.ToHexString(value);
                    }

                    tr.SetCommitUlong((ulong)t);
                    tr.Commit();
                }

                db.CreateKvi(CancellationToken.None);
            }

            var index = Directory.GetFiles(directory, "*.kvi").Single();
            var original = File.ReadAllBytes(index);
            for (var round = 0; round < 40; round++)
            {
                var content = (byte[])original.Clone();
                // Changed bytes keep the length, so only the checksum can tell the content changed.
                for (var changes = random.Next(1, 3); changes > 0; changes--)
                {
                    var at = random.Next(content.Length);
                    if (random.Next(2) == 0) content[at] = (byte)random.Next(256);
                    else content[at] ^= (byte)(1 << random.Next(8));
                }

                var files = Directory.GetFiles(directory).ToDictionary(f => f, File.ReadAllBytes);
                File.WriteAllBytes(index, content);
                try
                {
                    using var reopened = new BTreeKeyValueDB(Options(directory, compressed));
                    using var read = reopened.StartReadOnlyTransaction();
                    using var cursor = read.CreateCursor();
                    var actual = new SortedDictionary<string, string>(StringComparer.Ordinal);
                    if (cursor.FindFirstKey([]))
                        do actual[Convert.ToHexString(cursor.SlowGetKey())] = Convert.ToHexString(cursor.SlowGetValue());
                        while (cursor.FindNextKey([]));
                    Assert.True(expected.SequenceEqual(actual), $"Seed {seed} round {round}: other content loaded");
                }
                catch (Exception error) when (error is BTDBException or InvalidDataException or EndOfStreamException)
                {
                }

                foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
                foreach (var (path, bytes) in files) File.WriteAllBytes(path, bytes);
                File.WriteAllBytes(index, original);
            }
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }
}
