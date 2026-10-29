using System;
using System.IO;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.ReadOnly;
using Xunit;

namespace BTDBTest;

/// A read-only database image with random bytes changed or cut off opens and reads, perhaps returning wrong keys or
/// values as nothing checks them, or fails with InvalidDataException; nothing else escapes and no read leaves the data.
public class ReadOnlyKeyValueDBFuzzTest
{
    static byte[] Image(Random random)
    {
        using var source = new BTreeKeyValueDB(new InMemoryFileCollection());
        using (var tr = source.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = random.Next(1, 200); i > 0; i--)
            {
                var key = new byte[random.Next(1, 12)];
                random.NextBytes(key);
                key[0] = (byte)random.Next(4);
                cursor.CreateOrUpdateKeyValue(key, new byte[random.Next(3) == 0 ? random.Next(100, 2000) : random.Next(6)]);
            }

            tr.SetUlong(0, 7);
            tr.SetCommitUlong(9);
            tr.Commit();
        }

        using var read = source.StartReadOnlyTransaction();
        var creator = new ReadOnlyCreator(read);
        creator.SetMaxChildren((uint)random.Next(2, 9));
        return creator.WriteToArray();
    }

    static void ReadEverything(byte[] bytes)
    {
        using var db = ReadOnlyKeyValueDB.Create(bytes);
        using var tr = db.StartReadOnlyTransaction();
        tr.GetUlong(0);
        tr.GetCommitUlong();
        tr.CalcBTreeStats();
        using var cursor = tr.CreateCursor();
        var count = 0L;
        if (cursor.FindFirstKey([]))
            do
            {
                var key = Span<byte>.Empty;
                var value = Span<byte>.Empty;
                cursor.GetKeySpan(ref key);
                cursor.GetValueSpan(ref value);
                cursor.GetKeyIndex();
                if (++count > 100_000) throw new InvalidOperationException("Iteration does not end");
            } while (cursor.FindNextKey([]));
        tr.GetKeyValueCount();
        for (byte b = 0; b < 4; b++)
        {
            cursor.Find([b, 0x80], 0);
            cursor.FindLastKey([b]);
            cursor.FindFirstKey([b]);
        }
    }

    // Seed 3025 damages a child pointer to point back up the tree: before the fix descending through it never ended.
    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 60).Append(3025));

    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedImageReadsOrFailsWithInvalidData(int seed)
    {
        var random = new Random(seed);
        var original = Image(random);
        ReadEverything(original);
        for (var round = 0; round < 100; round++)
        {
            var bytes = (byte[])original.Clone();
            if (random.Next(5) == 0) bytes = bytes[..random.Next(bytes.Length)];
            else
                for (var changes = random.Next(1, 4); changes > 0; changes--)
                    bytes[random.Next(bytes.Length)] = (byte)random.Next(256);
            try
            {
                ReadEverything(bytes);
            }
            catch (InvalidDataException)
            {
            }
            catch (Exception error)
            {
                Assert.Fail($"Seed {seed} round {round}: {error.GetType().Name}: {error.Message}\n{error.StackTrace}");
            }
        }
    }
}
