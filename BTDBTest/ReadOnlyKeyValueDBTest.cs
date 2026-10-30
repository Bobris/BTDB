using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.ReadOnly;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ReadOnlyKeyValueDBTest
{
    static BTreeKeyValueDB NewOracle() => new(new KeyValueDBOptions
    {
        FileCollection = new InMemoryFileCollection(), Compression = new NoCompressionStrategy(),
        CompactorScheduler = null
    });

    static ReadOnlyKeyValueDB CreateReadOnly(IKeyValueDB source, uint maxChildren = 31)
    {
        using var tr = source.StartReadOnlyTransaction();
        var creator = new ReadOnlyCreator(tr);
        creator.SetMaxChildren(maxChildren);
        return ReadOnlyKeyValueDB.Create(creator.WriteToArray());
    }

    static List<byte[]> RandomKeys(Random random, int count, int maxLength)
    {
        // Small alphabet makes many shared prefixes, keys which are prefixes of other keys and an empty key
        byte[] alphabet = [0, 1, 2, 128, 255];
        var keys = new HashSet<string>();
        var result = new List<byte[]>();
        while (result.Count < count)
        {
            var key = new byte[random.Next(maxLength + 1)];
            for (var i = 0; i < key.Length; i++) key[i] = alphabet[random.Next(alphabet.Length)];
            if (keys.Add(Convert.ToHexString(key))) result.Add(key);
        }

        return result;
    }

    static void Fill(IKeyValueDB db, IEnumerable<byte[]> keys, Random random, int maxValueLength = 40)
    {
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        foreach (var key in keys)
        {
            var value = new byte[random.Next(maxValueLength + 1)];
            random.NextBytes(value);
            cursor.CreateOrUpdateKeyValue(key, value);
        }

        tr.SetCommitUlong(42);
        tr.SetUlong(1, 7);
        tr.Commit();
    }

    [Theory]
    [InlineData(1, 0, 4, 31)]
    [InlineData(2, 1, 4, 31)]
    [InlineData(3, 2, 4, 2)]
    [InlineData(4, 300, 6, 2)]
    [InlineData(5, 300, 6, 3)]
    [InlineData(6, 2000, 8, 31)]
    [InlineData(7, 5000, 10, 127)]
    [InlineData(8, 3000, 12, 5)]
    public void CursorBehavesLikeBTreeKeyValueDB(int seed, int keyCount, int maxKeyLength, uint maxChildren)
    {
        var random = new Random(seed);
        using var oracle = NewOracle();
        var keys = RandomKeys(random, keyCount, maxKeyLength);
        Fill(oracle, keys, random);
        using var db = CreateReadOnly(oracle, maxChildren);
        using var oracleTr = oracle.StartReadOnlyTransaction();
        using var tr = db.StartReadOnlyTransaction();
        Assert.Equal(oracleTr.GetKeyValueCount(), tr.GetKeyValueCount());
        Assert.Equal(42ul, tr.GetCommitUlong());
        Assert.Equal(oracleTr.GetUlongCount(), tr.GetUlongCount());
        Assert.Equal(7ul, tr.GetUlong(1));

        using var expected = oracleTr.CreateCursor();
        using var actual = tr.CreateCursor();
        var allKeys = keys.OrderBy(k => k, ByteArrayComparer.Instance).ToList();
        for (var op = 0; op < 3000; op++)
        {
            var step = $"op {op}";
            var prefix = RandomPrefix(random, allKeys, maxKeyLength);
            switch (random.Next(10))
            {
                case 0:
                    Same(step + " FindFirstKey", expected.FindFirstKey(prefix), actual.FindFirstKey(prefix));
                    break;
                case 1:
                    Same(step + " FindLastKey", expected.FindLastKey(prefix), actual.FindLastKey(prefix));
                    break;
                case 2:
                    Same(step + " FindNextKey", expected.FindNextKey(prefix), actual.FindNextKey(prefix));
                    break;
                case 3:
                    Same(step + " FindPreviousKey", expected.FindPreviousKey(prefix), actual.FindPreviousKey(prefix));
                    break;
                case 4:
                {
                    var prefixLen = (uint)random.Next(prefix.Length + 1);
                    Same(step + " Find", expected.Find(prefix, prefixLen), actual.Find(prefix, prefixLen));
                    break;
                }
                case 5:
                {
                    var index = random.NextInt64(-2, keyCount + 3);
                    Same(step + " FindKeyIndex", expected.FindKeyIndex(index), actual.FindKeyIndex(index));
                    break;
                }
                case 6:
                {
                    var index = random.NextInt64(-2, 8);
                    Same(step + " FindKeyIndex prefix", expected.FindKeyIndex(prefix, index),
                        actual.FindKeyIndex(prefix, index));
                    break;
                }
                case 7:
                    Same(step + " KeyHasPrefix", expected.KeyHasPrefix(prefix), actual.KeyHasPrefix(prefix));
                    break;
                case 8:
                    expected.Invalidate();
                    actual.Invalidate();
                    break;
                default:
                {
                    var stopAfter = random.Next(5);
                    var expectedKeys = Iterate(expected, stopAfter);
                    var actualKeys = Iterate(actual, stopAfter);
                    Same(step + " FastIterate", expectedKeys, actualKeys);
                    break;
                }
            }

            CompareCursors(step, expected, actual);
        }
    }

    static byte[] RandomPrefix(Random random, List<byte[]> keys, int maxKeyLength)
    {
        if (keys.Count > 0 && random.Next(3) > 0)
        {
            var key = keys[random.Next(keys.Count)];
            var prefix = key.AsSpan(0, random.Next(key.Length + 1)).ToArray();
            // Sometimes a key just after an existing one
            return random.Next(4) == 0 ? [.. prefix, 1] : prefix;
        }

        return RandomKeys(random, 1, maxKeyLength)[0];
    }

    static string Iterate(IKeyValueDBCursor cursor, int stopAfter)
    {
        var seen = new List<string>();
        var buffer = Span<byte>.Empty;
        cursor.FastIterate(ref buffer, (index, key) =>
        {
            seen.Add(index + ":" + Convert.ToHexString(key));
            return seen.Count > stopAfter;
        });
        return string.Join(",", seen);
    }

    static void CompareCursors(string step, IKeyValueDBCursor expected, IKeyValueDBCursor actual)
    {
        Same(step + " IsValid", expected.IsValid(), actual.IsValid());
        // BTreeKeyValueDB keeps the key count as index of a cursor invalidated by finishing FastIterate
        if (!expected.IsValid())
        {
            Same(step + " GetKeyIndex", -1L, actual.GetKeyIndex());
            return;
        }

        Same(step + " GetKeyIndex", expected.GetKeyIndex(), actual.GetKeyIndex());
        Same(step + " key", Convert.ToHexString(expected.SlowGetKey()), Convert.ToHexString(actual.SlowGetKey()));
        Same(step + " value", Convert.ToHexString(expected.SlowGetValue()),
            Convert.ToHexString(actual.SlowGetValue()));
        Memory<byte> buffer = default;
        Same(step + " value memory", Convert.ToHexString(expected.GetValueMemory(ref buffer, true).Span),
            Convert.ToHexString(actual.GetValueMemory(ref buffer).Span));
        Same(step + " key memory", Convert.ToHexString(expected.GetKeyMemory(ref buffer, true).Span),
            Convert.ToHexString(actual.GetKeyMemory(ref buffer).Span));
        Same(step + " storage key size", expected.GetStorageSizeOfCurrentKey().Key,
            actual.GetStorageSizeOfCurrentKey().Key);
        Same(step + " storage value size", expected.GetStorageSizeOfCurrentKey().Value,
            actual.GetStorageSizeOfCurrentKey().Value);
    }

    static void Same<T>(string step, T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            Assert.Fail($"{step}: expected {expected}, actual {actual}");
    }

    sealed class ByteArrayComparer : IComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();
        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }

    [Fact]
    public void EmptyDatabaseHasNoKeys()
    {
        using var oracle = NewOracle();
        using var db = CreateReadOnly(oracle);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.Equal(0, tr.GetKeyValueCount());
        Assert.False(cursor.FindFirstKey([]));
        Assert.False(cursor.FindLastKey([]));
        Assert.Equal(FindResult.NotFound, cursor.Find([1], 0));
        Assert.False(cursor.FindKeyIndex(0));
        Assert.Empty(tr.CalcBTreeStats());
    }

    [Fact]
    public void LongKeysSplitNodesBySize()
    {
        var random = new Random(1);
        using var oracle = NewOracle();
        var keys = Enumerable.Range(0, 200).Select(i =>
        {
            var key = new byte[random.Next(1, 20000)];
            random.NextBytes(key);
            key[0] = (byte)i;
            return key;
        }).Append(new byte[ushort.MaxValue]).ToList();
        Fill(oracle, keys, random, 100000);
        using var db = CreateReadOnly(oracle, 127);
        using var oracleTr = oracle.StartReadOnlyTransaction();
        using var tr = db.StartReadOnlyTransaction();
        using var expected = oracleTr.CreateCursor();
        using var actual = tr.CreateCursor();
        Assert.True(actual.FindFirstKey([]));
        Assert.True(expected.FindFirstKey([]));
        do
        {
            CompareCursors("iterate", expected, actual);
            Same("next", expected.FindNextKey([]), actual.FindNextKey([]));
        } while (expected.IsValid());

        foreach (var key in keys)
        {
            Assert.Equal(FindResult.Exact, actual.Find(key, 0));
            Assert.Equal(key, actual.SlowGetKey());
        }
    }

    [Fact]
    public void KeyLongerThan64KBCannotBeWritten()
    {
        var random = new Random(1);
        using var oracle = NewOracle();
        Fill(oracle, [new byte[ushort.MaxValue + 1]], random);
        using var tr = oracle.StartReadOnlyTransaction();
        Assert.Throws<BTDBException>(() => new ReadOnlyCreator(tr).WriteToArray());
    }

    [Fact]
    public async Task ModificationsAreRejected()
    {
        using var oracle = NewOracle();
        Fill(oracle, [[1], [2]], new Random(1));
        using var db = CreateReadOnly(oracle);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([]));
        Assert.Throws<BTDBException>(() => cursor.CreateOrUpdateKeyValue([3], [3]));
        Assert.Throws<BTDBException>(() => cursor.SetValue([3]));
        Assert.Throws<BTDBException>(() => cursor.EraseCurrent());
        Assert.Throws<BTDBException>(() => tr.SetCommitUlong(1));
        tr.SetCommitUlong(42);
        await Assert.ThrowsAsync<BTDBException>(async () => await db.StartWritingTransaction());
        Assert.True(tr.IsReadOnly());
    }

    [Fact]
    public void ValueMemoryReturnedWithoutCopyCanBePinned()
    {
        using var oracle = NewOracle();
        using (var writeTr = oracle.StartTransaction())
        {
            using var writeCursor = writeTr.CreateCursor();
            writeCursor.CreateOrUpdateKeyValue([1], [1, 2, 3]);
            writeTr.Commit();
        }

        using var db = CreateReadOnly(oracle);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([]));
        Memory<byte> buffer = default;
        var value = cursor.GetValueMemory(ref buffer);
        using (value.Pin())
        {
        }

        using (value[1..].Pin())
        {
        }

        Assert.Equal(new byte[] { 1, 2, 3 }, value.ToArray());
    }

    static byte[] CreateImage(int seed, int keyCount = 300)
    {
        var random = new Random(seed);
        using var oracle = NewOracle();
        Fill(oracle, RandomKeys(random, keyCount, 8), random);
        using var tr = oracle.StartReadOnlyTransaction();
        return new ReadOnlyCreator(tr).WriteToArray();
    }

    // Without the checksum a changed key or value byte reads as other content; checking reads all data first.
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void ChecksumFindsEveryChangedByte(int seed)
    {
        var image = CreateImage(seed);
        using (var intact = ReadOnlyKeyValueDB.Create(image, verifyChecksum: true))
            Assert.True(intact.HasChecksum);
        var random = new Random(seed);
        for (var round = 0; round < 200; round++)
        {
            var damaged = (byte[])image.Clone();
            damaged[random.Next(damaged.Length)] ^= (byte)(1 << random.Next(8));
            Assert.Throws<InvalidDataException>(() =>
            {
                using var db = ReadOnlyKeyValueDB.Create(damaged, verifyChecksum: true);
            });
        }
    }

    // Opening without the check reads only the header and footer, so an image is not read whole when mapped.
    [Fact]
    public void DamagedValueOpensWithoutCheckAndFailsVerifyChecksum()
    {
        var image = CreateImage(4);
        image[image.Length - 20] ^= 1;
        using var db = ReadOnlyKeyValueDB.Create(image);
        Assert.Throws<InvalidDataException>(db.VerifyChecksum);
    }

    // Images written before version 2 have no checksum; they open as before and cannot be checked.
    [Fact]
    public void ImageWithoutChecksumStillOpens()
    {
        var random = new Random(5);
        using var oracle = NewOracle();
        var keys = RandomKeys(random, 300, 8);
        Fill(oracle, keys, random);
        byte[] image;
        using (var tr = oracle.StartReadOnlyTransaction())
            image = new ReadOnlyCreator(tr).WriteToArray();
        var old = image[..^8];
        Assert.Equal(2, old[6] << 8 | old[7]);
        old[7] = 1;
        using var db = ReadOnlyKeyValueDB.Create(old);
        Assert.False(db.HasChecksum);
        Assert.Throws<InvalidDataException>(db.VerifyChecksum);
        using var readTr = db.StartTransaction();
        using var cursor = readTr.CreateCursor();
        using var oracleTr = oracle.StartReadOnlyTransaction();
        using var expected = oracleTr.CreateCursor();
        Assert.Equal(42ul, readTr.GetCommitUlong());
        foreach (var key in keys)
        {
            Assert.Equal(FindResult.Exact, cursor.Find(key, 0));
            Assert.Equal(FindResult.Exact, expected.Find(key, 0));
            CompareCursors("old format", expected, cursor);
        }
    }

    [Fact]
    public void DatabaseCanBeMemoryMappedFromFile()
    {
        var fileName = Path.Combine(Path.GetTempPath(), "BTDB-ReadOnly-" + Guid.NewGuid() + ".robtdb");
        try
        {
            var random = new Random(3);
            using var oracle = NewOracle();
            var keys = RandomKeys(random, 1000, 8);
            Fill(oracle, keys, random);
            using (var tr = oracle.StartReadOnlyTransaction())
                new ReadOnlyCreator(tr).WriteToFile(fileName);
            using var db = ReadOnlyKeyValueDB.OpenFile(fileName, verifyChecksum: true);
            Assert.True(db.HasChecksum);
            using var readTr = db.StartTransaction();
            using var cursor = readTr.CreateCursor();
            using var oracleTr = oracle.StartReadOnlyTransaction();
            using var expected = oracleTr.CreateCursor();
            foreach (var key in keys)
            {
                Assert.Equal(FindResult.Exact, cursor.Find(key, 0));
                Assert.Equal(FindResult.Exact, expected.Find(key, 0));
                CompareCursors("file", expected, cursor);
            }
        }
        finally
        {
            File.Delete(fileName);
        }
    }

    [Fact]
    public void CorruptedDataAreRejected()
    {
        using var oracle = NewOracle();
        Fill(oracle, RandomKeys(new Random(1), 100, 6), new Random(1));
        using var tr = oracle.StartReadOnlyTransaction();
        var data = new ReadOnlyCreator(tr).WriteToArray();
        Assert.Throws<InvalidDataException>(() => ReadOnlyKeyValueDB.Create(data.AsMemory(0, data.Length - 1)));
        Assert.Throws<InvalidDataException>(() => ReadOnlyKeyValueDB.Create(data.AsMemory(1)));
        Assert.Throws<InvalidDataException>(() => ReadOnlyKeyValueDB.Create(new byte[3]));
    }

    public class Person
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        void Insert(Person person);
        Person? FindById(ulong id);
        IEnumerable<Person> ListByName(AdvancedEnumeratorParam<string> param);
    }

    [Fact]
    public void ObjectDBCanReadReadOnlyDatabase()
    {
        using var oracle = NewOracle();
        using (var odb = new ObjectDB())
        {
            odb.Open(oracle, false);
            using var tr = odb.StartTransaction();
            var table = tr.GetRelation<IPersonTable>();
            for (var i = 1ul; i <= 100; i++)
                table.Insert(new() { Id = i, Name = "Name" + (100 - i), Age = (int)i });
            tr.Commit();
        }

        using var db = CreateReadOnly(oracle);
        using var readOnlyOdb = new ObjectDB();
        readOnlyOdb.Open(db, false);
        using var readTr = readOnlyOdb.StartReadOnlyTransaction();
        var readTable = readTr.GetRelation<IPersonTable>();
        Assert.Equal(100, readTable.Count);
        Assert.Equal(42, readTable.FindById(42)!.Age);
        Assert.Equal("Name0", readTable.ListByName(new(EnumerationOrder.Ascending)).First().Name);
        Assert.Throws<BTDBException>(() => readTable.Insert(new() { Id = 1000, Name = "X" }));
    }
}
