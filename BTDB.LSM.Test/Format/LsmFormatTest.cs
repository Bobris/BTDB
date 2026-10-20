using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Format;

/// Format stability and corruption detection (ImplementationPlan.md, section 19.3). A deterministic sample database
/// exercises every object kind: tables on several levels with partitioned indexes and filters, range tombstones, value
/// files, manifests and WAL chunks holding commits that are not in tables yet.
public class LsmFormatTest(ITestOutputHelper output)
{
    static LsmKeyValueDBOptions Options(ILsmStorage storage, bool writer) => new()
    {
        Storage = storage, AcquireWriterLeaseOnOpen = writer, BackgroundCompaction = false, MemtableSize = 4096,
        BlockSize = 256, RestartInterval = 4, IndexPartitionSize = 128, Level0CompactionTrigger = 2,
        LevelBaseBytes = 2048, LevelMultiplier = 2, LevelCount = 4, TargetFileSize = 1024, PointDeleteRangeLimit = 2,
        ValueSeparationThreshold = 64, WalChunkTargetSize = 1024, DeletionGracePeriod = TimeSpan.Zero,
        BlockCacheSize = 1 << 20, TimeProvider = FixedTime.Instance
    };

    /// Manifests record when objects became obsolete, so the sample needs a fixed clock to be deterministic.
    sealed class FixedTime : TimeProvider
    {
        public static readonly FixedTime Instance = new();

        public override DateTimeOffset GetUtcNow() => new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    }

    /// Writes the sample and returns its content as seen by the writer; the writer then crashes, so the last commits
    /// stay only in the WAL.
    static async Task<SortedDictionary<string, string>> WriteSample(InMemoryLsmStorage storage)
    {
        var db = await LsmKeyValueDB.OpenAsync(Options(storage, true));
        var random = new Random(7);
        for (var commit = 1; commit <= 160; commit++)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            for (var op = 0; op < 10; op++)
            {
                if (random.Next(8) == 0 && tr.GetKeyValueCount() > 20)
                {
                    using var to = tr.CreateCursor();
                    var from = random.NextInt64(tr.GetKeyValueCount() - 10);
                    Assert.True(cursor.FindKeyIndex(from));
                    Assert.True(to.FindKeyIndex(from + random.Next(1, 10)));
                    cursor.EraseUpTo(to);
                    continue;
                }

                var value = new byte[random.Next(3) == 0 ? random.Next(64, 200) : random.Next(12)];
                random.NextBytes(value);
                cursor.CreateOrUpdateKeyValue([(byte)random.Next(20), (byte)random.Next(100)], value);
            }

            tr.SetCommitUlong((ulong)commit);
            tr.SetUlong(1, (ulong)commit * 3);
            tr.Commit();
        }

        // A few small commits after the last flush stay only in the WAL.
        for (var commit = 161; commit <= 164; commit++)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([30, (byte)commit], [(byte)commit]);
            tr.SetCommitUlong((ulong)commit);
            tr.Commit();
        }

        await db.FlushAsync();
        var stats = db.CalcStats();
        Assert.Contains("Level0:", stats);
        Assert.Contains("Level2:", stats);
        var content = Content(db);
        db.SimulateCrash();
        return content;
    }

    static SortedDictionary<string, string> Content(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["commitUlong"] = tr.GetCommitUlong().ToString(), ["ulong1"] = tr.GetUlong(1).ToString(),
            ["count"] = tr.GetKeyValueCount().ToString()
        };
        using var cursor = tr.CreateCursor();
        var keys = new List<byte[]>();
        if (cursor.FindFirstKey([]))
            do
            {
                var key = cursor.SlowGetKey();
                keys.Add(key);
                result.Add(Convert.ToHexString(key), Convert.ToHexString(cursor.SlowGetValue()));
            } while (cursor.FindNextKey([]));

        // Exact lookups read the bloom filter partitions as well.
        using var probe = tr.CreateCursor();
        foreach (var key in keys) Assert.True(probe.FindExactKey(key));
        for (var i = 0; i < 100; i++) Assert.False(probe.FindExactKey([200, (byte)i]));
        return result;
    }

    static InMemoryLsmStorage Copy(InMemoryLsmStorage source)
    {
        var copy = new InMemoryLsmStorage();
        foreach (var key in source.Keys(""))
            if (key != "LOCK")
                copy.Overwrite(key, source.Peek(key)!);
        return copy;
    }

    static async Task<SortedDictionary<string, string>> ReadAsNewInstance(InMemoryLsmStorage storage)
    {
        using var db = await LsmKeyValueDB.OpenAsync(Options(storage, false));
        return Content(db);
    }

    static string GoldenDirectory([CallerFilePath] string path = "") =>
        Path.Combine(Path.GetDirectoryName(path)!, "Golden");

    [Fact]
    public async Task SampleCoversEveryObjectKind()
    {
        var storage = new InMemoryLsmStorage();
        await WriteSample(storage);
        foreach (var prefix in new[] { "tables/", "values/", "manifest/", "wal/" })
            Assert.NotEmpty(storage.Keys(prefix));
        output.WriteLine(string.Join(Environment.NewLine,
            storage.Keys("").Select(k => $"{k} {storage.Peek(k)!.Length}")));
    }

    /// Fails when the bytes written for the same operations change. After an intentional format change, bump the
    /// format version of the changed object and regenerate with BTDB_LSM_UPDATE_GOLDEN=1.
    [Fact]
    public async Task WrittenObjectsMatchGoldenFiles()
    {
        var storage = new InMemoryLsmStorage();
        await WriteSample(storage);
        var directory = GoldenDirectory();
        var objects = storage.Keys("").Where(k => k != "LOCK").ToArray();
        if (Environment.GetEnvironmentVariable("BTDB_LSM_UPDATE_GOLDEN") == "1")
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
            foreach (var key in objects)
            {
                var path = Path.Combine(directory, key);
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                await File.WriteAllBytesAsync(path, storage.Peek(key)!);
            }
        }

        var golden = Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(directory, f).Replace(Path.DirectorySeparatorChar, '/'))
            .Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(golden, objects.Order(StringComparer.Ordinal).ToArray());
        foreach (var key in objects)
            Assert.True((await File.ReadAllBytesAsync(Path.Combine(directory, key))).AsSpan()
                .SequenceEqual(storage.Peek(key)), $"{key} differs from its golden file");
    }

    [Fact]
    public async Task GoldenFilesAreReadable()
    {
        var expected = await WriteSample(new InMemoryLsmStorage());
        var storage = new InMemoryLsmStorage();
        var directory = GoldenDirectory();
        foreach (var file in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories))
            storage.Overwrite(Path.GetRelativePath(directory, file).Replace(Path.DirectorySeparatorChar, '/'),
                await File.ReadAllBytesAsync(file));
        Assert.Equal(expected, await ReadAsNewInstance(storage));
    }

    /// Every damaged object is either reported with BTDBException or not needed at all; the database never returns
    /// wrong content. Tables, WAL chunks and the manifest are always fully read, so their damage is always reported;
    /// value files may hold dead values that no key references.
    [Theory]
    [InlineData("flip")]
    [InlineData("truncate")]
    public async Task DamagedObjectsAreReported(string damage)
    {
        var original = new InMemoryLsmStorage();
        var expected = await WriteSample(original);
        var random = new Random(11);
        var cases = 0;
        var reported = 0;
        foreach (var key in original.Keys("").Where(k => k != "LOCK"))
        {
            var content = original.Peek(key)!;
            var positions = damage == "flip"
                ? new[] { 0, content.Length / 2, content.Length - 1, random.Next(content.Length), random.Next(content.Length) }
                : new[] { 0, 1, content.Length / 2, content.Length - 8, content.Length - 1 };
            foreach (var position in positions.Where(p => p >= 0 && p < content.Length).Distinct())
            {
                var storage = Copy(original);
                var damaged = damage == "flip" ? content.ToArray() : content[..position];
                if (damage == "flip") damaged[position] ^= (byte)(1 << random.Next(8));
                storage.Overwrite(key, damaged);
                cases++;
                SortedDictionary<string, string>? actual = null;
                try
                {
                    actual = await ReadAsNewInstance(storage);
                }
                catch (BTDBException)
                {
                    reported++;
                    continue;
                }

                Assert.True(key.StartsWith("values/"),
                    $"{damage} of {key} at {position} of {content.Length} was not reported");
                Assert.Equal(expected, actual);
            }
        }

        output.WriteLine($"{damage}: {reported} of {cases} damaged objects reported");
        Assert.True(reported > cases / 2);
    }
}
