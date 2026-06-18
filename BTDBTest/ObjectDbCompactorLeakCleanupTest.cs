using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbCompactorLeakCleanupTest : IDisposable
{
    readonly InMemoryFileCollection _fileCollection = new();
    readonly BTreeKeyValueDB _lowDb;
    readonly TestLogger _logger = new();
    readonly ObjectDB _db;

    public ObjectDbCompactorLeakCleanupTest()
    {
        _lowDb = new(new KeyValueDBOptions
        {
            CompactorScheduler = null,
            Compression = new NoCompressionStrategy(),
            FileCollection = _fileCollection
        });
        _db = new();
        _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration().WithLogger(_logger)
            .WithCompactorLeakDetectorMode(CompactorLeakDetectorMode.Erase));
    }

    [Fact]
    public async Task CompactorRemovesLeaksOnFifthAndThenEveryTwentiethRun()
    {
        CreateDictionaryLeak("programming", "code", "debug");

        for (var i = 0; i < 4; i++)
        {
            await _lowDb.Compact(CancellationToken.None);
            Assert.NotEmpty(FindLeaks());
        }

        await _lowDb.Compact(CancellationToken.None);
        Assert.Empty(FindLeaks());

        CreateDictionaryLeak("chess", "mate", "fork");
        for (var i = 0; i < 19; i++)
        {
            await _lowDb.Compact(CancellationToken.None);
            Assert.NotEmpty(FindLeaks());
        }

        await _lowDb.Compact(CancellationToken.None);
        Assert.Empty(FindLeaks());
    }

    [Fact]
    public async Task CompactorLogsLeakedObjectTypesOnceAndRemovedKeyCount()
    {
        CreateObjectLeak("read", "write");
        CreateObjectLeak("run", "walk");
        CreateDictionaryLeak("programming", "code", "debug");
        CreateDictionaryLeak("chess", "mate", "fork");

        for (var i = 0; i < 5; i++)
            await _lowDb.Compact(CancellationToken.None);

        Assert.NotEmpty(_logger.RemovedLeaks);
        var last = _logger.RemovedLeaks[^1];
        Assert.True(last.RemovedKeyCount > 0);
        Assert.Equal(last.TypeNames.Count, last.TypeNames.Distinct().Count());
        Assert.Contains(nameof(Job), last.TypeNames);
        Assert.True(last.Elapsed >= TimeSpan.Zero);
    }

    [Fact]
    public async Task CompactorRememberedLeakKeysAreCappedButAtLeastOneIsRemoved()
    {
        ((ObjectDB)_db).CompactorLeakCleanupMaxKeyBytes = 1;
        CreateDictionaryLeak("programming", "code", "debug");
        CreateDictionaryLeak("chess", "mate", "fork");
        var leaksBefore = FindLeaks().Count;
        Assert.True(leaksBefore > 1);

        for (var i = 0; i < 5; i++)
            await _lowDb.Compact(CancellationToken.None);

        var leaksAfter = FindLeaks().Count;
        Assert.True(leaksAfter < leaksBefore);
        Assert.True(leaksAfter > 0);
        Assert.Equal(1UL, _logger.RemovedLeaks[^1].RemovedKeyCount);
    }

    [Fact]
    public async Task CompactorRemovesLeakedRoaringBitmapContent()
    {
        Func<IObjectDBTransaction, IBitmapLinks> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IBitmapLinks>("BitmapLinksRelation");
            var links = creator(tr);
            var bitmap = CreateBitmap(tr, 1, 65536 + 7, 2 * 65536 + 42);
            links.Insert(new BitmapLink { Id = 1, Bits = bitmap });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.True(creator(tr).ShallowRemoveById(1));
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());

        for (var i = 0; i < 5; i++)
            await _lowDb.Compact(CancellationToken.None);

        Assert.Empty(FindLeaks());
    }

    [Fact]
    public async Task CompactorDetectsLeaksByDefaultButDoesNotRemoveThem()
    {
        using var context = new TestDbContext(new DBOptions().WithoutAutoRegistration());
        CreateDictionaryLeak("programming", "code", "debug", context.Db);
        var leaksBefore = FindLeaks(context.Db);
        Assert.NotEmpty(leaksBefore);

        for (var i = 0; i < 5; i++)
            await context.LowDb.Compact(CancellationToken.None);

        Assert.Equal(leaksBefore, FindLeaks(context.Db));
        Assert.NotEmpty(context.Logger.DetectedLeaks);
        Assert.Empty(context.Logger.RemovedLeaks);
        Assert.True(context.Logger.DetectedLeaks[^1].LeakedKeyCount > 0);
        Assert.True(context.Logger.DetectedLeaks[^1].Elapsed >= TimeSpan.Zero);
    }

    [Fact]
    public async Task CompactorLeakDetectorCanBeDisabled()
    {
        using var context = new TestDbContext(new DBOptions().WithoutAutoRegistration()
            .WithCompactorLeakDetectorMode(CompactorLeakDetectorMode.Off));
        CreateDictionaryLeak("programming", "code", "debug", context.Db);
        var leaksBefore = FindLeaks(context.Db);
        Assert.NotEmpty(leaksBefore);

        for (var i = 0; i < 5; i++)
            await context.LowDb.Compact(CancellationToken.None);

        Assert.Equal(leaksBefore, FindLeaks(context.Db));
        Assert.Empty(context.Logger.DetectedLeaks);
        Assert.Empty(context.Logger.RemovedLeaks);
    }

    void CreateDictionaryLeak(string key, string oldActivity, string newActivity, IObjectDB? db = null)
    {
        db ??= _db;
        using (var tr = db.StartTransaction())
        {
            var directory = tr.Singleton<Directory>();
            directory.Items[key] = new JobMap
            {
                Jobs = new Dictionary<ulong, Job>
                {
                    [0] = new() { Activity = oldActivity }
                }
            };
            tr.Commit();
        }

        var leakedRow = CaptureFirstDictionaryRow(db, FindJobsDictionaryId(db, key));
        using (var tr = db.StartTransaction())
        {
            var directory = tr.Singleton<Directory>();
            directory.Items[key] = new JobMap
            {
                Jobs = new Dictionary<ulong, Job>
                {
                    [0] = new() { Activity = newActivity }
                }
            };
            RestoreRow(tr, leakedRow);
            tr.Commit();
        }
    }

    static ulong FindJobsDictionaryId(IObjectDB db, string key)
    {
        using var tr = db.StartReadOnlyTransaction();
        var directory = tr.Singleton<Directory>();
        return ((IInternalODBDictionary)directory.Items[key].Jobs).DictId;
    }

    static (byte[] Key, byte[] Value) CaptureFirstDictionaryRow(IObjectDB db, ulong dictId)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        var prefix = DictionaryPrefix(dictId);
        Span<byte> keyBuffer = stackalloc byte[4096];
        Span<byte> valueBuffer = stackalloc byte[4096];
        if (!cursor.FindNextKey(prefix)) throw new InvalidOperationException("Dictionary row not found.");
        return (cursor.GetKeySpan(ref keyBuffer).ToArray(), cursor.GetValueSpan(ref valueBuffer).ToArray());
    }

    static void RestoreRow(IObjectDBTransaction tr, (byte[] Key, byte[] Value) row)
    {
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue(row.Key, row.Value);
    }

    static byte[] DictionaryPrefix(ulong dictId)
    {
        var len = PackUnpack.LengthVUInt(dictId);
        var prefix = new byte[ObjectDB.AllDictionariesPrefixLen + len];
        MemoryMarshal.GetReference(prefix.AsSpan()) = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(prefix.AsSpan()[ObjectDB.AllDictionariesPrefixLen..]),
            dictId, len);
        return prefix;
    }

    void CreateObjectLeak(string oldActivity, string newActivity, IObjectDB? db = null)
    {
        db ??= _db;
        using (var tr = db.StartTransaction())
        {
            var holder = tr.Singleton<IndirectJobHolder>();
            holder.Job.Value = new Job { Activity = oldActivity };
            tr.Store(holder);
            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            var holder = tr.Singleton<IndirectJobHolder>();
            holder.Job.Value = new Job { Activity = newActivity };
            tr.Store(holder);
            tr.Commit();
        }
    }

    static ODBRoaringBitmap CreateBitmap(IObjectDBTransaction tr, params ulong[] values)
    {
        var bitmap = new ODBRoaringBitmap((IInternalObjectDBTransaction)tr);
        foreach (var value in values)
            bitmap.Set(value, true);
        bitmap.Flush();
        return bitmap;
    }

    List<string> FindLeaks(IObjectDB? db = null)
    {
        db ??= _db;
        using var visitor = new FindUnusedKeysVisitor();
        using var tr = db.StartReadOnlyTransaction();
        visitor.ImportAllKeys(tr);
        visitor.Iterate(tr);
        return visitor.UnseenKeys().Select(unseen => Convert.ToHexString(unseen.Key)).ToList();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
        _fileCollection.Dispose();
    }

    [Generate]
    public class Directory
    {
        public IDictionary<string, JobMap> Items { get; set; }
    }

    [Generate]
    public class JobMap
    {
        public IDictionary<ulong, Job> Jobs { get; set; }
    }

    [Generate]
    public class Job
    {
        public string Activity { get; set; }
    }

    [Generate]
    public class IndirectJobHolder
    {
        public IIndirect<Job> Job { get; set; }
    }

    public class BitmapLink
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IRoaringBitmap Bits { get; set; }
    }

    public interface IBitmapLinks : IRelation<BitmapLink>
    {
        void Insert(BitmapLink link);
        bool ShallowRemoveById(ulong id);
    }

    sealed class TestLogger : IObjectDBLogger
    {
        public readonly List<RemovedLeaksLog> RemovedLeaks = [];
        public readonly List<DetectedLeaksLog> DetectedLeaks = [];

        public void ReportIncompatiblePrimaryKey(string relationName, string field)
        {
        }

        public void CompactorDetectedLeaks(IReadOnlyCollection<string> leakedObjectTypeNames, ulong leakedKeyCount,
            TimeSpan elapsed)
        {
            DetectedLeaks.Add(new(leakedObjectTypeNames.ToList(), leakedKeyCount, elapsed));
        }

        public void CompactorRemovedLeaks(IReadOnlyCollection<string> leakedObjectTypeNames, ulong removedKeyCount,
            TimeSpan elapsed)
        {
            RemovedLeaks.Add(new(leakedObjectTypeNames.ToList(), removedKeyCount, elapsed));
        }
    }

    sealed class TestDbContext : IDisposable
    {
        readonly InMemoryFileCollection _fileCollection = new();
        public readonly BTreeKeyValueDB LowDb;
        public readonly TestLogger Logger = new();
        public readonly ObjectDB Db = new();

        public TestDbContext(DBOptions options)
        {
            LowDb = new(new KeyValueDBOptions
            {
                CompactorScheduler = null,
                Compression = new NoCompressionStrategy(),
                FileCollection = _fileCollection
            });
            Db.Open(LowDb, false, options.WithLogger(Logger));
        }

        public void Dispose()
        {
            Db.Dispose();
            LowDb.Dispose();
            _fileCollection.Dispose();
        }
    }

    sealed record DetectedLeaksLog(List<string> TypeNames, ulong LeakedKeyCount, TimeSpan Elapsed);
    sealed record RemovedLeaksLog(List<string> TypeNames, ulong RemovedKeyCount, TimeSpan Elapsed);
}
