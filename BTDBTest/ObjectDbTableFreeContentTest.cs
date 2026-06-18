using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbTableFreeContentTest : IDisposable
{
    IKeyValueDB _lowDb;
    IObjectDB _db;

    public ObjectDbTableFreeContentTest()
    {
        _lowDb = new InMemoryKeyValueDB();
        OpenDb();
    }

    void OpenDb()
    {
        _db = new ObjectDB();
        _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration());
    }

    void ReopenDb()
    {
        _db.Dispose();
        OpenDb();
    }

    public class Link
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IDictionary<ulong, ulong> Edges { get; set; }
    }

    public interface ILinks : IRelation<Link>
    {
        void Insert(Link link);
        void Update(Link link);
        void ShallowUpdate(Link link);
        bool ShallowUpsert(Link link);
        bool RemoveById(ulong id);
        bool ShallowRemoveById(ulong id);
        Link FindById(ulong id);
    }

    [Fact]
    public void FreeIDictionary()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void FreeIDictionaryByRemoveAll()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            links.RemoveAll();
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    Func<IObjectDBTransaction, ILinks> InitILinks()
    {
        Func<IObjectDBTransaction, ILinks> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinks>("LinksRelation");
            var links = creator(tr);
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 } };
            links.Insert(link);
            tr.Commit();
        }

        return creator;
    }

    public class BitmapLink
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IRoaringBitmap Bits { get; set; }
    }

    public interface IBitmapLinks : IRelation<BitmapLink>
    {
        void Insert(BitmapLink link);
        void Update(BitmapLink link);
        bool RemoveById(ulong id);
        bool ShallowRemoveById(ulong id);
        BitmapLink FindById(ulong id);
    }

    [Fact]
    public void RoaringBitmapPersistsPagesAndCount()
    {
        var creator = InitBitmapLinks(out _);
        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            Assert.True(link.Bits.Get(1));
            Assert.True(link.Bits.Get(65536 + 7));
            Assert.True(link.Bits.Get(2 * 65536 + 42));
            Assert.False(link.Bits.Get(65536 + 8));
            Assert.Equal(new List<ulong> { 1, 65536 + 7, 2 * 65536 + 42 }, new List<ulong>(link.Bits));
            Assert.Equal(3ul, link.Bits.Count);
            link.Bits.Set(65536 + 7, false);
            link.Bits.Set(65536 + 8, true);
            Assert.False(link.Bits.Get(65536 + 7));
            Assert.True(link.Bits.Get(65536 + 8));
            Assert.Equal(3ul, link.Bits.Count);
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            Assert.True(link.Bits.Get(65536 + 7));
            Assert.False(link.Bits.Get(65536 + 8));
            Assert.Equal(3ul, link.Bits.Count);
            link.Bits.Set(65536 + 7, false);
            link.Bits.Set(65536 + 8, true);
            link.Bits.Flush();
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            Assert.False(link.Bits.Get(65536 + 7));
            Assert.True(link.Bits.Get(65536 + 8));
            Assert.Equal(3ul, link.Bits.Count);
        }
    }

    [Fact]
    public void FreeRoaringBitmapByRemoveAndUpdate()
    {
        var creator = InitBitmapLinks(out var firstBitmapId);
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(HasExternalContent(firstBitmapId));
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
        Assert.False(HasExternalContent(firstBitmapId));

        ulong secondBitmapId;
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var bitmap = CreateBitmap(tr, 10);
            secondBitmapId = bitmap.Id;
            links.Insert(new BitmapLink { Id = 2, Bits = bitmap });
            tr.Commit();
        }

        ulong thirdBitmapId;
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var bitmap = CreateBitmap(tr, 20);
            thirdBitmapId = bitmap.Id;
            links.Update(new BitmapLink { Id = 2, Bits = bitmap });
            tr.Commit();
        }

        AssertNoLeaksInDb();
        Assert.False(HasExternalContent(secondBitmapId));
        Assert.True(HasExternalContent(thirdBitmapId));
    }

    [Fact]
    public void ClearRoaringBitmapDeletesExternalContent()
    {
        var creator = InitBitmapLinks(out var bitmapId);
        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            link.Bits.Clear();
            tr.Commit();
        }

        AssertNoLeaksInDb();
        Assert.False(HasExternalContent(bitmapId));
        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            Assert.Equal(0ul, link.Bits.Count);
            Assert.False(link.Bits.Get(1));
        }
    }

    [Fact]
    public void ShallowRemoveRoaringBitmapLeaksLikeDictionary()
    {
        var creator = InitBitmapLinks(out var bitmapId);
        using (var tr = _db.StartTransaction())
        {
            Assert.True(creator(tr).ShallowRemoveById(1));
            tr.Commit();
        }

        Assert.Contains(ExternalContentPrefixHex(bitmapId), FindLeaks());
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncAppliesCommandsInShortTransactions()
    {
        var creator = InitBitmapLinks(out _);
        var left = RoaringBitmap.Source(new ulong[] { 1, 2, 65536 + 1, 2 * 65536 + 2 });
        var right = RoaringBitmap.Source(new ulong[] { 2, 65536 + 2, 2 * 65536 + 2, 3 * 65536 + 3 });
        var remove = RoaringBitmap.Source(new ulong[] { 2, 3 * 65536 + 3 });
        var operation = RoaringBitmap.And(RoaringBitmap.Op(left, right), RoaringBitmap.Not(remove));

        await RoaringBitmap.BuildAsync(operation, (commands, _) =>
        {
            using var tr = _db.StartTransaction();
            var link = creator(tr).FindById(1);
            link.Bits.ApplyCommands(commands);
            tr.Commit();
            return Task.CompletedTask;
        }, new byte[RoaringBitmaps.BitmapSize + 64], 3 * 65536 + 3, CancellationToken.None);

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            Assert.Equal(4ul, link.Bits.Count);
            Assert.Equal(new List<ulong> { 1, 65536 + 1, 65536 + 2, 2 * 65536 + 2 },
                new List<ulong>(link.Bits));
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncSupportsBoundedNot()
    {
        var creator = InitBitmapLinks(out _);
        var operation = RoaringBitmap.Not(RoaringBitmap.Source(new ulong[] { 1, 65536 + 1 }));

        await RoaringBitmap.BuildAsync(operation, (commands, _) =>
        {
            using var tr = _db.StartTransaction();
            var link = creator(tr).FindById(1);
            link.Bits.ApplyCommands(commands);
            tr.Commit();
            return Task.CompletedTask;
        }, new byte[RoaringBitmaps.BitmapSize + 64], 65536 + 2, CancellationToken.None);

        ReopenDb();

        using var tr = _db.StartTransaction();
        var link = creator(tr).FindById(1);
        Assert.True(link.Bits.Get(0));
        Assert.False(link.Bits.Get(1));
        Assert.True(link.Bits.Get(2));
        Assert.False(link.Bits.Get(65536 + 1));
        Assert.True(link.Bits.Get(65536 + 2));
        Assert.False(link.Bits.Get(65536 + 3));
        Assert.Equal(65537ul, link.Bits.Count);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncStartsWithClearAndEndsWithCount()
    {
        var chunks = new List<byte[]>();
        await RoaringBitmap.BuildAsync(RoaringBitmap.Source(new ulong[] { 1, 65536 + 1 }), (commands, _) =>
        {
            chunks.Add(commands.ToArray());
            return Task.CompletedTask;
        }, new byte[RoaringBitmaps.BitmapSize + 64], 65536 + 1, CancellationToken.None);

        Assert.NotEmpty(chunks);
        Assert.Equal(0, chunks[0][0]);
        Assert.Equal(2, chunks[^1][^2]);
        Assert.Equal(2, chunks[^1][^1]);
        foreach (var chunk in chunks)
        {
            var offset = 0;
            while (offset < chunk.Length)
            {
                var command = chunk[offset++];
                if (command == 0)
                    continue;
                if (command == 2)
                {
                    PackUnpack.UnpackVUInt(chunk, ref offset);
                    continue;
                }

                Assert.Equal(1, command);
                PackUnpack.UnpackVUInt(chunk, ref offset);
                offset = (offset + 1) & ~1;
                Assert.Equal(0, offset & 1);
                var valueLength = BinaryPrimitives.ReadUInt16LittleEndian(chunk.AsSpan(offset));
                offset += sizeof(ushort);
                Assert.Equal(0, offset & 1);
                offset += valueLength;
            }
        }
    }

    [Fact]
    public void RoaringBitmapIsCompleteDetectsMissingCount()
    {
        var creator = InitBitmapLinks(out _);
        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            link.Bits.Clear();
            link.Bits.ApplyCommands(new byte[] { 1, 0, 2, 0, 1, 0 });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            Assert.False(link.Bits.IsComplete());
            link.Bits.Flush();
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var link = creator(tr).FindById(1);
            Assert.True(link.Bits.IsComplete());
        }
    }

    [Fact]
    public void RoaringBitmapApplyCommandsRequiresFlushedLocalChanges()
    {
        using var tr = _db.StartTransaction();
        var bitmap = new ODBRoaringBitmap((IInternalObjectDBTransaction)tr);
        bitmap.Set(1, true);

        Assert.Throws<InvalidOperationException>(() => bitmap.ApplyCommands(new byte[] { 0 }));

        bitmap.Flush();
        bitmap.ApplyCommands(new byte[] { 0 });
        Assert.Equal(0ul, bitmap.Count);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncSupportsSourceVariants()
    {
        var listResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.Source(new List<ulong> { 1, 2, 65536, 65537 }), 65536);
        Assert.Equal(3ul, listResult.Count);
        Assert.Equal(new List<ulong> { 1, 2, 65536 }, listResult.Values);

        using var tr = _db.StartTransaction();
        var bitmap = CreateBitmap(tr, 3, 65536 + 4);
        var bitmapResult = await BuildRoaringBitmapCommands(RoaringBitmap.Source((IRoaringBitmap)bitmap), 65536 + 4);
        Assert.Equal(2ul, bitmapResult.Count);
        Assert.Equal(new List<ulong> { 3, 65536 + 4 }, bitmapResult.Values);

        var doubleNotResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.Not(RoaringBitmap.Not(RoaringBitmap.Source(new ulong[] { 5, 6 }))), 6);
        Assert.Equal(2ul, doubleNotResult.Count);
        Assert.Equal(new List<ulong> { 5, 6 }, doubleNotResult.Values);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncCopiesStoredBitmapPagesWithoutRecompressing()
    {
        using var tr = _db.StartTransaction();
        var bitmap = new ODBRoaringBitmap((IInternalObjectDBTransaction)tr);
        var storedPage = new byte[] { 0, 0, 255, 255, 0 };
        bitmap.ApplyCommands(new byte[] { 1, 0, 5, 0, 0, 0, 255, 255, 0 });

        var chunks = await BuildRoaringBitmapChunks(RoaringBitmap.Source((IRoaringBitmap)bitmap),
            ushort.MaxValue, RoaringBitmaps.BitmapSize + 64, false);
        var payloads = ExtractRoaringBitmapPagePayloads(chunks);

        var payload = Assert.Single(payloads);
        Assert.Equal(storedPage, payload);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncSupportsOrAndNotOperations()
    {
        var left = RoaringBitmap.Source(new ulong[] { 1, 2, 65536 + 1, 2 * 65536 + 5 });
        var right = RoaringBitmap.Source(new ulong[] { 2, 3, 65536 + 2, 3 * 65536 + 1 });

        var orResult = await BuildRoaringBitmapCommands(RoaringBitmap.Or(left, right), 2 * 65536 + 10);
        Assert.Equal(6ul, orResult.Count);
        Assert.Equal(new List<ulong> { 1, 2, 3, 65536 + 1, 65536 + 2, 2 * 65536 + 5 }, orResult.Values);

        var andResult = await BuildRoaringBitmapCommands(RoaringBitmap.And(left, right), 2 * 65536 + 10);
        Assert.Equal(1ul, andResult.Count);
        Assert.Equal(new List<ulong> { 2 }, andResult.Values);

        var notResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.Not(RoaringBitmap.Source(new ulong[] { 1, 65536 + 1 })), 65536 + 2);
        Assert.Equal(65537ul, notResult.Count);
        Assert.Contains(0ul, notResult.Values);
        Assert.DoesNotContain(1ul, notResult.Values);
        Assert.Contains(65536ul, notResult.Values);
        Assert.DoesNotContain(65536ul + 1, notResult.Values);
        Assert.Contains(65536ul + 2, notResult.Values);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncSupportsGenericOperationsAndAsyncApplier()
    {
        var left = RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 1, 65536 + 1, 2 * 65536 + 1 });
        var right = RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 65536 + 1, 2 * 65536 + 2 });

        var orResult = await BuildRoaringBitmapCommands(RoaringBitmap.Or(left, right), 2 * 65536 + 2,
            asyncApplier: true);
        Assert.Equal(new List<ulong> { 1, 65536 + 1, 2 * 65536 + 1, 2 * 65536 + 2 }, orResult.Values);

        var andResult = await BuildRoaringBitmapCommands(RoaringBitmap.And(left, right), 2 * 65536 + 2,
            asyncApplier: true);
        Assert.Equal(new List<ulong> { 65536 + 1 }, andResult.Values);

        var andNotResult = await BuildRoaringBitmapCommands(RoaringBitmap.And(left, RoaringBitmap.Not(right)),
            2 * 65536 + 1, asyncApplier: true);
        Assert.Equal(new List<ulong> { 1, 2 * 65536 + 1 }, andNotResult.Values);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncValidatesArgumentsAndHandlesAsyncFlush()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => RoaringBitmap.BuildAsync(
            RoaringBitmap.Source(new ulong[] { 1 }), (_, _) => Task.CompletedTask, new byte[16], 1));

        await Assert.ThrowsAsync<ArgumentException>(() => RoaringBitmap.BuildAsync(new UnknownRoaringBitmapOp(),
            (_, _) => Task.CompletedTask, new byte[RoaringBitmaps.BitmapSize + 32], 1));

        var result = await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.Source(Array.Empty<ulong>())),
            2 * 65536 + 1, RoaringBitmaps.BitmapSize + 32, true);
        Assert.Equal(2 * 65536ul + 2, result.Count);
        Assert.True(result.Chunks.Count > 1);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncCoversAsyncEnumerableFlushes()
    {
        var arrayValues = ValuesOnPages(1200);
        arrayValues[1199] = 1200ul << 16;
        var arrayResult = await BuildRoaringBitmapCommands(RoaringBitmap.Source(arrayValues), 1199ul << 16,
            RoaringBitmaps.BitmapSize + 32, true);
        Assert.Equal(1199ul, arrayResult.Count);
        Assert.DoesNotContain(1200ul << 16, arrayResult.Values);
        Assert.True(arrayResult.Chunks.Count > 1);

        IRoaringBitmapOp enumerableArray = RoaringBitmap.Source((IEnumerable<ulong>)new ulong[] { 7, 8 });
        var enumerableArrayResult = await BuildRoaringBitmapCommands(enumerableArray, 8);
        Assert.Equal(new List<ulong> { 7, 8 }, enumerableArrayResult.Values);

        var listResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>(ValuesOnPages(1200))), 1199ul << 16,
            RoaringBitmaps.BitmapSize + 32, true);
        Assert.Equal(1200ul, listResult.Count);
        Assert.True(listResult.Chunks.Count > 1);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncCoversGenericNotAndIteratorBooleanPages()
    {
        var genericLeft = RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>
            { 1, 65536 + 1, 2 * 65536 + 1, 4 * 65536 + 1 });
        var genericRight = RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>
            { 0, 65536 + 1, 3 * 65536 + 1 });

        var notResult = await BuildRoaringBitmapCommands(RoaringBitmap.Not(genericRight), 2 * 65536,
            asyncApplier: true);
        Assert.Contains(1ul, notResult.Values);
        Assert.DoesNotContain(65536ul + 1, notResult.Values);
        Assert.DoesNotContain(2ul * 65536 + 1, notResult.Values);

        var orWithNotResult = await BuildRoaringBitmapCommands(RoaringBitmap.Or(RoaringBitmap.Not(genericRight),
            genericLeft), 2 * 65536, asyncApplier: true);
        Assert.Contains(2ul * 65536, orWithNotResult.Values);
        Assert.DoesNotContain(2ul * 65536 + 1, orWithNotResult.Values);

        var notAndNotResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.Not(RoaringBitmap.And(genericLeft, RoaringBitmap.Not(genericRight))), 2 * 65536,
            asyncApplier: true);
        Assert.DoesNotContain(1ul, notAndNotResult.Values);

        var leftNotAndResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.Not(RoaringBitmap.And(RoaringBitmap.Not(genericRight), genericLeft)), 2 * 65536,
            asyncApplier: true);
        Assert.Contains(2ul * 65536, leftNotAndResult.Values);

        var directLeftNotAndResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.And(RoaringBitmap.Not(genericRight), genericLeft), 2 * 65536, asyncApplier: true);
        Assert.Equal(new List<ulong> { 1 }, directLeftNotAndResult.Values);

        var maxPageAndNotResult = await BuildRoaringBitmapCommands(
            RoaringBitmap.Not(RoaringBitmap.And(
                RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 2 * 65536 }),
                RoaringBitmap.Not(RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>())))), 2 * 65536,
            asyncApplier: true);
        Assert.DoesNotContain(2ul * 65536, maxPageAndNotResult.Values);

        var notOrResult = await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.Or(genericLeft,
            genericRight)), 2 * 65536, asyncApplier: true);
        Assert.DoesNotContain(1ul, notOrResult.Values);

        var notAndResult = await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.And(genericLeft,
            genericRight)), 2 * 65536, asyncApplier: true);
        Assert.Contains(1ul, notAndResult.Values);

        await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.And(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 0 }),
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 65536 }))), 65536, asyncApplier: true);
        await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.And(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 65536 }),
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 0 }))), 65536, asyncApplier: true);
        await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.And(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 0 }),
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>()))), 0, asyncApplier: true);
        await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.Or(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 0 }),
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 65536 }))), 65536, asyncApplier: true);
        await BuildRoaringBitmapCommands(RoaringBitmap.Not(RoaringBitmap.Or(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>()),
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 0 }))), 0, asyncApplier: true);

        var singleValueNot = await BuildRoaringBitmapCommands(
            RoaringBitmap.Not(RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>())), 0);
        Assert.Equal(new List<ulong> { 0 }, singleValueNot.Values);
    }

    [Fact]
    public async Task RoaringBitmapBuildAsyncCoversBinaryEdgeBranches()
    {
        var left = ValuesOnPages(1200);
        var right = ValuesOnPages(1200, 1);
        left[1199] = 1200ul << 16;
        right[1199] = 1200ul << 16;

        var asyncOr = await BuildRoaringBitmapCommands(RoaringBitmap.Or(RoaringBitmap.Source(left),
                RoaringBitmap.Source(right)), 1199ul << 16, RoaringBitmaps.BitmapSize + 32, true);
        Assert.Equal(2398ul, asyncOr.Count);
        Assert.DoesNotContain(1200ul << 16, asyncOr.Values);
        Assert.True(asyncOr.Chunks.Count > 1);

        var asyncAnd = await BuildRoaringBitmapCommands(
            RoaringBitmap.And(RoaringBitmap.Source(new ulong[] { 2 * 65536 + 1, 3 * 65536 + 1 }),
                RoaringBitmap.Source(new ulong[] { 1, 2 * 65536 + 1, 4 * 65536 + 1 })), 4 * 65536 + 1,
            asyncApplier: true);
        Assert.Equal(new List<ulong> { 2ul * 65536 + 1 }, asyncAnd.Values);

        var genericAnd = await BuildRoaringBitmapCommands(
            RoaringBitmap.And(
                RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 2 * 65536 + 1, 4 * 65536 + 1 }),
                RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 65536 + 1, 2 * 65536 + 1 })),
            4 * 65536 + 1, asyncApplier: true);
        Assert.Equal(new List<ulong> { 2ul * 65536 + 1 }, genericAnd.Values);

        var genericRightOnlyOr = await BuildRoaringBitmapCommands(
            RoaringBitmap.Or(RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>()),
                RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 1 })), 1, asyncApplier: true);
        Assert.Equal(new List<ulong> { 1 }, genericRightOnlyOr.Values);

        var leftMany = ValuesOnPages(1204);
        var rightMany = ValuesOnPages(1204);
        leftMany[1200] = 1201ul << 16;
        leftMany[1201] = 1204ul << 16;
        leftMany[1202] = 1205ul << 16;
        leftMany[1203] = 1206ul << 16;
        rightMany[1200] = 1202ul << 16;
        rightMany[1201] = 1203ul << 16;
        rightMany[1202] = 1205ul << 16;
        rightMany[1203] = 1207ul << 16;
        var asyncAndWithContinuationSkips = await BuildRoaringBitmapCommands(
            RoaringBitmap.And(RoaringBitmap.Source(leftMany), RoaringBitmap.Source(rightMany)), 1207ul << 16,
            RoaringBitmaps.BitmapSize + 32, true);
        Assert.Contains(1205ul << 16, asyncAndWithContinuationSkips.Values);

        var notChunks = await BuildRoaringBitmapChunks(
            RoaringBitmap.Not(RoaringBitmap.Source(Array.Empty<ulong>())), 1200ul << 16,
            RoaringBitmaps.BitmapSize + 32, true);
        Assert.True(notChunks.Count > 1);

        var outOfRangeNot = await BuildRoaringBitmapCommands(
            RoaringBitmap.Not(RoaringBitmap.Source(new ulong[] { 65536 + 1 })), 65536);
        Assert.Contains(65536ul, outOfRangeNot.Values);

        var arrayOr = await BuildRoaringBitmapCommands(
            RoaringBitmap.Or(RoaringBitmap.Source(new ulong[] { 1 }), RoaringBitmap.Source(new ulong[] { 1, 2 })),
            2);
        Assert.Equal(new List<ulong> { 1, 2 }, arrayOr.Values);

        var arrayAnd = await BuildRoaringBitmapCommands(
            RoaringBitmap.And(RoaringBitmap.Source(new ulong[] { 5 }), RoaringBitmap.Source(new ulong[] { 1, 5 })),
            5);
        Assert.Equal(new List<ulong> { 5 }, arrayAnd.Values);

        var outOfRange = await BuildRoaringBitmapCommands(
            RoaringBitmap.Or(RoaringBitmap.Source(new ulong[] { 10 }), RoaringBitmap.Source(new ulong[] { 11 })), 1);
        Assert.Empty(outOfRange.Values);

        var directArray = await BuildRoaringBitmapCommands(RoaringBitmap.Source(new ulong[] { 1, 2, 10 }), 2);
        Assert.Equal(new List<ulong> { 1, 2 }, directArray.Values);
    }

    [Fact]
    public async Task RoaringBitmapCommandWriterCoversDefensiveAsyncBranches()
    {
        var writerType = typeof(RoaringBitmap).GetNestedType("CommandWriter", BindingFlags.NonPublic)!;
        var ctor = writerType.GetConstructor(BindingFlags.Instance | BindingFlags.Public, null,
            new[] { typeof(Memory<byte>), typeof(Func<ReadOnlyMemory<byte>, CancellationToken, Task>), typeof(CancellationToken) },
            null)!;
        var chunks = new List<byte[]>();
        Func<ReadOnlyMemory<byte>, CancellationToken, Task> applier = async (commands, _) =>
        {
            chunks.Add(commands.ToArray());
            await Task.Yield();
        };
        var writer = ctor.Invoke(new object[] { new Memory<byte>(new byte[4]), applier, CancellationToken.None });
        writerType.GetMethod("WriteClear")!.Invoke(writer, Array.Empty<object>());
        writerType.GetMethod("WriteCount")!.Invoke(writer, new object[] { 128ul });
        await AwaitValueTask(writerType.GetMethod("FlushAsync")!.Invoke(writer, Array.Empty<object>())!);
        await AwaitValueTask(writerType.GetMethod("FlushAsync")!.Invoke(writer, Array.Empty<object>())!);

        writer = ctor.Invoke(new object[]
            { new Memory<byte>(new byte[RoaringBitmaps.BitmapSize + 32]), applier, CancellationToken.None });
        writerType.GetMethod("WriteClear")!.Invoke(writer, Array.Empty<object>());
        writerType.GetMethod("WriteCount")!.Invoke(writer, new object[] { 0ul });
        var startPage = writerType.GetMethod("StartPage")!;
        var commitPage = writerType.GetMethod("CommitPage")!;
        var pageContent = (Memory<byte>)startPage.Invoke(writer, new object[] { 0ul })!;
        pageContent.Span[0] = 0;
        pageContent.Span[1] = 0;
        await AwaitValueTask(commitPage.Invoke(writer, new object[] { 2u })!);
        startPage.Invoke(writer, new object[] { 1ul });
        await AwaitValueTask(commitPage.Invoke(writer, new object[] { 0u })!);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            AwaitValueTask(commitPage.Invoke(writer, new object[] { (uint)RoaringBitmaps.BitmapSize + 1 })!));

        writer = ctor.Invoke(new object[] { new Memory<byte>(new byte[RoaringBitmaps.BitmapSize + 12]), applier,
            CancellationToken.None });
        writerType.GetMethod("WriteClear")!.Invoke(writer, Array.Empty<object>());
        startPage.Invoke(writer, new object[] { 2ul });
        await AwaitValueTask(commitPage.Invoke(writer, new object[] { 0u })!);

        writer = ctor.Invoke(new object[] { new Memory<byte>(new byte[16]), applier, CancellationToken.None });
        var tooSmall = Assert.Throws<TargetInvocationException>(() => startPage.Invoke(writer, new object[] { 0ul }));
        Assert.IsType<ArgumentException>(tooSmall.InnerException);
        Assert.NotEmpty(chunks);
    }

    [Fact]
    public void RoaringBitmapPrivatePageIteratorsCanBeFullyEnumerated()
    {
        var maxPageAndNot = RoaringBitmap.And(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 65536 }),
            RoaringBitmap.Not(RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>())));
        FullyEnumeratePages(maxPageAndNot, 65536);

        var rightOnlyOr = RoaringBitmap.Or(
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong>()),
            RoaringBitmap.Source((IEnumerable<ulong>)new List<ulong> { 0 }));
        FullyEnumeratePages(rightOnlyOr, 65536);
    }

    class UnknownRoaringBitmapOp : IRoaringBitmapOp
    {
    }

    static async Task<(List<ulong> Values, ulong Count, List<byte[]> Chunks)> BuildRoaringBitmapCommands(
        IRoaringBitmapOp operation, ulong maxValidIndex, int bufferSize = RoaringBitmaps.BitmapSize + 64,
        bool asyncApplier = false)
    {
        var values = new List<ulong>();
        var chunks = new List<byte[]>();
        ulong count = 0;
        await RoaringBitmap.BuildAsync(operation, async (commands, _) =>
        {
            var chunk = commands.ToArray();
            chunks.Add(chunk);
            if (asyncApplier)
                await Task.Yield();
            ParseRoaringBitmapCommands(chunk, values, ref count);
        }, new byte[bufferSize], maxValidIndex, CancellationToken.None);
        values.Sort();
        return (values, count, chunks);
    }

    static async Task<List<byte[]>> BuildRoaringBitmapChunks(IRoaringBitmapOp operation, ulong maxValidIndex,
        int bufferSize, bool asyncApplier)
    {
        var chunks = new List<byte[]>();
        await RoaringBitmap.BuildAsync(operation, async (commands, _) =>
        {
            chunks.Add(commands.ToArray());
            if (asyncApplier)
                await Task.Yield();
        }, new byte[bufferSize], maxValidIndex, CancellationToken.None);
        return chunks;
    }

    static void ParseRoaringBitmapCommands(byte[] commands, List<ulong> values, ref ulong count)
    {
        var offset = 0;
        while (offset < commands.Length)
        {
            var command = commands[offset++];
            switch (command)
            {
                case 0:
                    values.Clear();
                    count = 0;
                    break;
                case 1:
                {
                    var pageIndex = PackUnpack.UnpackVUInt(commands, ref offset);
                    offset = (offset + 1) & ~1;
                    var valueLength = BinaryPrimitives.ReadUInt16LittleEndian(commands.AsSpan(offset));
                    offset += sizeof(ushort);
                    var encoded = commands.AsMemory(offset, valueLength);
                    foreach (var value in RoaringBitmaps.Enumerate(encoded, pageIndex << 16))
                        values.Add(value);
                    offset += valueLength;
                    break;
                }
                case 2:
                    count = PackUnpack.UnpackVUInt(commands, ref offset);
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }
    }

    static List<byte[]> ExtractRoaringBitmapPagePayloads(List<byte[]> chunks)
    {
        var result = new List<byte[]>();
        foreach (var commands in chunks)
        {
            var offset = 0;
            while (offset < commands.Length)
            {
                var command = commands[offset++];
                switch (command)
                {
                    case 0:
                        break;
                    case 1:
                    {
                        PackUnpack.UnpackVUInt(commands, ref offset);
                        offset = (offset + 1) & ~1;
                        var valueLength = BinaryPrimitives.ReadUInt16LittleEndian(commands.AsSpan(offset));
                        offset += sizeof(ushort);
                        result.Add(commands[offset..(offset + valueLength)]);
                        offset += valueLength;
                        break;
                    }
                    case 2:
                        PackUnpack.UnpackVUInt(commands, ref offset);
                        break;
                    default:
                        throw new InvalidOperationException();
                }
            }
        }

        return result;
    }

    static ulong[] ValuesOnPages(int count, ushort offset = 0)
    {
        var result = new ulong[count];
        for (var i = 0; i < result.Length; i++)
            result[i] = ((ulong)i << 16) + offset;
        return result;
    }

    static async Task AwaitValueTask(object valueTask)
    {
        var task = (Task)valueTask.GetType().GetMethod("AsTask")!.Invoke(valueTask, Array.Empty<object>())!;
        await task;
    }

    static void FullyEnumeratePages(IRoaringBitmapOp operation, ulong maxValidIndex)
    {
        using var pages = (IDisposable)operation.GetType()
            .GetMethod("Pages", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(operation, new object[] { maxValidIndex })!;
        var readNext = pages.GetType().GetMethod("ReadNext")!;
        var readValue = pages.GetType().GetMethod("ReadValue")!;
        var buffer = new Memory<byte>(new byte[RoaringBitmaps.BitmapSize]);
        while (readNext.Invoke(pages, Array.Empty<object>()) is not null)
            readValue.Invoke(pages, new object[] { buffer });
    }

    Func<IObjectDBTransaction, IBitmapLinks> InitBitmapLinks(out ulong bitmapId)
    {
        Func<IObjectDBTransaction, IBitmapLinks> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IBitmapLinks>("BitmapLinksRelation");
            var links = creator(tr);
            var bitmap = CreateBitmap(tr, 1, 65536 + 7, 2 * 65536 + 42);
            bitmapId = bitmap.Id;
            links.Insert(new BitmapLink { Id = 1, Bits = bitmap });
            tr.Commit();
        }

        return creator;
    }

    static ODBRoaringBitmap CreateBitmap(IObjectDBTransaction tr, params ulong[] values)
    {
        var bitmap = new ODBRoaringBitmap((IInternalObjectDBTransaction)tr);
        foreach (var value in values)
            bitmap.Set(value, true);
        bitmap.Flush();
        return bitmap;
    }

    bool HasExternalContent(ulong id)
    {
        var len = PackUnpack.LengthVUInt(id);
        Span<byte> prefix = stackalloc byte[ObjectDB.AllDictionariesPrefixLen + (int)len];
        prefix[0] = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(ref prefix[ObjectDB.AllDictionariesPrefixLen], id, len);
        using var tr = _lowDb.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        return cursor.FindNextKey(prefix);
    }

    static string ExternalContentPrefixHex(ulong id)
    {
        var len = PackUnpack.LengthVUInt(id);
        Span<byte> prefix = stackalloc byte[ObjectDB.AllDictionariesPrefixLen + (int)len];
        prefix[0] = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(ref prefix[ObjectDB.AllDictionariesPrefixLen], id, len);
        var builder = new StringBuilder();
        foreach (var b in prefix)
            builder.Append(b.ToString("X2"));
        return builder.ToString();
    }

    [Fact]
    public void FreeIDictionaryInUpdate()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            links.Insert(new Link { Id = 2, Edges = new Dictionary<ulong, ulong> { [10] = 20 } });
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.Update(link); //replace dict
            link = links.FindById(2);
            link.Edges.Add(20, 30);
            links.Update(link); //update dict, must not free
            link = links.FindById(2);
            Assert.Equal(2, link.Edges.Count);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void LeakingIDictionaryInShallowUpdate()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            links.Insert(new Link { Id = 2, Edges = new Dictionary<ulong, ulong> { [10] = 20 } });
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.ShallowUpdate(link); //replace dict
            link = links.FindById(2);
            link.Edges.Add(20, 30);
            links.ShallowUpdate(link); //update dict, must not free
            link = links.FindById(2);
            Assert.Equal(2, link.Edges.Count);
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    [Fact]
    public void LeakingIDictionaryInShallowRemove()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.ShallowRemoveById(1)); //remove without free
            Assert.Empty(links);
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    [Fact]
    public void ReuseIDictionaryAfterShallowRemove()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var value = links.FindById(1);
            links.ShallowRemoveById(1); //remove without free
            Assert.Empty(links);
            links.Insert(value);
            Assert.Equal(3, value.Edges.Count);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void FreeIDictionaryInUpsert()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.Upsert(link); //replace dict
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void LeakingIDictionaryInShallowUpsert()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.ShallowUpsert(link); //replace dict
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    public class LinkInList
    {
        [PrimaryKey] public ulong Id { get; set; }
        public List<IDictionary<ulong, ulong>> EdgesList { get; set; }
    }

    public interface ILinksInList : IRelation<LinkInList>
    {
        void Insert(LinkInList link);
        void Update(LinkInList link);
        LinkInList FindById(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInList()
    {
        Func<IObjectDBTransaction, ILinksInList> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInList>("ListLinksRelation");
            var links = creator(tr);
            var link = new LinkInList
            {
                Id = 1,
                EdgesList = new List<IDictionary<ulong, ulong>>
                {
                    new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 },
                    new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }
                }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);

            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void FreeIDictionaryInListInUpdate()
    {
        Func<IObjectDBTransaction, ILinksInList> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInList>("ListLinksRelation");
            var links = creator(tr);
            var link = new LinkInList
            {
                Id = 1,
                EdgesList = new List<IDictionary<ulong, ulong>>
                {
                    new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 },
                    new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }
                }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = links.FindById(1);
            for (int i = 0; i < 20; i++)
                link.EdgesList.Add(new Dictionary<ulong, ulong> { [10] = 20 });
            links.Update(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class LinkInDict
    {
        [PrimaryKey] public ulong Id { get; set; }
        public Dictionary<int, IDictionary<ulong, ulong>> EdgesIDict { get; set; }
        public string Name { get; set; }
    }

    public interface ILinksInDict : IRelation<LinkInDict>
    {
        void Insert(LinkInDict link);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInDictionary()
    {
        Func<IObjectDBTransaction, ILinksInDict> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInDict>("DictLinksRelation");
            var links = creator(tr);
            var link = new LinkInDict
            {
                Id = 1,
                EdgesIDict = new Dictionary<int, IDictionary<ulong, ulong>>
                {
                    [0] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 },
                    [1] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }
                }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class LinkInIDict
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IDictionary<int, IDictionary<ulong, ulong>> EdgesIDict { get; set; }
    }

    public interface ILinksInIDict : IRelation<LinkInIDict>
    {
        void Insert(LinkInIDict link);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInIDictionary()
    {
        Func<IObjectDBTransaction, ILinksInIDict> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInIDict>("IDictLinksRelation");
            var links = creator(tr);
            var link = new LinkInIDict
            {
                Id = 1,
                EdgesIDict = new Dictionary<int, IDictionary<ulong, ulong>>
                {
                    [0] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 },
                    [1] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }
                }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class LinkInOrderedIDict
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IOrderedDictionary<int, IDictionary<int, int>> Edges { get; set; }
    }

    public interface ILinksInOrderedIDict : IRelation<LinkInOrderedIDict>
    {
        void Insert(LinkInOrderedIDict link);
        LinkInOrderedIDict FindById(ulong id);
    }

    [Fact]
    public void ReplacingIDictionaryValueFreesNestedIDictionary()
    {
        var creator = InitOrderedDictionaryWithNestedDictionaries();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = links.FindById(1);
            link.Edges[1] = new Dictionary<int, int> { [30] = 40 };
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void RemovingIDictionaryValueFreesNestedIDictionary()
    {
        var creator = InitOrderedDictionaryWithNestedDictionaries();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = links.FindById(1);
            Assert.True(link.Edges.Remove(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void ClearingIDictionaryFreesNestedIDictionaries()
    {
        var creator = InitOrderedDictionaryWithNestedDictionaries();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = links.FindById(1);
            link.Edges.Clear();
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void RemovingIDictionaryRangeFreesNestedIDictionaries()
    {
        var creator = InitOrderedDictionaryWithNestedDictionaries();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = links.FindById(1);
            Assert.Equal(2, link.Edges.RemoveRange(1, true, 2, true));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void ReplacingIDictionaryValueThroughAdvancedEnumeratorFreesNestedIDictionary()
    {
        var creator = InitOrderedDictionaryWithNestedDictionaries();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = links.FindById(1);
            using var enumerator = link.Edges.GetAdvancedEnumerator(new(EnumerationOrder.Ascending, 1,
                KeyProposition.Included, 1, KeyProposition.Included));
            Assert.True(enumerator.NextKey(out var key));
            Assert.Equal(1, key);
            enumerator.CurrentValue = new Dictionary<int, int> { [50] = 60 };
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    Func<IObjectDBTransaction, ILinksInOrderedIDict> InitOrderedDictionaryWithNestedDictionaries()
    {
        Func<IObjectDBTransaction, ILinksInOrderedIDict> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInOrderedIDict>("OrderedIDictLinksRelation");
            var links = creator(tr);
            links.Insert(new LinkInOrderedIDict
            {
                Id = 1
            });
            var link = links.FindById(1);
            link.Edges[1] = new Dictionary<int, int> { [10] = 20 };
            link.Edges[2] = new Dictionary<int, int> { [20] = 30 };
            link.Edges[3] = new Dictionary<int, int> { [30] = 40 };
            tr.Commit();
        }

        AssertNoLeaksInDb();
        return creator;
    }

    public class Nodes
    {
        public IDictionary<ulong, ulong> Edges { get; set; }
    }

    public class Links
    {
        [PrimaryKey] public ulong Id { get; set; }
        public Nodes Nodes { get; set; }
    }

    public interface ILinksWithNodes : IRelation<Links>
    {
        void Insert(Links link);
        Links FindByIdOrDefault(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInInlineObject()
    {
        Func<IObjectDBTransaction, ILinksWithNodes> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksWithNodes>("IDictObjLinksRelation");
            var links = creator(tr);
            var link = new Links
            {
                Id = 1,
                Nodes = new Nodes { Edges = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 } }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var l = links.FindByIdOrDefault(1);
            Assert.Equal(2ul, l.Nodes.Edges[1]);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }


    public class BlobLocation
    {
        public string Name { get; set; }
    }

    public class LicenseFileDb
    {
        public string FileName { get; set; }
        public BlobLocation Location { get; set; }
    }

    public class LicenseDb
    {
        [PrimaryKey(1)] public ulong ItemId { get; set; }
        public LicenseFileDb LicenseFile { get; set; }
    }

    public interface ILicenseTable : IRelation<LicenseDb>
    {
        void Insert(LicenseDb license);
        void Update(LicenseDb license);
    }

    [Fact]
    public void DoNotCrashOnUnknownType()
    {
        Func<IObjectDBTransaction, ILicenseTable> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenseTable>("LicRel");
            var lics = creator(tr);
            var license = new LicenseDb { ItemId = 1 }; //no LicenseFileDb inserted
            lics.Insert(license);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var lics = creator(tr);
            var license = new LicenseDb { ItemId = 1 };
            lics.Update(license);

            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class License
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong UserId { get; set; }

        public IDictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>> ConcurrentFeautureItemsSessions
        {
            get;
            set;
        }
    }

    public class ConcurrentFeatureItemInfo
    {
        public DateTime UsedFrom { get; set; }
    }

    public interface ILicenses : IRelation<License>
    {
        void Insert(License license);
        bool RemoveById(ulong companyId, ulong userId);
        int RemoveById(ulong companyId);
    }

    [Fact]
    public void AlsoFieldsInsideIDictionaryAreStoredInlineByDefault()
    {
        Func<IObjectDBTransaction, ILicenses> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel");
            var lics = creator(tr);
            lics.Insert(new License());
            var license = new License
            {
                CompanyId = 1,
                UserId = 1,
                ConcurrentFeautureItemsSessions =
                    new Dictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>>
                    {
                        [4] = new Dictionary<ulong, ConcurrentFeatureItemInfo>
                            { [2] = new ConcurrentFeatureItemInfo() }
                    }
            };
            lics.Insert(license);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel");
            var lics = creator(tr);
            lics.RemoveById(0, 1);
            lics.RemoveById(1, 1);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }


    public class File
    {
        [PrimaryKey] public ulong Id { get; set; }

        public IIndirect<RawData> Data { get; set; }
    }

    [Generate]
    public class RawData
    {
        public byte[] Data { get; set; }
        public IDictionary<ulong, ulong> Edges { get; set; }
    }

    public interface IHddRelation : IRelation<File>
    {
        void Insert(File file);
        void RemoveById(ulong id);
        File FindById(ulong id);
    }

    [Fact]
    public void IIndirectIsNotFreedAutomatically()
    {
        Func<IObjectDBTransaction, IHddRelation> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IHddRelation>("HddRelation");
            var files = creator(tr);
            var file = new File
            {
                Id = 1,
                Data = new DBIndirect<RawData>(new RawData
                {
                    Data = new byte[] { 1, 2, 3 },
                    Edges = new Dictionary<ulong, ulong> { [10] = 20 }
                })
            };
            files.Insert(file);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var files = creator(tr);
            var file = files.FindById(1);
            Assert.Equal(file.Data.Value.Data, new byte[] { 1, 2, 3 });
            files.RemoveById(1);
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    [Fact]
    public void IIndirectMustBeFreedManually()
    {
        Func<IObjectDBTransaction, IHddRelation> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IHddRelation>("HddRelation");
            var files = creator(tr);
            var file = new File
            {
                Id = 1,
                Data = new DBIndirect<RawData>(new RawData
                {
                    Data = new byte[] { 1, 2, 3 },
                    Edges = new Dictionary<ulong, ulong> { [10] = 20 }
                })
            };
            files.Insert(file);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var files = creator(tr);
            var file = files.FindById(1);
            Assert.Equal(file.Data.Value.Data, new byte[] { 1, 2, 3 });
            file.Data.Value.Edges.Clear();
            tr.Delete(file.Data);
            files.RemoveById(1);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class Setting
    {
        [PrimaryKey] public ulong Id { get; set; }
        public License License { get; set; }
    }

    public interface ISettings : IRelation<Setting>
    {
        void Insert(Setting license);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void PreferInlineIsTransferredThroughDBObject()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<ISettings>("SettingRel");
            var settings = creator(tr);
            var setting = new Setting
            {
                Id = 1,
                License = new License
                {
                    CompanyId = 1,
                    ConcurrentFeautureItemsSessions =
                        new Dictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>>
                        {
                            [4] = new Dictionary<ulong, ConcurrentFeatureItemInfo>
                                { [2] = new ConcurrentFeatureItemInfo() }
                        }
                }
            };
            settings.Insert(setting);
            settings.RemoveById(1);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Generate]
    public interface INodes
    {
    }

    public class NodesA : INodes
    {
        public string F { get; set; }
        public IDictionary<ulong, ulong> A { get; set; }
    }

    public class NodesB : INodes
    {
        public IDictionary<ulong, ulong> B { get; set; }
        public string E { get; set; }
    }

    public class Graph
    {
        [PrimaryKey] public ulong Id { get; set; }
        public INodes Nodes { get; set; }
    }

    public interface IGraph : IRelation<Graph>
    {
        void Insert(Graph license);
        Graph FindById(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeWorksAlsoForDifferentSubObjects()
    {
        _db.RegisterType(typeof(NodesA));
        _db.RegisterType(typeof(NodesB));

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IGraph>("Graph");
            var table = creator(tr);
            var graph = new Graph
            {
                Id = 1,
                Nodes = new NodesA { A = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, F = "f" }
            };
            table.Insert(graph);
            graph = new Graph
            {
                Id = 2,
                Nodes = new NodesB { B = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, E = "e" }
            };
            table.Insert(graph);

            Assert.True(table.FindById(1).Nodes is NodesA);
            Assert.True(table.FindById(2).Nodes is NodesB);

            table.RemoveById(1);
            table.RemoveById(2);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class Component
    {
        public IList<Component> Children { get; set; }
        public IDictionary<string, string> Props { get; set; }
    }

    public class View
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public Component Component { get; set; }
    }

    public interface IViewTable : IRelation<View>
    {
        void Insert(View license);
        void RemoveById(ulong id);
    }

    [Fact]
    public void FreeWorksInRecursiveStructures()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IViewTable>("View");
            var table = creator(tr);
            table.Insert(new View
            {
                Id = 1,
                Component = new Component
                {
                    Children = new List<Component>
                    {
                        new Component { Props = new Dictionary<string, string> { ["a"] = "A" } },
                        new Component { Props = new Dictionary<string, string> { ["b"] = "B" } }
                    }
                }
            });
            table.RemoveById(1);
            AssertNoLeaksInDb();
        }
    }

    [Fact]
    public void FreeWorksTogetherWithRemoveByPrefix()
    {
        Func<IObjectDBTransaction, ILicenses> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel2");
            var lics = creator(tr);
            lics.Insert(new License());
            var license = new License
            {
                CompanyId = 1,
                UserId = 1,
                ConcurrentFeautureItemsSessions =
                    new Dictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>>
                    {
                        [4] = new Dictionary<ulong, ConcurrentFeatureItemInfo>
                            { [2] = new ConcurrentFeatureItemInfo() }
                    }
            };
            lics.Insert(license);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel2");
            var lics = creator(tr);
            Assert.Equal(1, lics.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public enum SimpleEnum
    {
        A,
        B
    }

    public class ComplexClass
    {
        public SimpleEnum Enum { get; set; }
        public IList<SimpleEnum> ListOfEnums { get; set; }
    }

    public class RowWithComplexClass
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public IList<SimpleEnum> ListOfEnums { get; set; }
        public IDictionary<int, SimpleEnum> Dict { get; set; }
        public ComplexClass Klass { get; set; }
    }

    public interface IRowWithComplexClassTable : IRelation<RowWithComplexClass>
    {
        int RemoveById();
    }

    [Fact]
    public void NeedFreeContentWorksOnComplexInlineClass()
    {
        using var tr = _db.StartTransaction();
        tr.GetRelation<IRowWithComplexClassTable>().RemoveById();
    }

    public abstract class UploadDataBase
    {
        public BlobLocation Location { get; set; }
        public string FileName { get; set; }
    }

    public class SharedImageFile : UploadDataBase
    {
        [PrimaryKey(1)]
        [SecondaryKey("CompanyId")]
        public ulong CompanyId { get; set; }

        [PrimaryKey(2)] public ulong Id { get; set; }

        public new BlobLocation Location
        {
            get => base.Location;
            set => base.Location = value;
        }

        public BlobLocation TempLocation { get; set; }

        public int Width { get; set; }
        public int Height { get; set; }

        public IDictionary<int, bool> Dict { get; set; }
    }

    public interface IFileTable : IRelation<SharedImageFile>
    {
        void Insert(SharedImageFile license);
        void RemoveById(ulong companyId, ulong id);
    }

    [Fact]
    public void IterateWellObjectsWithSharedInstance()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IFileTable>("IFileTable");
            var files = creator(tr);
            var loc = new BlobLocation();
            files.Insert(new SharedImageFile
            {
                Location = loc,
                TempLocation = loc,
                Dict = new Dictionary<int, bool> { [1] = true }
            });
            files.RemoveById(0, 0);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class ImportData
    {
        [PrimaryKey] public ulong CompanyId { get; set; }
        [PrimaryKey(Order = 1)] public ulong Id { get; set; }

        public IDictionary<ObjectId, ObjectNode> Items { get; set; }
    }

    public interface IImportDataTable : IRelation<ImportData>
    {
        bool Insert(ImportData item);
        void Update(ImportData item);
        int RemoveById(ulong companyId);
    }

    public class ObjectNode
    {
        public string Sample { get; set; }
    }

    public class ObjectId
    {
        public ulong Id { get; set; }
    }

    [Fact]
    public void DoNotPanicWhenUnknownStatusInIDictionaryKey()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IImportDataTable>("ImportData");
            var table = creator(tr);
            table.Insert(new ImportData
            {
                Items = new Dictionary<ObjectId, ObjectNode>
                {
                    [new ObjectId()] = new ObjectNode()
                }
            });
            tr.Commit();
        }
    }

    [Generate]
    [PersistedName("NodesBase")]
    public class NodesBase
    {
    }

    public class NodesOne : NodesBase
    {
        public string F { get; set; }
        public IDictionary<ulong, ulong> A { get; set; }
    }

    public class NodesTwo : NodesBase
    {
        public IDictionary<ulong, ulong> B { get; set; }
        public string E { get; set; }
    }

    public class NodesGraph
    {
        [PrimaryKey] public ulong Id { get; set; }
        public NodesBase Nodes { get; set; }
    }

    public interface IGraphTable : IRelation<NodesGraph>
    {
        void Insert(NodesGraph license);
        NodesGraph FindById(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeWorksAlsoForDifferentSubObjectsWithoutIface()
    {
        _db.RegisterType(typeof(NodesOne));
        _db.RegisterType(typeof(NodesTwo));

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IGraphTable>("GraphTable");
            var table = creator(tr);
            var graph = new NodesGraph
            {
                Id = 1,
                Nodes = new NodesOne { A = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, F = "f" }
            };
            table.Insert(graph);
            graph = new NodesGraph
            {
                Id = 2,
                Nodes = new NodesTwo { B = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, E = "e" }
            };
            table.Insert(graph);
            graph = new NodesGraph
            {
                Id = 3,
                Nodes = new NodesBase()
            };
            table.Insert(graph);

            Assert.True(table.FindById(1).Nodes is NodesOne);
            Assert.True(table.FindById(2).Nodes is NodesTwo);

            table.RemoveById(1);
            table.RemoveById(2);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class EmailMessage
    {
        public IDictionary<string, string> Bcc { get; set; }
        public IDictionary<string, string> Cc { get; set; }
        public IDictionary<string, string> To { get; set; }
        public IOrderedSet<string> Tags { get; set; }
    }

    public class EmailDb
    {
        public EmailMessage Content { get; set; }
    }

    public class BatchDb
    {
        [PrimaryKey(1)] public Guid ItemId { get; set; }
        public IDictionary<Guid, EmailDb> MailPieces { get; set; }
    }

    public interface IBatchTable : IRelation<BatchDb>
    {
        void Insert(BatchDb batch);
        void Update(BatchDb batch);
        BatchDb FindByIdOrDefault(Guid itemId);
    }

    [Fact]
    public void ReplacingDictionaryValueFreesNestedDictionaries()
    {
        Func<IObjectDBTransaction, IBatchTable> creator = null;
        var guid = Guid.NewGuid();
        var mailGuid = Guid.NewGuid();

        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IBatchTable>("IBatchTable");
            var table = creator(tr);
            var batch = new BatchDb
            {
                ItemId = guid,
                MailPieces = new Dictionary<Guid, EmailDb>
                {
                    [mailGuid] = new EmailDb
                    {
                        Content = new EmailMessage
                        {
                            Bcc = new Dictionary<string, string> { ["a"] = "b" },
                            Cc = new Dictionary<string, string> { ["c"] = "d" },
                            To = new Dictionary<string, string> { ["e"] = "f" }
                        }
                    }
                }
            };
            table.Insert(batch);
            batch = table.FindByIdOrDefault(guid);
            batch.MailPieces[mailGuid].Content.Tags.Add("Important");
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            var batch = table.FindByIdOrDefault(guid);
            batch.MailPieces[mailGuid] =
                null; //the dictionary setter must free nested dictionaries from the replaced value
            table.Update(batch);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    void AssertNoLeaksInDb()
    {
        var leaks = FindLeaks();
        Assert.Equal("", leaks);
    }

    string FindLeaks()
    {
        using (var visitor = new FindUnusedKeysVisitor())
        {
            using (var tr = _db.StartReadOnlyTransaction())
            {
                visitor.ImportAllKeys(tr);
                visitor.Iterate(tr);
                return DumpUnseenKeys(visitor, " ");
            }
        }
    }

    static string DumpUnseenKeys(FindUnusedKeysVisitor visitor, string concat)
    {
        var builder = new StringBuilder();
        foreach (var unseenKey in visitor.UnseenKeys())
        {
            if (builder.Length > 0)
                builder.Append(concat);
            foreach (var b in unseenKey.Key)
                builder.Append(b.ToString("X2"));
            builder.Append(" Value len:");
            builder.Append(unseenKey.ValueSize);
        }

        return builder.ToString();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }
}
