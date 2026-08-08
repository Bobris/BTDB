using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class LazyUlongListTest : IDisposable
{
    readonly IKeyValueDB _lowDb = new InMemoryKeyValueDB();
    IObjectDB _db;

    public LazyUlongListTest()
    {
        _db = OpenDb();
    }

    public class Row
    {
        [PrimaryKey] public ulong Id { get; set; }
        public ILazyUlongList Values { get; set; }
    }

    public interface IRows : IRelation<Row>
    {
        void Insert(Row row);
        bool RemoveById(ulong id);
        bool ShallowRemoveById(ulong id);
        Row FindById(ulong id);
    }

    [Fact]
    public void AddPersistsOrderedValuesAcrossRecordBoundary()
    {
        var values = Enumerable.Range(0, 5000).Select(i => (i % 5) switch
        {
            0 => (ulong)i,
            1 => ulong.MaxValue - (ulong)i,
            2 => 42ul,
            3 => (ulong)(5000 - i),
            _ => 0ul
        }).ToList();
        var creator = CreateRow(values, out _);

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var list = creator(tr).FindById(1).Values;
            Assert.Equal((ulong)values.Count, list.Count);
            Assert.Equal(values, list.ToList());
            list.Add(ulong.MaxValue);
            list.Add(7);
            list.Flush();
            tr.Commit();
        }

        values.Add(ulong.MaxValue);
        values.Add(7);
        ReopenDb();
        using var readTr = _db.StartTransaction();
        Assert.Equal(values, creator(readTr).FindById(1).Values.ToList());
    }

    [Fact]
    public async Task BuildAsyncStreamsRecordsAndStoresAtMost4096ValuesInEach()
    {
        var creator = CreateRow([123], out var listId);
        var values = Enumerable.Range(0, 9000).Select(i => (i % 4) switch
        {
            0 => ulong.MaxValue - (ulong)i,
            1 => 0ul,
            2 => (ulong)i,
            _ => 17ul
        }).ToArray();
        var appliedChunks = 0;

        await LazyUlongList.BuildAsync(values, (commands, _) =>
        {
            using var tr = _db.StartTransaction();
            creator(tr).FindById(1).Values.ApplyCommands(commands);
            tr.Commit();
            appliedChunks++;
            return Task.CompletedTask;
        }, new byte[LazyUlongList.MaxRecordPayloadLength + 12]);

        Assert.True(appliedChunks > 1);
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var list = creator(tr).FindById(1).Values;
            Assert.True(list.IsComplete());
            Assert.Equal(9000ul, list.Count);
            Assert.Equal(values, list.ToArray());
        }

        Assert.Equal([4096, 4096, 808], StoredRecordCounts(listId));
        AssertFirstRecordUsesVUIntAndVIntDeltas(listId, values);
    }

    [Fact]
    public async Task BuildAsyncSupportsWorstCaseRecordSize()
    {
        var values = new ulong[LazyUlongList.ValuesPerRecord];
        for (var i = 0; i < values.Length; i++)
            values[i] = (i & 1) == 0 ? ulong.MaxValue : long.MaxValue;
        var chunks = new List<byte[]>();

        await LazyUlongList.BuildAsync(values, (commands, _) =>
        {
            chunks.Add(commands.ToArray());
            return Task.CompletedTask;
        }, new byte[LazyUlongList.MaxRecordPayloadLength + 12]);

        Assert.Equal([1, LazyUlongList.MaxRecordPayloadLength + 4, 3], chunks.Select(chunk => chunk.Length));
        Assert.Equal(LazyUlongList.MaxRecordPayloadLength,
            BinaryPrimitives.ReadUInt16LittleEndian(chunks[1].AsSpan(2)));
    }

    [Fact]
    public void ClearAndRelationRemovalDeleteExternalContent()
    {
        var creator = CreateRow([1, 2, 3], out var firstListId);
        using (var tr = _db.StartTransaction())
        {
            creator(tr).FindById(1).Values.Clear();
            tr.Commit();
        }

        Assert.False(HasExternalContent(firstListId));
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var list = creator(tr).FindById(1).Values;
            Assert.Equal(0ul, list.Count);
            Assert.Empty(list);
        }

        ulong secondListId;
        using (var tr = _db.StartTransaction())
        {
            var list = CreateList(tr, [10, 20]);
            secondListId = list.Id;
            creator(tr).Insert(new Row { Id = 2, Values = list });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.True(creator(tr).RemoveById(2));
            tr.Commit();
        }

        Assert.False(HasExternalContent(secondListId));
    }

    [Fact]
    public void ApplyCommandsRejectsUnflushedAdds()
    {
        var creator = CreateRow([1], out _);
        using var tr = _db.StartTransaction();
        var list = creator(tr).FindById(1).Values;
        list.Add(2);
        Assert.Throws<InvalidOperationException>(() => list.ApplyCommands(new byte[] { 0 }));
    }

    [Fact]
    public void EnumerateFromIndexStartsInsideRequestedRecord()
    {
        var values = Enumerable.Range(0, 9000).Select(i => (ulong)(i * 3)).ToList();
        var creator = CreateRow(values, out _);
        using var tr = _db.StartTransaction();
        var list = creator(tr).FindById(1).Values;
        Assert.Equal(values, list.EnumerateFromIndex(0));
        Assert.Equal(values.Skip(4094), list.EnumerateFromIndex(4094));
        Assert.Equal(values.Skip(8192), list.EnumerateFromIndex(8192));
        Assert.Empty(list.EnumerateFromIndex((ulong)values.Count));
        Assert.Empty(list.EnumerateFromIndex((ulong)values.Count + 1000));

        list.Add(123456);
        Assert.Equal([123456ul], list.EnumerateFromIndex((ulong)values.Count));
    }

    [Fact]
    public void IsCompleteUsesCountRecordAsCompletionMarker()
    {
        using var tr = _db.StartTransaction();
        var list = new ODBLazyUlongList((IInternalObjectDBTransaction)tr);
        Assert.True(list.IsComplete());
        list.ApplyCommands(new byte[] { 1, 0, 1, 0, 42 });
        Assert.False(list.IsComplete());
        Assert.Throws<BTDBException>(() => list.Count);
        list.ApplyCommands(new byte[] { 2, 1 });
        Assert.True(list.IsComplete());
    }

    Func<IObjectDBTransaction, IRows> CreateRow(IEnumerable<ulong> values, out ulong listId)
    {
        using var tr = _db.StartTransaction();
        var creator = tr.InitRelation<IRows>("LazyUlongListRows");
        var list = CreateList(tr, values);
        listId = list.Id;
        creator(tr).Insert(new Row { Id = 1, Values = list });
        tr.Commit();
        return creator;
    }

    static ODBLazyUlongList CreateList(IObjectDBTransaction tr, IEnumerable<ulong> values)
    {
        var list = new ODBLazyUlongList((IInternalObjectDBTransaction)tr);
        foreach (var value in values)
            list.Add(value);
        list.Flush();
        return list;
    }

    List<int> StoredRecordCounts(ulong listId)
    {
        var prefix = ExternalContentPrefix(listId);
        using var tr = _lowDb.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Memory<byte> keyBuffer = new byte[32];
        Memory<byte> valueBuffer = new byte[LazyUlongList.MaxRecordPayloadLength];
        var result = new List<int>();
        while (cursor.FindNextKey(prefix))
        {
            if (cursor.GetKeyMemory(ref keyBuffer).Length == prefix.Length)
                continue;
            var reader = MemReader.CreateFromReadOnlyMemory(cursor.GetValueMemory(ref valueBuffer, copy: true));
            var count = 0;
            while (!reader.Eof)
            {
                if (count++ == 0)
                    reader.ReadVUInt64();
                else
                    reader.ReadVInt64();
            }

            reader.Dispose();
            result.Add(count);
        }

        return result;
    }

    void AssertFirstRecordUsesVUIntAndVIntDeltas(ulong listId, ulong[] values)
    {
        var prefix = ExternalContentPrefix(listId);
        using var tr = _lowDb.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindNextKey(prefix));
        Memory<byte> keyBuffer = new byte[32];
        if (cursor.GetKeyMemory(ref keyBuffer).Length == prefix.Length)
            Assert.True(cursor.FindNextKey(prefix));
        Memory<byte> valueBuffer = new byte[LazyUlongList.MaxRecordPayloadLength];
        var reader = MemReader.CreateFromReadOnlyMemory(cursor.GetValueMemory(ref valueBuffer, copy: true));
        Assert.Equal(values[0], reader.ReadVUInt64());
        Assert.Equal(unchecked((long)(values[1] - values[0])), reader.ReadVInt64());
        Assert.Equal(unchecked((long)(values[2] - values[1])), reader.ReadVInt64());
        reader.Dispose();
    }

    bool HasExternalContent(ulong id)
    {
        var prefix = ExternalContentPrefix(id);
        using var tr = _lowDb.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        return cursor.FindNextKey(prefix);
    }

    static byte[] ExternalContentPrefix(ulong id)
    {
        var length = PackUnpack.LengthVUInt(id);
        var prefix = new byte[ObjectDB.AllDictionariesPrefixLen + length];
        prefix[0] = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(ref prefix[ObjectDB.AllDictionariesPrefixLen], id, length);
        return prefix;
    }

    IObjectDB OpenDb()
    {
        var db = new ObjectDB();
        db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration());
        return db;
    }

    void ReopenDb()
    {
        _db.Dispose();
        _db = OpenDb();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }
}
