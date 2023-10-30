using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using BTDB.Bon;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableBonTest : ObjectDbTestBase
{
    public ObjectDbTableBonTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Document
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }

        [PrimaryKey(2)] public string Key { get; set; }

        public ReadOnlyMemory<byte> Value { get; set; }
    }

    public interface IDocumentTable : IRelation<Document>
    {
        IEnumerable<Document> ScanById(Constraint<ulong> tenantId, Constraint<string> key);
    }

    [Fact]
    public async Task EndToEndTest()
    {
        CreateSampleRecords();
        var ms = new MemoryStream();
        await WriteDocumentsTo(ms);
        ms.Position = 0;
        Assert.Equal(10012L, await ReadDocumentsFrom(ms));
    }

    async Task<long> ReadDocumentsFrom(Stream stream)
    {
        var blockForLength = new byte[4];
        var buffer = Array.Empty<byte>();
        var res = 0L;
        while (true)
        {
            await stream.ReadExactlyAsync(blockForLength);
            var len = BinaryPrimitives.ReadUInt32LittleEndian(blockForLength.AsSpan());
            if (len == 0) break;
            if (len > buffer.Length) buffer = new byte[len];
            await stream.ReadExactlyAsync(new(buffer, 0, (int)len));
            res += ExtractKeyValue(new(buffer, 0, (int)len));
        }

        return res;
    }

    static long ExtractKeyValue(ReadOnlyMemory<byte> buffer)
    {
        var reader = new SpanReader(buffer);
        var key = reader.ReadStringInUtf8();
        var bon = new Bon(new ReadOnlyMemoryMemReader(reader.ReadByteArrayAsMemory()));
        // From here it is just sample
        if (!bon.TryGetArray(out var bona)) throw new InvalidDataException();
        return key.Length + bona.Items;
    }

    async Task WriteDocumentsTo(Stream stream)
    {
        using var tr = _db.StartReadOnlyTransaction();
        var table = tr.GetRelation<IDocumentTable>();
        foreach (var document in table.ScanById(Constraint.Unsigned.Exact(1), Constraint.String.Any))
        {
            var block1 = WriteDocumentToBlock1(document);
            var block2 = document.Value;
            await stream.WriteAsync(block1);
            await stream.WriteAsync(block2);
        }

        // final zero length to mark end of enumerable
        await stream.WriteAsync(new byte[4]);
    }

    static ReadOnlyMemory<byte> WriteDocumentToBlock1(Document document)
    {
        var writer = new SpanWriter();
        writer.WriteUInt32LE(0); // Leave space for length
        writer.WriteStringInUtf8(document.Key);
        writer.WriteByteArrayLength(document.Value);
        var len = writer.NoControllerGetCurrentPosition();
        writer.NoControllerSetCurrentPosition(0);
        writer.WriteUInt32LE(len - 4 + (uint)document.Value.Length); // Overwrite by known length
        writer.NoControllerSetCurrentPosition(len);
        return writer.GetPersistentMemoryAndReset();
    }

    void CreateSampleRecords()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDocumentTable>();
        table.Upsert(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10000) });
        table.Upsert(new() { TenantId = 1, Key = "Second", Value = CreateSampleBonData(1) });
        tr.Commit();
    }

    static ReadOnlyMemory<byte> CreateSampleBonData(int size)
    {
        var bonBuilder = new BonBuilder();
        bonBuilder.StartArray();
        for (var id = 0; id < size; id++)
        {
            bonBuilder.StartObject();
            bonBuilder.WriteKey("Id");
            bonBuilder.Write(id);
            bonBuilder.WriteKey("Name");
            bonBuilder.Write("Bobris " + id);
            bonBuilder.FinishObject();
        }

        bonBuilder.FinishArray();
        return bonBuilder.FinishAsMemory();
    }
}
