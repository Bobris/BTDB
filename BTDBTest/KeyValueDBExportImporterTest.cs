using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class KeyValueDBExportImporterTest
{
    static IKeyValueDB Create(bool inMemory) => inMemory
        ? new InMemoryKeyValueDB()
        : new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = new InMemoryFileCollection(), Compression = new NoCompressionStrategy(),
            CompactorScheduler = null
        });

    static List<(byte[] Key, byte[] Value)> FillRandom(IKeyValueDB db, int seed, ulong commitUlong, uint ulongCount)
    {
        var rnd = new Random(seed);
        var model = new SortedDictionary<byte[], byte[]>(Comparer<byte[]>.Create((a, b) =>
            a.AsSpan().SequenceCompareTo(b)));
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        for (var i = 0; i < 300; i++)
        {
            // Include keys and values bigger than import buffers and 512 bytes stack buffer of Export
            var key = new byte[rnd.Next(10) == 0 ? 5000 + rnd.Next(3000) : rnd.Next(1, 20)];
            rnd.NextBytes(key);
            var value = new byte[rnd.Next(10) == 0 ? 4097 + rnd.Next(5000) : rnd.Next(40)];
            rnd.NextBytes(value);
            cursor.CreateOrUpdateKeyValue(key, value);
            model[key] = value;
        }

        tr.SetCommitUlong(commitUlong);
        for (var i = 0u; i < ulongCount; i++) tr.SetUlong(i, i * 1000 + 1);
        tr.Commit();
        var res = new List<(byte[], byte[])>();
        foreach (var kv in model) res.Add((kv.Key, kv.Value));
        return res;
    }

    static void AssertContent(IKeyValueDB db, List<(byte[] Key, byte[] Value)> expected, ulong commitUlong,
        uint ulongCount)
    {
        using var tr = db.StartReadOnlyTransaction();
        Assert.Equal(expected.Count, tr.GetKeyValueCount());
        Assert.Equal(commitUlong, tr.GetCommitUlong());
        for (var i = 0u; i < ulongCount; i++) Assert.Equal(i * 1000 + 1, tr.GetUlong(i));
        using var cursor = tr.CreateCursor();
        var idx = 0;
        while (cursor.FindNextKey([]))
        {
            Assert.Equal(expected[idx].Key, cursor.SlowGetKey());
            Assert.Equal(expected[idx].Value, cursor.SlowGetValue());
            idx++;
        }

        Assert.Equal(expected.Count, idx);
    }

    [Theory]
    [InlineData(true, false, false, 0ul, 0u)]
    [InlineData(true, true, true, 0ul, 0u)]
    [InlineData(false, false, true, 42ul, 0u)]
    [InlineData(false, true, false, 0ul, 3u)]
    [InlineData(true, true, false, 7ul, 5u)]
    [InlineData(false, false, false, 7ul, 5u)]
    public async Task ExportImportRoundTrip(bool sourceInMemory, bool asyncExport, bool asyncImport,
        ulong commitUlong, uint ulongCount)
    {
        using var source = Create(sourceInMemory);
        var expected = FillRandom(source, (int)commitUlong + (int)ulongCount, commitUlong, ulongCount);
        var stream = new MemoryStream();
        using (var tr = source.StartReadOnlyTransaction())
        {
            if (asyncExport) await KeyValueDBExportImporter.ExportAsync(tr, stream);
            else KeyValueDBExportImporter.Export(tr, stream);
        }

        stream.Position = 0;
        using var target = Create(!sourceInMemory);
        using (var tr = target.StartTransaction())
        {
            if (asyncImport) await KeyValueDBExportImporter.ImportAsync(tr, stream);
            else KeyValueDBExportImporter.Import(tr, stream);
            tr.Commit();
        }

        AssertContent(target, expected, commitUlong, ulongCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ImportWorksWithStreamReturningPartialReads(bool async)
    {
        using var source = Create(true);
        var expected = FillRandom(source, 1, 11, 2);
        var stream = new MemoryStream();
        using (var tr = source.StartReadOnlyTransaction())
        {
            KeyValueDBExportImporter.Export(tr, stream);
        }

        using var target = Create(false);
        using (var tr = target.StartTransaction())
        {
            var trickle = new TrickleStream(stream.ToArray());
            if (async) await KeyValueDBExportImporter.ImportAsync(tr, trickle);
            else KeyValueDBExportImporter.Import(tr, trickle);
            tr.Commit();
        }

        AssertContent(target, expected, 11, 2);
    }

    [Fact]
    public async Task ExportOfEmptyDbWritesOnlyHeader()
    {
        using var db = Create(false);
        using var tr = db.StartReadOnlyTransaction();
        var stream = new MemoryStream();
        KeyValueDBExportImporter.Export(tr, stream);
        Assert.Equal(16, stream.Length);
        var stream2 = new MemoryStream();
        await KeyValueDBExportImporter.ExportAsync(tr, stream2);
        Assert.Equal(stream.ToArray(), stream2.ToArray());
        Assert.Equal("BTDBEXP2"u8.ToArray(), stream.ToArray()[..8]);
        Assert.Equal(0, PackUnpack.UnpackInt64LE(stream.ToArray(), 8));
    }

    [Fact]
    public async Task ArgumentsAreValidated()
    {
        using var db = Create(true);
        using var tr = db.StartTransaction();
        Assert.Throws<ArgumentNullException>(() => KeyValueDBExportImporter.Export(null!, new MemoryStream()));
        Assert.Throws<ArgumentNullException>(() => KeyValueDBExportImporter.Export(tr, null!));
        Assert.Throws<ArgumentException>(() => KeyValueDBExportImporter.Export(tr, new MemoryStream([], false)));
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            KeyValueDBExportImporter.ExportAsync(null!, new MemoryStream()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => KeyValueDBExportImporter.ExportAsync(tr, null!));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            KeyValueDBExportImporter.ExportAsync(tr, new MemoryStream([], false)));
        var notReadable = new NotReadableStream();
        Assert.Throws<ArgumentNullException>(() => KeyValueDBExportImporter.Import(null!, new MemoryStream()));
        Assert.Throws<ArgumentNullException>(() => KeyValueDBExportImporter.Import(tr, null!));
        Assert.Throws<ArgumentException>(() => KeyValueDBExportImporter.Import(tr, notReadable));
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            KeyValueDBExportImporter.ImportAsync(null!, new MemoryStream()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => KeyValueDBExportImporter.ImportAsync(tr, null!));
        await Assert.ThrowsAsync<ArgumentException>(() => KeyValueDBExportImporter.ImportAsync(tr, notReadable));
    }

    static byte[] Header(long count)
    {
        var res = new byte[16];
        "BTDBEXP2"u8.CopyTo(res);
        PackUnpack.PackInt64LE(res, 8, count);
        return res;
    }

    static byte[] Concat(params byte[][] parts)
    {
        var ms = new MemoryStream();
        foreach (var p in parts) ms.Write(p);
        return ms.ToArray();
    }

    static byte[] Int32(int v)
    {
        var res = new byte[4];
        PackUnpack.PackInt32LE(res, 0, v);
        return res;
    }

    public static IEnumerable<object[]> InvalidInputs()
    {
        yield return [new byte[10], typeof(EndOfStreamException)];
        yield return [Concat("BTDBEXP1"u8.ToArray(), new byte[8]), typeof(BTDBException)];
        yield return [Header(-1), typeof(BTDBException)];
        yield return [Header(1), typeof(EndOfStreamException)];
        yield return [Concat(Header(1), Int32(-1)), typeof(BTDBException)];
        yield return [Concat(Header(1), Int32(5), [1, 2]), typeof(EndOfStreamException)];
        yield return [Concat(Header(1), Int32(5000), new byte[100]), typeof(EndOfStreamException)];
        yield return [Concat(Header(1), Int32(1), [1]), typeof(EndOfStreamException)];
        yield return [Concat(Header(1), Int32(1), [1], Int32(-5)), typeof(BTDBException)];
        yield return [Concat(Header(1), Int32(1), [1], Int32(3), [1]), typeof(EndOfStreamException)];
        yield return [Concat(Header(1), Int32(1), [1], Int32(5000), [1]), typeof(EndOfStreamException)];
        // commit ulong and ulong count present, but ulong values truncated
        yield return [Concat(Header(0), new byte[8], Int32(2), new byte[8], new byte[3]), typeof(EndOfStreamException)];
    }

    [Theory]
    [MemberData(nameof(InvalidInputs))]
    public async Task InvalidInputThrows(byte[] data, Type exceptionType)
    {
        using var db = Create(true);
        using var tr = db.StartTransaction();
        Assert.Throws(exceptionType, () => KeyValueDBExportImporter.Import(tr, new MemoryStream(data)));
        await Assert.ThrowsAsync(exceptionType, () => KeyValueDBExportImporter.ImportAsync(tr, new MemoryStream(data)));
    }

    /// A key or value size far beyond what the stream holds, as in a damaged export, fails at the end of the stream
    /// without allocating the declared size.
    [Theory]
    [InlineData(true, 1 << 30)]
    [InlineData(false, 1 << 30)]
    [InlineData(true, int.MaxValue)]
    [InlineData(false, int.MaxValue)]
    public async Task SizesBeyondTheStreamFailWithoutAllocatingThem(bool key, int declared)
    {
        var data = key
            ? Concat(Header(1), Int32(declared), new byte[100])
            : Concat(Header(1), Int32(1), [1], Int32(declared), new byte[100]);
        using var db = Create(true);
        using var tr = db.StartTransaction();
        var before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Throws<EndOfStreamException>(() => KeyValueDBExportImporter.Import(tr, new MemoryStream(data)));
        Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 1 << 20);
        before = GC.GetAllocatedBytesForCurrentThread();
        await Assert.ThrowsAsync<EndOfStreamException>(() => KeyValueDBExportImporter.ImportAsync(tr, new MemoryStream(data)));
        Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 1 << 20);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OptionalTrailingPartsCanBeMissing(bool async)
    {
        // Only commit ulong without ulongs, and partial ulong count is ignored
        var data = Concat(Header(1), Int32(1), [7], Int32(1), [8], BitConverter.GetBytes(99ul), [1, 2]);
        using var db = Create(true);
        using var tr = db.StartTransaction();
        if (async) await KeyValueDBExportImporter.ImportAsync(tr, new MemoryStream(data));
        else KeyValueDBExportImporter.Import(tr, new MemoryStream(data));
        Assert.Equal(99ul, tr.GetCommitUlong());
        Assert.Equal(0u, tr.GetUlongCount());
        Assert.Equal(1, tr.GetKeyValueCount());
    }

    // Stream which returns at most 3 bytes from each read, it is still valid Stream behavior
    sealed class TrickleStream(byte[] data) : Stream
    {
        int _pos;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            var len = Math.Min(Math.Min(3, buffer.Length), data.Length - _pos);
            data.AsSpan(_pos, len).CopyTo(buffer);
            _pos += len;
            return len;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(Read(buffer.Span));
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return Task.FromResult(Read(buffer.AsSpan(offset, count)));
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    sealed class NotReadableStream : MemoryStream
    {
        public override bool CanRead => false;
    }
}
