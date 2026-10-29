using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class ChunkStorageTest : IDisposable
{
    readonly ThreadLocal<HashAlgorithm> _hashAlg = new(SHA1.Create);

    readonly IFileCollection _fileCollection;
    BTreeKeyValueDB _kvdb;
    IChunkStorage _cs;

    ByteBuffer CalcHash(byte[] bytes)
    {
        return ByteBuffer.NewAsync(_hashAlg.Value.ComputeHash(bytes));
    }

    public ChunkStorageTest()
    {
        _fileCollection = new InMemoryFileCollection();
        _kvdb = new BTreeKeyValueDB(_fileCollection);
        _cs = _kvdb.GetSubDB<IChunkStorage>(1)!;
    }

    public void Dispose()
    {
        _kvdb.Dispose();
        _fileCollection.Dispose();
    }

    void ReopenStorage()
    {
        _kvdb.Dispose();
        _kvdb = new BTreeKeyValueDB(_fileCollection);
        _cs = _kvdb.GetSubDB<IChunkStorage>(1);
    }

    [Fact]
    public void CreateEmptyStorage()
    {
        Assert.NotNull(_cs);
    }

    [Fact]
    public void CreateTransaction()
    {
        using (var tr = _cs.StartTransaction())
        {
            Assert.NotNull(tr);
        }
    }

    [Fact]
    public async Task WhatIPutICanGetInSameTransaction()
    {
        using (var tr = _cs.StartTransaction())
        {
            tr.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }), true);
            Assert.Equal(new byte[] { 1 }, (await tr.Get(CalcHash(new byte[] { 0 }))).ToByteArray());
        }
    }

    [Fact]
    public async Task WhatIPutICanGetInNextTransaction()
    {
        using (var tr = _cs.StartTransaction())
        {
            tr.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }), true);
        }

        using (var tr = _cs.StartTransaction())
        {
            Assert.Equal(new byte[] { 1 }, (await tr.Get(CalcHash(new byte[] { 0 }))).ToByteArray());
        }
    }

    [Fact]
    public async Task ItRemembersContentAfterReopen()
    {
        using (var tr = _cs.StartTransaction())
        {
            tr.Put(CalcHash([0]), ByteBuffer.NewAsync([1]), true);
        }

        ReopenStorage();
        using (var tr = _cs.StartTransaction())
        {
            Assert.Equal([1], (await tr.Get(CalcHash([0]))).ToByteArray());
        }
    }

    [Fact]
    public async Task ItRemembersContentStoredBeforeAndAfterAReopen()
    {
        using (var tr = _cs.StartTransaction())
            tr.Put(CalcHash([0]), ByteBuffer.NewAsync([1]), true);
        ReopenStorage();
        using (var tr = _cs.StartTransaction())
            tr.Put(CalcHash([2]), ByteBuffer.NewAsync([3]), true);
        ReopenStorage();
        using (var tr = _cs.StartTransaction())
        {
            Assert.Equal([3], (await tr.Get(CalcHash([2]))).ToByteArray());
            Assert.Equal([1], (await tr.Get(CalcHash([0]))).ToByteArray());
        }
    }

    [Fact]
    public async Task ItKeepsContentStoredInSeveralTransactionsAcrossReopen()
    {
        for (byte i = 0; i < 3; i++)
            using (var tr = _cs.StartTransaction())
                tr.Put(CalcHash([i]), ByteBuffer.NewAsync([i, i]), true);
        using (var tr = _cs.StartTransaction())
            for (byte i = 0; i < 3; i++)
                Assert.Equal([i, i], (await tr.Get(CalcHash([i]))).ToByteArray());
        ReopenStorage();
        using (var tr = _cs.StartTransaction())
            for (byte i = 0; i < 3; i++)
                Assert.Equal([i, i], (await tr.Get(CalcHash([i]))).ToByteArray());
    }

    [Fact]
    public async Task OnDiskItKeepsContentStoredInSeveralTransactionsAcrossReopen()
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-chunks-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            for (var round = 0; round < 2; round++)
            {
                using var files = new OnDiskFileCollection(directory);
                using var db = new BTreeKeyValueDB(files);
                var storage = db.GetSubDB<IChunkStorage>(1);
                for (var i = (byte)(round * 3); i < round * 3 + 3; i++)
                    using (var tr = storage.StartTransaction())
                        tr.Put(CalcHash([i]), ByteBuffer.NewAsync([i, i]), true);
                using (var tr = storage.StartTransaction())
                    for (byte i = 0; i < round * 3 + 3; i++)
                        Assert.Equal([i, i], (await tr.Get(CalcHash([i]))).ToByteArray());
            }

            using (var files = new OnDiskFileCollection(directory))
            using (var db = new BTreeKeyValueDB(files))
            using (var tr = db.GetSubDB<IChunkStorage>(1).StartTransaction())
                for (byte i = 0; i < 6; i++)
                    Assert.Equal([i, i], (await tr.Get(CalcHash([i]))).ToByteArray());
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }
}
