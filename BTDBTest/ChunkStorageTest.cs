using System;
using System.Security.Cryptography;
using System.Threading;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class ChunkStorageTest : IDisposable
    {
        readonly ThreadLocal<HashAlgorithm> _hashAlg =
            new ThreadLocal<HashAlgorithm>(() => new SHA1CryptoServiceProvider());

        IFileCollection _fileCollection;
        KeyValueDB _kvdb;
        IChunkStorage _cs;

        ByteBuffer CalcHash(byte[] bytes)
        {
            return ByteBuffer.NewAsync(_hashAlg.Value.ComputeHash(bytes));
        }

        public ChunkStorageTest()
        {
            _fileCollection = new InMemoryFileCollection();
            _kvdb = new KeyValueDB(_fileCollection);
            _cs = _kvdb.GetSubDB<IChunkStorage>(1);
        }

        public void Dispose()
        {
            _kvdb.Dispose();
            _fileCollection.Dispose();
        }

        void ReopenStorage()
        {
            _kvdb.Dispose();
            _kvdb = new KeyValueDB(_fileCollection);
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
        public void WhatIPutICanGetInSameTransaction()
        {
            using (var tr = _cs.StartTransaction())
            {
                tr.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }), true);
                Assert.Equal(new byte[] { 1 }, tr.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
            }
        }

        [Fact]
        public void WhatIPutICanGetInNextTransaction()
        {
            using (var tr = _cs.StartTransaction())
            {
                tr.Put(CalcHash(new byte[] {0}), ByteBuffer.NewAsync(new byte[] {1}), true);
            }
            using (var tr = _cs.StartTransaction())
            {
                Assert.Equal(new byte[] { 1 }, tr.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
            }
        }

        [Fact]
        public void ItRemebersContentAfterReopen()
        {
            using (var tr = _cs.StartTransaction())
            {
                tr.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }), true);
            }
            ReopenStorage();
            using (var tr = _cs.StartTransaction())
            {
                Assert.Equal(new byte[] { 1 }, tr.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
            }
        }
    }
}