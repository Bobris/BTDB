using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using BTDB.Buffer;
using BTDB.ChunkCache;
using BTDB.KVDBLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class ChunkStorageTest
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

        [OneTimeSetUp]
        public void CreateChunkStorageSubDB()
        {
            _fileCollection = new InMemoryFileCollection();
            _kvdb = new KeyValueDB(_fileCollection);
            _cs = _kvdb.GetSubDB<IChunkStorage>(1);
        }

        [OneTimeTearDown]
        public void DestroyChunkStorageSubDB()
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

        [Test]
        public void CreateEmptyStorage()
        {
            Assert.NotNull(_cs);
        }

        [Test]
        public void CreateTransaction()
        {
            using (var tr = _cs.StartTransaction())
            {
                Assert.NotNull(tr);
            }
        }

        [Test]
        public void WhatIPutICanGetInSameTransaction()
        {
            using (var tr = _cs.StartTransaction())
            {
                tr.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }), true);
                Assert.AreEqual(new byte[] { 1 }, tr.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
            }
        }

        [Test]
        public void WhatIPutICanGetInNextTransaction()
        {
            using (var tr = _cs.StartTransaction())
            {
                tr.Put(CalcHash(new byte[] {0}), ByteBuffer.NewAsync(new byte[] {1}), true);
            }
            using (var tr = _cs.StartTransaction())
            {
                Assert.AreEqual(new byte[] { 1 }, tr.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
            }
        }

        [Test]
        public void ItRemebersContentAfterReopen()
        {
            using (var tr = _cs.StartTransaction())
            {
                tr.Put(CalcHash(new byte[] { 0 }), ByteBuffer.NewAsync(new byte[] { 1 }), true);
            }
            ReopenStorage();
            using (var tr = _cs.StartTransaction())
            {
                Assert.AreEqual(new byte[] { 1 }, tr.Get(CalcHash(new byte[] { 0 })).Result.ToByteArray());
            }
        }
    }
}