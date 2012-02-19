using BTDB.Buffer;
using BTDB.KV2DBLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class KeyValue2DBTest
    {
        [Test]
        public void CreateEmptyDatabase()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (var db = new KeyValue2DB(fileCollection))
            {
            }
        }

        [Test]
        public void FirstTransaction()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValue2DB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void MoreComplexTransaction()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValue2DB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.False(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.AreEqual(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(_key2)));
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key2), ByteBuffer.NewAsync(new byte[0])));
                    Assert.AreEqual(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(_key1)));
                    Assert.AreEqual(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(_key2)));
                    Assert.AreEqual(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(_key3)));
                    Assert.AreEqual(FindResult.Next, tr.Find(ByteBuffer.NewEmpty()));
                    tr.Commit();
                }
            }
        }

        readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        readonly byte[] _key2 = new byte[] { 1, 3, 2 };
        readonly byte[] _key3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    }
}