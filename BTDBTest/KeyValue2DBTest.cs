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
                    tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(new byte[] {0}), ByteBuffer.NewAsync(new byte[] {0}));
                    tr.Commit();
                }
            }
        }
        
    }
}