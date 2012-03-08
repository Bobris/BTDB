using BTDB.Buffer;
using NUnit.Framework;
using BTDB.SnappyCompression;

namespace BTDBTest
{
    [TestFixture]
    public class SnappyTest
    {
        [Test]
        public void Basic()
        {
            var s = new byte[] { 1, 2, 3, 1, 2, 3, 1, 2, 3 };
            var t = new byte[10];
            var r = SnappyCompress.Compress(ByteBuffer.NewSync(t), ByteBuffer.NewSync(s));
            Assert.AreEqual(7, r);
            var d = SnappyDecompress.Decompress(ByteBuffer.NewSync(t, 0, r));
            Assert.AreEqual(s, d.ToArraySegment());
        }
    }
}
