using System;
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

        [Test]
        public void RandomData([Values(1, 2, 3, 4, 5, 10, 100, 1000, 10000, 100000, 1000000, 10000000)] int len)
        {
            var s = new byte[len];
            new Random(1234567).NextBytes(s);
            RoundTrip(s);
        }

        [Test]
        public void ConstantData([Values(1, 2, 3, 4, 5, 10, 100, 1000, 10000, 100000, 1000000, 10000000)] int len)
        {
            var s = new byte[len];
            RoundTrip(s);
        }

        [Test]
        public void RepeatingData([Values(2, 3, 4, 5, 6, 7, 8, 15, 30, 60, 120, 240, 255)] int len)
        {
            var s = new byte[len * 10];
            for (int i = 0; i < s.Length; i++) s[i] = (byte)(i % len);
            RoundTrip(s);
            s = new byte[len * 2];
            for (int i = 0; i < s.Length; i++) s[i] = (byte)(i % len);
            RoundTrip(s);
            s = new byte[len * 2 + len / 2];
            for (int i = 0; i < s.Length; i++) s[i] = (byte)(i % len);
            RoundTrip(s);
        }

        void RoundTrip(byte[] source)
        {
            var compressed = new byte[(long)source.Length * 6 / 5 + 32];
            var compressedLength = SnappyCompress.Compress(ByteBuffer.NewSync(compressed), ByteBuffer.NewSync(source));
            var decompressed = SnappyDecompress.Decompress(ByteBuffer.NewSync(compressed, 0, compressedLength));
            Assert.AreEqual(source, decompressed.ToArraySegment());
            compressed = new byte[compressedLength / 2];
            Assert.AreEqual(-1, SnappyCompress.Compress(ByteBuffer.NewSync(compressed), ByteBuffer.NewSync(source)));
        }
    }
}
