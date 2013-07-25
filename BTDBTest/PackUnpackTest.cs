using System.Collections.Generic;
using BTDB.Buffer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class PackUnpackTest
    {

        [Test, TestCaseSource("GenerateULongs")]
        public void PackVUIntIsOrderable(ulong t)
        {
            var buf1 = new byte[9];
            var o1 = 0;
            PackUnpack.PackVUInt(buf1, ref o1, t - 1);
            var buf2 = new byte[9];
            var o2 = 0;
            PackUnpack.PackVUInt(buf2, ref o2, t);
            if (t <= uint.MaxValue)
            {
                Assert.AreEqual(o2, PackUnpack.LengthVUInt((uint)t));
            }
            Assert.AreEqual(o2, PackUnpack.LengthVUInt(t));
            Assert.AreEqual(o2, PackUnpack.LengthVUInt(buf2, 0));
            Assert.Greater(0, BitArrayManipulation.CompareByteArray(buf1, o1, buf2, o2));
            var o1A = 0;
            Assert.AreEqual(t - 1, PackUnpack.UnpackVUInt(buf1, ref o1A));
            Assert.AreEqual(o1, o1A);
            var o2A = 0;
            Assert.AreEqual(t, PackUnpack.UnpackVUInt(buf2, ref o2A));
            Assert.AreEqual(o2, o2A);
        }

        // ReSharper disable UnusedMember.Global
        public static IEnumerable<ulong> GenerateULongs()
        // ReSharper restore UnusedMember.Global
        {
            yield return 1;
            yield return 2;
            for (ulong t = 4; t != 0; t *= 2)
            {
                yield return t - 1;
                yield return t;
                yield return t + 1;
            }
            yield return ulong.MaxValue;
            yield return 123456789UL;
            yield return 123456789123456789UL;
        }

        [Test, TestCaseSource("GeneratePositiveLongs")]
        public void PackVIntIsOrderableForPositive(long t)
        {
            var buf1 = new byte[9];
            var o1 = 0;
            PackUnpack.PackVInt(buf1, ref o1, t - 1);
            var buf2 = new byte[9];
            var o2 = 0;
            PackUnpack.PackVInt(buf2, ref o2, t);
            if (t >= int.MinValue && t <= int.MaxValue)
            {
                Assert.AreEqual(o2, PackUnpack.LengthVInt((int)t));
            }
            Assert.AreEqual(o2, PackUnpack.LengthVInt(t));
            Assert.AreEqual(o2, PackUnpack.LengthVInt(buf2, 0));
            Assert.Greater(0, BitArrayManipulation.CompareByteArray(buf1, o1, buf2, o2));
            var o1A = 0;
            Assert.AreEqual(t - 1, PackUnpack.UnpackVInt(buf1, ref o1A));
            Assert.AreEqual(o1, o1A);
            var o2A = 0;
            Assert.AreEqual(t, PackUnpack.UnpackVInt(buf2, ref o2A));
            Assert.AreEqual(o2, o2A);
        }

        // ReSharper disable UnusedMember.Global
        public static IEnumerable<long> GeneratePositiveLongs()
        // ReSharper restore UnusedMember.Global
        {
            yield return 0;
            yield return 1;
            yield return 2;
            for (long t = 4; t > 0; t *= 2)
            {
                yield return t - 1;
                yield return t;
                yield return t + 1;
            }
            yield return long.MaxValue;
            yield return 123456789L;
            yield return 123456789123456789L;
        }

        [Test, TestCaseSource("GenerateNegativeLongs")]
        public void PackVIntIsOrderableForNegative(long t)
        {
            var buf1 = new byte[9];
            var o1 = 0;
            PackUnpack.PackVInt(buf1, ref o1, t - 1);
            var buf2 = new byte[9];
            var o2 = 0;
            PackUnpack.PackVInt(buf2, ref o2, t);
            if (t >= int.MinValue && t <= int.MaxValue)
            {
                Assert.AreEqual(o2, PackUnpack.LengthVInt((int)t));
            }
            Assert.AreEqual(o2, PackUnpack.LengthVInt(t));
            Assert.AreEqual(o2, PackUnpack.LengthVInt(buf2, 0));
            Assert.Greater(0, BitArrayManipulation.CompareByteArray(buf1, o1, buf2, o2), "{0} is not before {1}", t - 1, t);
            var o1A = 0;
            Assert.AreEqual(t - 1, PackUnpack.UnpackVInt(buf1, ref o1A));
            Assert.AreEqual(o1, o1A);
            var o2A = 0;
            Assert.AreEqual(t, PackUnpack.UnpackVInt(buf2, ref o2A));
            Assert.AreEqual(o2, o2A);
        }

        // ReSharper disable UnusedMember.Global
        public static IEnumerable<long> GenerateNegativeLongs()
        // ReSharper restore UnusedMember.Global
        {
            yield return -1;
            yield return -2;
            for (long t = -4; t - 1 < 0; t *= 2)
            {
                yield return t - 1;
                yield return t;
                yield return t + 1;
            }
            yield return long.MinValue + 1;
            yield return -123456789L;
            yield return -123456789123456789L;
        }

    }
}