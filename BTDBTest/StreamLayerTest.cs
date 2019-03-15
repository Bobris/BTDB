using System;
using System.IO;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest
{
    public class StreamLayerTest: IDisposable
    {
        const string TestFileName = "testfile.txt";
        byte[] buff = new byte[8];

        public StreamLayerTest()
        {
            if (!File.Exists(TestFileName))
            {
                using (var s = File.Create(TestFileName))
                {
                    s.WriteByte(11);
                }
            }
        }

        [Fact]
        public void ReadingStreamManaged()
        {
            using (var s = File.OpenRead(TestFileName))
            using (var p = new PositionLessStreamProxy(s, false))
            {
                Assert.Equal(1, p.Read(buff, 0, buff.Length, 0));
                Assert.Equal(11, buff[0]);
                Assert.Equal(0, p.Read(buff, 0, buff.Length, 1));
                Assert.Equal(0, p.Read(buff, 0, buff.Length, 1));
                Assert.Equal(0, p.Read(buff, 0, buff.Length, 10));
            }
        }

        [Fact]
        public void ReadingStreamNatively()
        {
            using (var s = File.OpenRead(TestFileName))
            using (var p = new PositionLessFileStreamProxy(s, false))
            {
                Assert.Equal(1, p.Read(buff, 0, buff.Length, 0));
                Assert.Equal(11, buff[0]);
                Assert.Equal(0, p.Read(buff, 0, buff.Length, 1));
                Assert.Equal(0, p.Read(buff, 0, buff.Length, 1));
                Assert.Equal(0, p.Read(buff, 0, buff.Length, 10));
            }
        }
        
        public void Dispose()
        {
            try
            {
                File.Delete(TestFileName);
            }
            catch
            {
                // ignored
            }
        }
    }
}