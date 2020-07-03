using System;
using System.IO;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest
{
    public class FileCollectionTests : IDisposable
    {
        string _dir;

        public FileCollectionTests()
        {
            _dir = Path.GetTempPath() + "/deleteMe";
            Directory.CreateDirectory(_dir);
        }

        [Fact]
        public void CanCreateFile()
        {
            using (var dc = new OnDiskFileCollection(_dir))
            {
                var f = dc.AddFile("kvi");
                var w = f.GetAppenderWriter();
                var writer = new SpanWriter(w);
                for (var i = 0; i < 32000; i++)
                {
                    writer.WriteInt32LE(i);
                }
                writer.Sync();

                var data = new byte[4];
                for (var i = 0; i < 32000; i++)
                {
                    f.RandomRead(data, (ulong) i * 4, false);
                    Assert.Equal(i, PackUnpack.UnpackInt32LE(data, 0));
                }
            }
        }

        public void Dispose()
        {
            Directory.Delete(_dir, true);
        }
    }
}
