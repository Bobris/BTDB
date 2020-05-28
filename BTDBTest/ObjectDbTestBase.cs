using System;
using BTDB.Encrypted;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit.Abstractions;

namespace BTDBTest
{
    public abstract class ObjectDbTestBase: IDisposable
    {
        protected readonly ITestOutputHelper _output;
        protected readonly IKeyValueDB _lowDb;
        protected IObjectDB _db;

        protected ObjectDbTestBase(ITestOutputHelper output)
        {
            _output = output;
            _lowDb = new BTreeKeyValueDB(new KeyValueDBOptions()
            {
                CompactorScheduler = null,
                Compression = new NoCompressionStrategy(),
                FileCollection = new InMemoryFileCollection()
            });
            OpenDb();
        }

        public void Dispose()
        {
            _db.Dispose();
            _lowDb.Dispose();
        }

        protected void ReopenDb()
        {
            _db.Dispose();
            OpenDb();
        }

        void OpenDb()
        {
            _db = new ObjectDB();
            _db.Open(_lowDb, false, new DBOptions()
                .WithoutAutoRegistration()
                .WithSymmetricCipher(new AesGcmSymmetricCipher(new byte[]
                {
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                    27, 28, 29, 30, 31
                })));
        }
    }
}