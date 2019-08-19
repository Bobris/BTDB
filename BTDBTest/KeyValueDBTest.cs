using BTDB.KVDBLayer;

namespace BTDBTest
{
    public class KeyValueDBTest : KeyValueDBTestBase
    {
        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection)
        {
            return new KeyValueDB(fileCollection);
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize = 2147483647)
        {
            return new KeyValueDB(fileCollection, compression, fileSplitSize);
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize, ICompactorScheduler compactorScheduler)
        {
            return new KeyValueDB(fileCollection, compression, fileSplitSize, compactorScheduler);
        }

        public override IKeyValueDB NewKeyValueDB(KeyValueDBOptions options)
        {
            return new KeyValueDB(options);
        }
    }
}
