using BTDB.Allocators;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class BTreeKeyValueDBTest : KeyValueDBTestBase
    {
        LeakDetectorWrapperAllocator _allocator;

        public BTreeKeyValueDBTest()
        {
            _allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
        }

        public void Dispose()
        {
            var leaks = _allocator.QueryAllocations();
            Assert.Equal(0ul, leaks.Count);
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection)
        {
            return new BTreeKeyValueDB(fileCollection);
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize = 2147483647)
        {
            return new BTreeKeyValueDB(fileCollection, compression, fileSplitSize);
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize, ICompactorScheduler compactorScheduler)
        {
            return new BTreeKeyValueDB(fileCollection, compression, fileSplitSize, compactorScheduler);
        }

        public override IKeyValueDB NewKeyValueDB(KeyValueDBOptions options)
        {
            options.Allocator = _allocator;
            return new BTreeKeyValueDB(options);
        }
    }
}
