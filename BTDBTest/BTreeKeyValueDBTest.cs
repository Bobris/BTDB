using System;
using BTDB.Allocators;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class BTreeKeyValueDBTest : KeyValueDBTestBase, IDisposable
    {
        LeakDetectorWrapperAllocator _allocator;

        public BTreeKeyValueDBTest()
        {
            _allocator = new LeakDetectorWrapperAllocator(new MallocAllocator());
        }

        public void Dispose()
        {
            var leaks = _allocator.QueryAllocations();
            Assert.Equal(0ul, leaks.Count);
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection)
        {
            return NewKeyValueDB(new KeyValueDBOptions { FileCollection = fileCollection, Compression = new SnappyCompressionStrategy(), FileSplitSize = 2147483647});
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize = 2147483647)
        {
            return NewKeyValueDB(new KeyValueDBOptions { FileCollection = fileCollection, Compression = compression, FileSplitSize = fileSplitSize});
        }

        public override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize, ICompactorScheduler compactorScheduler)
        {
            return NewKeyValueDB(new KeyValueDBOptions { FileCollection = fileCollection, Compression = compression, FileSplitSize = fileSplitSize, CompactorScheduler = compactorScheduler});
        }

        public override IKeyValueDB NewKeyValueDB(KeyValueDBOptions options)
        {
            options.Allocator = _allocator;
            return new BTreeKeyValueDB(options);
        }
    }
}
