using BTDB.Allocators;

namespace BTDB.KVDBLayer
{
    public class KeyValueDBOptions
    {
        public IFileCollection FileCollection;
        public ICompressionStrategy Compression = new SnappyCompressionStrategy();
        public uint FileSplitSize = int.MaxValue;
        public ICompactorScheduler CompactorScheduler = KVDBLayer.CompactorScheduler.Instance;
        public ulong? OpenUpToCommitUlong;
        public ulong? PreserveHistoryUpToCommitUlong;
        public ulong? CompactorReadBytesPerSecondLimit;
        public ulong? CompactorWriteBytesPerSecondLimit;
        public IOffHeapAllocator Allocator;
    }
}
