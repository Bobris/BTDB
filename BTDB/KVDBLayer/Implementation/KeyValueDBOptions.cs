using BTDB.Allocators;

namespace BTDB.KVDBLayer;

public class KeyValueDBOptions
{
    public IFileCollection? FileCollection;
    public ICompressionStrategy? Compression = new SnappyCompressionStrategy();
    public IKviCompressionStrategy KviCompressionStrategy = new DefaultCompressionKviStrategy();
    public uint FileSplitSize = int.MaxValue;
    public ICompactorScheduler? CompactorScheduler = KVDBLayer.CompactorScheduler.Instance;
    public IKeyValueDBLogger? Logger;
    public ulong? OpenUpToCommitUlong;
    public ulong? PreserveHistoryUpToCommitUlong;
    public ulong? CompactorReadBytesPerSecondLimit;
    public ulong? CompactorWriteBytesPerSecondLimit;
    public IOffHeapAllocator? Allocator;
    public bool ReadOnly;

    /// If true it will try to recover data in DB as much as possible
    public bool LenientOpen;
}
