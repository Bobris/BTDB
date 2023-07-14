using System;
using BTDB.Allocators;
using BTDB.KVDBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class BTreeKeyValueDBTest : KeyValueDBFileTestBase, IDisposable
{
    readonly LeakDetectorWrapperAllocator _allocator;

    public BTreeKeyValueDBTest(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
        _allocator = new(new MallocAllocator());
    }

    public void Dispose()
    {
        var leaks = _allocator.QueryAllocations();
        Assert.Equal(0ul, leaks.Count);
    }

    protected override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection)
    {
        return NewKeyValueDB(new KeyValueDBOptions { FileCollection = fileCollection, Compression = new SnappyCompressionStrategy(), FileSplitSize = 2147483647 });
    }

    protected override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize = int.MaxValue)
    {
        return NewKeyValueDB(new KeyValueDBOptions { FileCollection = fileCollection, Compression = compression, FileSplitSize = fileSplitSize });
    }

    protected override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize, ICompactorScheduler compactorScheduler)
    {
        return NewKeyValueDB(new KeyValueDBOptions { FileCollection = fileCollection, Compression = compression, FileSplitSize = fileSplitSize, CompactorScheduler = compactorScheduler });
    }

    protected override IKeyValueDB NewKeyValueDB(KeyValueDBOptions options)
    {
        options.Allocator = _allocator;
        return new BTreeKeyValueDB(options);
    }
}
