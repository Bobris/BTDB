using BTDB.KVDBLayer;
using Xunit.Abstractions;

namespace BTDBTest;

public class KeyValueDBTest : KeyValueDBFileTestBase
{
    public KeyValueDBTest(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }

    protected override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection)
    {
        return new KeyValueDB(fileCollection);
    }

    protected override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize = int.MaxValue)
    {
        return new KeyValueDB(fileCollection, compression, fileSplitSize);
    }

    protected override IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize, ICompactorScheduler compactorScheduler)
    {
        return new KeyValueDB(fileCollection, compression, fileSplitSize, compactorScheduler);
    }

    protected override IKeyValueDB NewKeyValueDB(KeyValueDBOptions options)
    {
        return new KeyValueDB(options);
    }
}
