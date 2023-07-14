using BTDB.KVDBLayer;
using Xunit.Abstractions;

namespace BTDBTest;

public class InMemoryInMemoryKeyValueDBTest : KeyValueDBTestBase
{
    public InMemoryInMemoryKeyValueDBTest(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }

    protected override IKeyValueDB NewKeyValueDB()
    {
        return new InMemoryKeyValueDB();
    }
}
