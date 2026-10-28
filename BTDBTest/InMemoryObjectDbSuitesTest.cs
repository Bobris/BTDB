using BTDB.KVDBLayer;
using Xunit.Abstractions;

namespace BTDBTest;

/// ObjectDB suites running on InMemoryKeyValueDB, whose cursors otherwise only the key value tests use.

public class InMemoryObjectDbTableTest(ITestOutputHelper output) : ObjectDbTableTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryObjectDbTableScanTest(ITestOutputHelper output) : ObjectDbTableScanTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryObjectDbRelationQueryTest(ITestOutputHelper output) : ObjectDbRelationQueryTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryObjectDbRelationQueryDifferentialTest(ITestOutputHelper output)
    : ObjectDbRelationQueryDifferentialTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryRelationMutationDifferentialTest(ITestOutputHelper output) : RelationMutationDifferentialTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryObjectDbAdvancedEnumeratorBoundaryTest(ITestOutputHelper output)
    : ObjectDbAdvancedEnumeratorBoundaryTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryRelationInKeyValueMutationTest(ITestOutputHelper output) : RelationInKeyValueMutationTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}
