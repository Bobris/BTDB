using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

// Field handler and ObjectDB collection suites on InMemoryKeyValueDB.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class InMemoryFieldHandlerEmitModeTest : FieldHandlerEmitModeTest
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class InMemoryFieldHandlerUpgradeConversionTest : FieldHandlerUpgradeConversionTest
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryODBDictionaryCoverageTest : ODBDictionaryCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryODBRoaringBitmapAndLazyListCoverageTest : ODBRoaringBitmapAndLazyListCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryODBSetCoverageTest : ODBSetCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

public class InMemoryObjectDBTransactionCoverageTest : ObjectDBTransactionCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class InMemoryRelationKeyFieldHandlerTest : RelationKeyFieldHandlerTest
{
    protected override IKeyValueDB CreateKeyValueDB() => new InMemoryKeyValueDB();
}
