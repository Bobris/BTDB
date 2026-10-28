using BTDB.KVDBLayer;
using BTDBTest;
using Xunit;

namespace BTDB.LSM.Test.Api;

// Field handler and ObjectDB collection suites from BTDBTest on LsmKeyValueDB.
[Collection(LsmObjectDb.Name)]
public class LsmFieldHandlerEmitModeTest : FieldHandlerEmitModeTest
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmFieldHandlerUpgradeConversionTest : FieldHandlerUpgradeConversionTest
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmODBDictionaryCoverageTest : ODBDictionaryCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmODBRoaringBitmapAndLazyListCoverageTest : ODBRoaringBitmapAndLazyListCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmODBSetCoverageTest : ODBSetCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDBTransactionCoverageTest : ObjectDBTransactionCoverageTest
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationKeyFieldHandlerTest : RelationKeyFieldHandlerTest
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}
