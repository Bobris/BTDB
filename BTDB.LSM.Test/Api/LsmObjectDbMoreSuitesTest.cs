using BTDB.KVDBLayer;
using BTDBTest;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Api;

// More ObjectDB suites from BTDBTest on LsmKeyValueDB, like LsmObjectDbTest.cs. ObjectDbVersionedSecondaryKeyTest is
// left out: it relies on transaction batches, and LsmKeyValueDB publishes every commit at once.

[Collection(LsmObjectDb.Name)]
public class LsmAdvancedEnumeratorEnumKeyTest(ITestOutputHelper output) : AdvancedEnumeratorEnumKeyTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmCommandHistoryNestedPolymorphicListRegressionTest(ITestOutputHelper output) : CommandHistoryNestedPolymorphicListRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmEmptyClassGenerateTest(ITestOutputHelper output) : EmptyClassGenerateTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmGenerateForRelationMetadataRegressionTest(ITestOutputHelper output) : GenerateForRelationMetadataRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmInKeyValueSecondaryKeyAdvancedEnumeratorTest(ITestOutputHelper output) : InKeyValueSecondaryKeyAdvancedEnumeratorTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmInterpreterStringQueryEndToEndTest(ITestOutputHelper output) : InterpreterStringQueryEndToEndTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmMultipleEnumerationTest(ITestOutputHelper output) : MultipleEnumerationTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmODBDictionaryNestedContentTest(ITestOutputHelper output) : ODBDictionaryNestedContentTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbEventSerializeTest(ITestOutputHelper output) : ObjectDbEventSerializeTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbExternalGenericBaseTest(ITestOutputHelper output) : ObjectDbExternalGenericBaseTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbIListArrayRegressionTest(ITestOutputHelper output) : ObjectDbIListArrayRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbIListCollectionRegressionTest(ITestOutputHelper output) : ObjectDbIListCollectionRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbInitializeRelationsTest(ITestOutputHelper output) : ObjectDbInitializeRelationsTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbNoEmitRelationMetadataTest(ITestOutputHelper output) : ObjectDbNoEmitRelationMetadataTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbRelationIdAllocatorTest(ITestOutputHelper output) : ObjectDbRelationIdAllocatorTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableBonTest(ITestOutputHelper output) : ObjectDbTableBonTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableSourceGeneratorRegressionTest(ITestOutputHelper output) : ObjectDbTableSourceGeneratorRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableUpdateByIdAllTypesTest(ITestOutputHelper output) : ObjectDbTableUpdateByIdAllTypesTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableWithSizesTest(ITestOutputHelper output) : ObjectDbTableWithSizesTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRawDataLayoutTest(ITestOutputHelper output) : RawDataLayoutTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationAdvancedEnumeratorDifferentialTest(ITestOutputHelper output) : RelationAdvancedEnumeratorDifferentialTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationBugRegressionTest(ITestOutputHelper output) : RelationBugRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationBuilderValidationTest(ITestOutputHelper output) : RelationBuilderValidationTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationConstraintDifferentialTest(ITestOutputHelper output) : RelationConstraintDifferentialTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationConstraintKeyTypesTest(ITestOutputHelper output) : RelationConstraintKeyTypesTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationManipulatorShapesTest(ITestOutputHelper output) : RelationManipulatorShapesTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationNestedContentLeakTest(ITestOutputHelper output) : RelationNestedContentLeakTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationQueryEngineCoverageTest(ITestOutputHelper output) : RelationQueryEngineCoverageTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationQueryKeyOrderTest(ITestOutputHelper output) : RelationQueryKeyOrderTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationQueryUpgradeAndKeyTypesTest(ITestOutputHelper output) : RelationQueryUpgradeAndKeyTypesTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmSchedulerRelationMetadataRegressionTest(ITestOutputHelper output) : SchedulerRelationMetadataRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmSparseInheritedEntityLoadRegressionTest(ITestOutputHelper output) : SparseInheritedEntityLoadRegressionTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmValueTypePropRefSetterDoubleLoadTest(ITestOutputHelper output) : ValueTypePropRefSetterDoubleLoadTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmRelationSnapshotIsolationTest(ITestOutputHelper output) : RelationSnapshotIsolationTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}
