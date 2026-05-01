using System;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbRelationIdAllocatorTest : ObjectDbTestBase
{
    public ObjectDbRelationIdAllocatorTest(ITestOutputHelper output) : base(output)
    {
    }

    public class FirstItem
    {
        [PrimaryKey] public ulong Id { get; set; }
    }

    public class SecondItem
    {
        [PrimaryKey] public ulong Id { get; set; }
    }

    public interface IFirstTable : IRelation<FirstItem>
    {
        ulong AllocateId();
    }

    public interface ISecondTable : IRelation<SecondItem>
    {
        ulong AllocateId();
    }

    [Fact]
    public void SourceGeneratedNoEmitAllocatesIdsPerRelation()
    {
        using (var tr = _db.StartTransaction())
        {
            var first = tr.GetRelation<IFirstTable>();
            var second = tr.GetRelation<ISecondTable>();

            Assert.Equal(1ul, first.AllocateId());
            Assert.Equal(2ul, first.AllocateId());
            Assert.Equal(1ul, second.AllocateId());
            Assert.Equal(3ul, first.AllocateId());
            Assert.Equal(2ul, second.AllocateId());
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(4ul, tr.GetRelation<IFirstTable>().AllocateId());
            Assert.Equal(3ul, tr.GetRelation<ISecondTable>().AllocateId());
            tr.Commit();
        }

        using (var tr = _lowDb.StartReadOnlyTransaction())
        {
            Assert.Equal(4ul, ReadLastAllocatedId(tr, 1));
            Assert.Equal(3ul, ReadLastAllocatedId(tr, 2));
        }
    }

    [Fact]
    public void ReflectionEmitAllocatesIdsPerRelation()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        ObjectDB.ResetAllMetadataCaches();
        try
        {
            ReopenEmptyDb();

            using var tr = _db.StartTransaction();
            var first = tr.GetRelation<IFirstTable>();
            var second = tr.GetRelation<ISecondTable>();

            Assert.Equal(1ul, first.AllocateId());
            Assert.Equal(2ul, first.AllocateId());
            Assert.Equal(1ul, second.AllocateId());
            tr.Commit();
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    static ulong ReadLastAllocatedId(IKeyValueDBTransaction tr, byte relationId)
    {
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindExactKey([0, 6, relationId]));
        Span<byte> buffer = stackalloc byte[16];
        return PackUnpack.UnpackVUInt(cursor.GetValueSpan(ref buffer));
    }
}

[CollectionDefinition("IFieldHandler.UseNoEmitForRelations", DisableParallelization = true)]
public class UseNoEmitForRelationsCollection
{
}
