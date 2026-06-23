using System;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.Serialization;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbNoEmitRelationMetadataTest : ObjectDbTestBase
{
    public ObjectDbNoEmitRelationMetadataTest(ITestOutputHelper output) : base(output)
    {
    }

    public class NoEmitMetadataRow
    {
        [PrimaryKey(1)]
        public int Id { get; set; }

        public string Name { get; set; } = "";
    }

    public interface INoEmitMetadataTable : IRelation<NoEmitMetadataRow>
    {
        NoEmitMetadataRow FindById(int id);
    }

    [Fact]
    public void EmitForRelationsUsesReflectionVersionInfoWithoutComparingGeneratedMetadata()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        ObjectDB.ResetAllMetadataCaches();
        var metadata = ReflectionMetadata.FindByType(typeof(NoEmitMetadataRow))!;
        var originalIndexOfInKeyValue = metadata.IndexOfInKeyValue;
        metadata.IndexOfInKeyValue = 0;

        try
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<INoEmitMetadataTable>();

            table.Upsert(new NoEmitMetadataRow { Id = 1, Name = "value" });

            Assert.Equal("value", table.FindById(1).Name);
        }
        finally
        {
            metadata.IndexOfInKeyValue = originalIndexOfInKeyValue;
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    [Fact]
    public void EmitForRelationsUsesReflectionSaverWhenGeneratedMetadataMissesField()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        ObjectDB.ResetAllMetadataCaches();
        var metadata = ReflectionMetadata.FindByType(typeof(NoEmitMetadataRow))!;
        var originalFields = metadata.Fields;
        metadata.Fields = metadata.Fields.Where(f => f.Name != nameof(NoEmitMetadataRow.Name)).ToArray();

        try
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<INoEmitMetadataTable>();

            table.Upsert(new NoEmitMetadataRow { Id = 1, Name = "value" });

            Assert.Equal("value", table.FindById(1).Name);
        }
        finally
        {
            metadata.Fields = originalFields;
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    [Fact]
    public void NoEmitForRelationsStillComparesReflectionAndGeneratedVersionInfo()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = true;
        ObjectDB.ResetAllMetadataCaches();
        var metadata = ReflectionMetadata.FindByType(typeof(NoEmitMetadataRow))!;
        var originalIndexOfInKeyValue = metadata.IndexOfInKeyValue;
        metadata.IndexOfInKeyValue = 0;

        try
        {
            using var tr = _db.StartTransaction();

            var ex = Assert.Throws<InvalidOperationException>(() => tr.GetRelation<INoEmitMetadataTable>());
            Assert.Contains("different metadata and reflection version info", ex.Message);
        }
        finally
        {
            metadata.IndexOfInKeyValue = originalIndexOfInKeyValue;
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
            ObjectDB.ResetAllMetadataCaches();
        }
    }
}
