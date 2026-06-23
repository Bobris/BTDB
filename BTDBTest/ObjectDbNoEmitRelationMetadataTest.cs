using System;
using System.Collections;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
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
    public void EmitForRelationsDoesNotNeedRegisteredMetadataForSaverAndOnSerialize()
    {
        var oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        ObjectDB.ResetAllMetadataCaches();
        var rowType = CreateReflectionOnlyRowType();

        try
        {
            Assert.Null(ReflectionMetadata.FindByType(rowType));
            using var tr = _db.StartTransaction();
            var relationType = typeof(IRelation<>).MakeGenericType(rowType);
            var table = tr.GetRelation(relationType);
            var row = Activator.CreateInstance(rowType)!;
            rowType.GetProperty("Id")!.SetValue(row, 1);
            rowType.GetProperty("Name")!.SetValue(row, "value");

            relationType.GetMethod(nameof(IRelation<object>.Upsert))!.Invoke(table, [row]);

            var saved = ((IEnumerable)table).Cast<object>().Single();
            Assert.Equal("VALUE", rowType.GetProperty("Name")!.GetValue(saved));
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = oldUseNoEmitForRelations;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    static Type CreateReflectionOnlyRowType()
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("BTDBTestDynamicRelations" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var module = assembly.DefineDynamicModule("Main");
        var type = module.DefineType("DynamicReflectionOnlyRow", TypeAttributes.Public | TypeAttributes.Class);

        DefineProperty(type, "Id", typeof(int), true);
        var nameAccessors = DefineProperty(type, "Name", typeof(string), false);

        var normalize = type.DefineMethod("Normalize",
            MethodAttributes.Private | MethodAttributes.HideBySig,
            typeof(void),
            Type.EmptyTypes);
        normalize.SetCustomAttribute(new CustomAttributeBuilder(
            typeof(OnSerializeAttribute).GetConstructor(Type.EmptyTypes)!,
            []));
        var il = normalize.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Call, nameAccessors.Getter);
        il.Emit(OpCodes.Callvirt, typeof(string).GetMethod(nameof(string.ToUpperInvariant), Type.EmptyTypes)!);
        il.Emit(OpCodes.Call, nameAccessors.Setter);
        il.Emit(OpCodes.Ret);

        return type.CreateType();
    }

    static (MethodBuilder Getter, MethodBuilder Setter) DefineProperty(TypeBuilder type, string name,
        Type propertyType, bool primaryKey)
    {
        var field = type.DefineField("_" + name, propertyType, FieldAttributes.Private);
        var property = type.DefineProperty(name, PropertyAttributes.None, propertyType, null);
        if (primaryKey)
        {
            property.SetCustomAttribute(new CustomAttributeBuilder(
                typeof(PrimaryKeyAttribute).GetConstructor([typeof(uint), typeof(bool)])!,
                [1u, false]));
        }

        var getter = type.DefineMethod("get_" + name,
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            propertyType,
            Type.EmptyTypes);
        var getterIl = getter.GetILGenerator();
        getterIl.Emit(OpCodes.Ldarg_0);
        getterIl.Emit(OpCodes.Ldfld, field);
        getterIl.Emit(OpCodes.Ret);
        property.SetGetMethod(getter);

        var setter = type.DefineMethod("set_" + name,
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(void),
            [propertyType]);
        var setterIl = setter.GetILGenerator();
        setterIl.Emit(OpCodes.Ldarg_0);
        setterIl.Emit(OpCodes.Ldarg_1);
        setterIl.Emit(OpCodes.Stfld, field);
        setterIl.Emit(OpCodes.Ret);
        property.SetSetMethod(setter);
        return (getter, setter);
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
