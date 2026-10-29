using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Invalid relation interfaces cannot be declared in source code, because BTDB.SourceGenerator reports them as compile
// errors, so they are emitted at runtime. Such interfaces do not have source generated implementation and are always
// built by IL emitting RelationBuilder, which must reject them with descriptive error.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class RelationBuilderValidationTest : ObjectDbTestBase
{
    public RelationBuilderValidationTest(ITestOutputHelper output) : base(output)
    {
    }

    public class VRow
    {
        [PrimaryKey(1)] public ulong Tenant { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    public class VRowNoSk
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public string Name { get; set; } = "";
    }

    public record M(string Name, Type Return, params (Type Type, string Name)[] Params);

    static readonly Type R = typeof(VRow);
    static readonly Type C = typeof(Constraint<ulong>);
    static readonly Type AepU = typeof(AdvancedEnumeratorParam<ulong>);

    static (Type, string) P(Type type, string name) => (type, name);

    static readonly Dictionary<string, (Type Row, M Method, Type Exception, string Message)> Cases = new()
    {
        ["RemoveBySecondaryKey"] = (R, new("RemoveByName", typeof(int), P(typeof(string), "name")),
            typeof(BTDBException), "Remove by secondary key in"),
        ["ContainsParamCount"] = (R, new("Contains", typeof(bool), P(typeof(ulong), "tenant")),
            typeof(BTDBException), "Number of parameters in Contains does not match primary key count 2"),
        ["IterateTooFewParams"] = (R, new("IterateById", typeof(void), P(typeof(Action<VRow>), "callback")),
            typeof(BTDBException), "expects callback and value parameters"),
        ["IterateValueNotClass"] = (R, new("IterateById", typeof(void), P(typeof(Action<int>), "callback"),
            P(typeof(int), "value")), typeof(BTDBException), "value parameter must have class type"),
        ["IterateWrongCallback"] = (R, new("IterateById", typeof(void), P(typeof(Action<string>), "callback"),
            P(typeof(VRow), "value")), typeof(BTDBException), "callback parameter must be Action<"),
        ["IterateNotAction"] = (R, new("IterateById", typeof(void), P(typeof(Func<VRow>), "callback"),
            P(typeof(VRow), "value")), typeof(BTDBException), "callback parameter must be Action<"),
        ["IterateTooManyKeys"] = (R, new("IterateById", typeof(void), P(typeof(ulong), "tenant"),
                P(typeof(ulong), "id"), P(typeof(ulong), "extra"), P(typeof(Action<VRow>), "callback"),
                P(typeof(VRow), "value")),
            typeof(BTDBException), "Number of parameters in IterateById is bigger than key count 2."),
        ["IterateReturnType"] = (R, new("IterateById", typeof(int), P(typeof(Action<VRow>), "callback"),
            P(typeof(VRow), "value")), typeof(BTDBException), "should be defined with Void return type"),
        ["FirstByIdNotClass"] = (R, new("FirstById", typeof(int), P(C, "tenant")), typeof(BTDBException),
            "must have class return type"),
        ["LastByIdNotClass"] = (R, new("LastById", typeof(int), P(C, "tenant")), typeof(BTDBException),
            "must have class return type"),
        ["RemoveByIdParamCount"] = (R, new("RemoveById", typeof(void), P(typeof(ulong), "tenant")),
            typeof(BTDBException), "Number of parameters in RemoveById does not match primary key count 2"),
        ["RemoveByIdAepReturn"] = (R, new("RemoveById", typeof(void), P(typeof(ulong), "tenant"),
            P(AepU, "id")), typeof(BTDBException), "RemoveById"),
        ["RemoveByIdPartialShape"] = (R, new("RemoveByIdPartial", typeof(int), P(typeof(ulong), "tenant"),
            P(typeof(int), "count")), typeof(BTDBException), "Invalid shape of RemoveByIdPartial."),
        ["RemoveByIdPartialNoParams"] = (R, new("RemoveByIdPartial", typeof(int)), typeof(BTDBException),
            "Invalid shape of RemoveByIdPartial."),
        ["ListByIdAepWrongReturn"] = (R, new("ListById", typeof(int), P(typeof(ulong), "tenant"),
            P(AepU, "id")), typeof(BTDBException), "Invalid method ListById"),
        ["ListByIdWrongReturn"] = (R, new("ListById", typeof(int), P(typeof(ulong), "tenant")),
            typeof(BTDBException), "Invalid method ListById"),
        ["ListByIdEnumerator"] = (R, new("ListById", typeof(IEnumerator<VRow>), P(typeof(ulong), "tenant")),
            typeof(NotSupportedException), "is not supported anymore"),
        ["ListByIdAepTypeMismatch"] = (R, new("ListById", typeof(IEnumerable<VRow>), P(typeof(ulong), "tenant"),
                P(typeof(AdvancedEnumeratorParam<string>), "id")), typeof(BTDBException),
            "Parameter type mismatch in ListById"),
        ["ListByIdDictWrongKey"] = (R, new("ListById", typeof(IOrderedDictionaryEnumerator<string, VRow>),
            P(typeof(ulong), "tenant"), P(AepU, "id")), typeof(BTDBException), "Invalid method ListById"),
        ["CountByIdReturnType"] = (R, new("CountById", typeof(Guid), P(typeof(ulong), "tenant")),
            typeof(BTDBException), "Invalid return type in method CountById"),
        ["AnyByIdReturnType"] = (R, new("AnyById", typeof(int), P(typeof(ulong), "tenant")),
            typeof(BTDBException), "should be defined with Boolean return type"),
        ["ListByNameAepWrongReturn"] = (R, new("ListByName", typeof(int),
            P(typeof(AdvancedEnumeratorParam<string>), "name")), typeof(BTDBException), "Invalid method ListByName"),
        ["ListByNameWrongReturn"] = (R, new("ListByName", typeof(int), P(typeof(string), "name")),
            typeof(BTDBException), "Invalid method ListByName"),
        ["ListByNameAepTypeMismatch"] = (R, new("ListByName", typeof(IEnumerable<VRow>), P(AepU, "name")),
            typeof(BTDBException), "Parameter type mismatch in ListByName"),
        ["CountByNameReturnType"] = (R, new("CountByName", typeof(Guid), P(typeof(string), "name")),
            typeof(BTDBException), "Invalid return type in method CountByName"),
        ["AnyByNameReturnType"] = (R, new("AnyByName", typeof(int), P(typeof(string), "name")),
            typeof(BTDBException), "should be defined with Boolean return type"),
        ["UpdateByIdNotEnoughParams"] = (R, new("UpdateById", typeof(void), P(typeof(ulong), "tenant")),
            typeof(BTDBException), "Not enough parameters in UpdateById"),
        ["UpdateByIdReturnType"] = (R, new("UpdateById", typeof(int), P(typeof(ulong), "tenant"),
            P(typeof(ulong), "id")), typeof(BTDBException), "should be defined with void or bool return type"),
        ["UpdateByIdWrongType"] = (R, new("UpdateById", typeof(void), P(typeof(ulong), "tenant"),
                P(typeof(ulong), "id"), P(typeof(Guid), "value")), typeof(BTDBException),
            "matched parameter value has wrong type"),
        ["UpdateByIdUnknownField"] = (R, new("UpdateById", typeof(void), P(typeof(ulong), "tenant"),
                P(typeof(ulong), "id"), P(typeof(int), "unknown")), typeof(BTDBException),
            "parameters unknown does not match any relation fields"),
        ["UpdateByIdDuplicateField"] = (R, new("UpdateById", typeof(void), P(typeof(ulong), "tenant"),
                P(typeof(ulong), "id"), P(typeof(int), "value"), P(typeof(int), "Value")), typeof(BTDBException),
            "parameters Value does not match any relation fields"),
        ["InsertParamCount"] = (R, new("Insert", typeof(void), P(R, "a"), P(R, "b")), typeof(BTDBException),
            "Method Insert expects 1 parameters count."),
        ["InsertParamType"] = (R, new("Insert", typeof(void), P(typeof(string), "a")), typeof(BTDBException),
            "Method Insert expects 0th parameter of type VRow."),
        ["InsertReturnType"] = (R, new("Insert", typeof(int), P(R, "a")), typeof(BTDBException),
            "should be defined with void or bool return type"),
        ["UnknownMethod"] = (R, new("DoSomething", typeof(void)), typeof(BTDBException),
            "Method DoSomething is not supported."),
        ["ManipulatorMethodReturnType"] = (R, new("ShallowUpsert", typeof(void), P(R, "obj")),
            typeof(BTDBException), "Method ShallowUpsert should be defined with Boolean return type."),
        ["ManipulatorMethodParamCount"] = (R, new("ShallowUpsert", typeof(bool)), typeof(BTDBException),
            "Method ShallowUpsert expects 1 parameters count."),
        ["UpsertWithSizesWithSecondaryKey"] = (R, new("ShallowUpsertWithSizes",
                typeof((bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize)), P(R, "obj"),
                P(typeof(bool), "allowInsert"), P(typeof(bool), "allowUpdate")), typeof(BTDBException),
            "cannot be used with relation with secondary indexes"),
        ["RemoveWithSizesWithSecondaryKey"] = (R, new("RemoveWithSizesById",
                typeof((ulong Count, ulong KeySizes, ulong ValueSizes)), P(C, "tenant")), typeof(BTDBException),
            "cannot be used with relation with secondary indexes"),
        ["RemoveWithSizesReturnType"] = (typeof(VRowNoSk), new("RemoveWithSizesById", typeof(ulong), P(C, "id")),
            typeof(BTDBException), "RemoveWithSizesById should be defined with"),
        ["ScanByIdTooManyConstraints"] = (R, new("ScanById", typeof(IEnumerable<VRow>), P(C, "tenant"),
                P(C, "id"), P(C, "extra")), typeof(BTDBException),
            "Number of constraints parameters in ScanById is bigger than key count 2."),
        ["ScanByIdNameMismatch"] = (R, new("ScanById", typeof(IEnumerable<VRow>), P(C, "company")),
            typeof(BTDBException), "Parameter and key mismatch in ScanById, Tenant!=company."),
        ["ScanByIdNotConstraint"] = (R, new("ScanById", typeof(IEnumerable<VRow>), P(typeof(ulong), "tenant")),
            typeof(BTDBException), "Parameter tenant in ScanById is not implementation of Constraint<T>."),
        ["ScanByIdConstraintTypeMismatch"] = (R, new("ScanById", typeof(IEnumerable<VRow>),
                P(typeof(Constraint<string>), "tenant")), typeof(BTDBException),
            "Parameter constraint type mismatch in ScanById"),
        ["ScanByIdReturnType"] = (R, new("ScanById", typeof(VRow), P(C, "tenant")), typeof(BTDBException),
            "Method ScanById must return IEnumerable<T> type."),
        ["ScanByNameReturnType"] = (R, new("ScanByName", typeof(VRow), P(typeof(Constraint<string>), "name")),
            typeof(BTDBException), "Method ScanByName must return IEnumerable<T> type."),
        ["GatherByIdReturnType"] = (R, new("GatherById", typeof(int), P(typeof(List<VRow>), "target"),
            P(typeof(long), "skip"), P(typeof(long), "take")), typeof(BTDBException), "UInt64 return type"),
        ["GatherByIdTooFewParams"] = (R, new("GatherById", typeof(ulong), P(typeof(List<VRow>), "target")),
            typeof(BTDBException), "Method GatherById expects at least 3 parameters."),
        ["GatherByIdNotCollection"] = (R, new("GatherById", typeof(ulong), P(typeof(IEnumerable<VRow>), "target"),
                P(typeof(long), "skip"), P(typeof(long), "take")), typeof(BTDBException),
            "first parameter must inherit from ICollection<>"),
        ["GatherByIdSkip"] = (R, new("GatherById", typeof(ulong), P(typeof(List<VRow>), "target"),
                P(typeof(int), "skip"), P(typeof(long), "take")), typeof(BTDBException),
            "second parameter must be long type and named skip"),
        ["GatherByIdTake"] = (R, new("GatherById", typeof(ulong), P(typeof(List<VRow>), "target"),
                P(typeof(long), "skip"), P(typeof(long), "count")), typeof(BTDBException),
            "parameter must be long type and named take"),
        ["GatherByNameSkip"] = (R, new("GatherByName", typeof(ulong), P(typeof(List<VRow>), "target"),
                P(typeof(long), "offset"), P(typeof(long), "take")), typeof(BTDBException),
            "second parameter must be long type and named skip"),
        ["FindByIdParamCount"] = (R, new("FindById", R, P(typeof(ulong), "tenant")), typeof(BTDBException),
            "Number of parameters in FindById does not match primary key count 2."),
        ["FindByIdNameMismatch"] = (R, new("FindById", R, P(typeof(ulong), "tenant"), P(typeof(ulong), "key")),
            typeof(BTDBException), "Parameter and key mismatch in FindById, Id!=key."),
        ["FindByIdTooManyParams"] = (R, new("FindById", R, P(typeof(ulong), "tenant"), P(typeof(ulong), "id"),
            P(typeof(string), "name")), typeof(BTDBException), "Number of parameters in FindById is bigger than key count 2."),
        ["ContainsTooManyParams"] = (R, new("Contains", typeof(bool), P(typeof(ulong), "tenant"),
                P(typeof(ulong), "id"), P(typeof(string), "name")), typeof(BTDBException),
            "Number of parameters in Contains is bigger than key count 2."),
        ["RemoveByIdTooManyParams"] = (R, new("RemoveById", typeof(bool), P(typeof(ulong), "tenant"),
                P(typeof(ulong), "id"), P(typeof(string), "name")), typeof(BTDBException),
            "Number of parameters in RemoveById is bigger than key count 2."),
        ["CountByIdTooManyParams"] = (R, new("CountById", typeof(int), P(typeof(ulong), "tenant"),
                P(typeof(ulong), "id"), P(typeof(string), "name")), typeof(BTDBException),
            "Number of parameters in CountById is bigger than key count 2."),
        ["ListByNameTooManyParams"] = (R, new("ListByName", typeof(IEnumerable<VRow>), P(typeof(string), "name"),
                P(typeof(ulong), "tenant"), P(typeof(ulong), "id"), P(typeof(string), "extra")), typeof(BTDBException),
            "Number of parameters in ListByName is bigger than key count 3."),
        ["FindByIdTypeMismatch"] = (R, new("FindById", R, P(typeof(ulong), "tenant"), P(typeof(Guid), "id")),
            typeof(BTDBException), "Parameter type mismatch in FindById"),
    };

    public static IEnumerable<object[]> CaseNames() => Cases.Keys.Select(k => new object[] { k });

    static Type DefineRelationInterface(Type rowType, M method)
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("BTDBTestInvalidRelation" + Guid.NewGuid().ToString("N")), AssemblyBuilderAccess.Run);
        var module = assembly.DefineDynamicModule("Main");
        var type = module.DefineType("IInvalid" + method.Name + "Relation",
            TypeAttributes.Public | TypeAttributes.Interface | TypeAttributes.Abstract, null,
            [typeof(IRelation<>).MakeGenericType(rowType)]);
        var methodBuilder = type.DefineMethod(method.Name,
            MethodAttributes.Public | MethodAttributes.Abstract | MethodAttributes.Virtual |
            MethodAttributes.HideBySig | MethodAttributes.NewSlot, method.Return,
            method.Params.Select(p => p.Type).ToArray());
        for (var i = 0; i < method.Params.Length; i++)
            methodBuilder.DefineParameter(i + 1, ParameterAttributes.None, method.Params[i].Name);
        return type.CreateType();
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public void InvalidRelationMethodIsRejected(string caseName)
    {
        var (row, method, exceptionType, message) = Cases[caseName];
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = false;
            ObjectDB.ResetAllMetadataCaches();
            var relationType = DefineRelationInterface(row, method);
            using var tr = _db.StartTransaction();
            var ex = Assert.ThrowsAny<Exception>(() => tr.GetRelation(relationType));
            if (ex is TargetInvocationException { InnerException: { } inner }) ex = inner;
            Assert.IsType(exceptionType, ex);
            Assert.Contains(message, ex.Message);
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    [Fact]
    public void ValidRuntimeEmittedRelationWorksInBothModes()
    {
        foreach (var noEmit in new[] { true, false })
        {
            var old = IFieldHandler.UseNoEmitForRelations;
            try
            {
                IFieldHandler.UseNoEmitForRelations = noEmit;
                ObjectDB.ResetAllMetadataCaches();
                // VRow has source generated metadata (thanks to IVRowTable below), but this interface does not have
                // source generated implementation, so RelationBuilder emits it even in no-emit mode
                var relationType = DefineRelationInterface(R,
                    new("FindByIdOrDefault", R, P(typeof(ulong), "tenant"), P(typeof(ulong), "id")));
                using var tr = _db.StartTransaction();
                var relation = (IRelation<VRow>)tr.GetRelation(relationType);
                relation.Upsert(new() { Tenant = 1, Id = 2, Name = "A", Value = 3 });
                var found = (VRow?)relationType.GetMethod("FindByIdOrDefault")!.Invoke(relation, [1ul, 2ul]);
                Assert.Equal(3, found!.Value);
                Assert.Null(relationType.GetMethod("FindByIdOrDefault")!.Invoke(relation, [1ul, 3ul]));
            }
            finally
            {
                IFieldHandler.UseNoEmitForRelations = old;
                ObjectDB.ResetAllMetadataCaches();
            }
        }
    }

    public interface IVRowTable : IRelation<VRow>
    {
        VRow? FindByIdOrDefault(ulong tenant, ulong id);
    }

    public interface IVRowNoSkTable : IRelation<VRowNoSk>
    {
        VRowNoSk? FindByIdOrDefault(ulong id);
    }
}
