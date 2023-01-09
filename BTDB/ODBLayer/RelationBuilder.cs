using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

// ReSharper disable ParameterOnlyUsedForPreconditionCheck.Local

namespace BTDB.ODBLayer;

public class RelationBuilder
{
    readonly Type _relationDbManipulatorType;
    readonly string _name;
    public readonly Type InterfaceType;
    public readonly Type ItemType;
    public readonly object PristineItemInstance;
    public readonly RelationVersionInfo ClientRelationVersionInfo;
    public readonly List<Type> LoadTypes = new();
    public readonly IRelationInfoResolver RelationInfoResolver;
    public IILDynamicMethodWithThis DelegateCreator { get; }

    static readonly MethodInfo SpanWriterGetSpanMethodInfo =
        typeof(SpanWriter).GetMethod(nameof(SpanWriter.GetSpan))!;

    static readonly MethodInfo SpanWriterGetPersistentSpanAndResetMethodInfo =
        typeof(SpanWriter).GetMethod(nameof(SpanWriter.GetPersistentSpanAndReset))!;

    static Dictionary<(Type, string?), RelationBuilder> _relationBuilderCache = new();
    static readonly object RelationBuilderCacheLock = new();

    internal static void Reset()
    {
        _relationBuilderCache = new();
    }

    internal static RelationBuilder GetFromCache(Type interfaceType, IRelationInfoResolver relationInfoResolver)
    {
        if (_relationBuilderCache.TryGetValue((interfaceType, relationInfoResolver.ActualOptions.Name),
                out var res))
        {
            return res;
        }

        lock (RelationBuilderCacheLock)
        {
            if (_relationBuilderCache.TryGetValue((interfaceType, relationInfoResolver.ActualOptions.Name),
                    out res))
            {
                return res;
            }

            _relationBuilderCache = new(_relationBuilderCache)
            {
                {
                    (interfaceType, relationInfoResolver.ActualOptions.Name),
                    res = new(interfaceType, relationInfoResolver)
                }
            };
        }

        return res;
    }

    public RelationBuilder(Type interfaceType, IRelationInfoResolver relationInfoResolver)
    {
        RelationInfoResolver = relationInfoResolver;
        InterfaceType = interfaceType;
        ItemType = interfaceType.SpecializationOf(typeof(ICovariantRelation<>))!.GenericTypeArguments[0];
        PristineItemInstance = CreatePristineInstance();
        if (ReferenceEquals(PristineItemInstance, CreatePristineInstance()))
            relationInfoResolver.ActualOptions.ThrowBTDBException(
                ItemType.ToSimpleName() + " cannot be registered as singleton");
        _name = InterfaceType.ToSimpleName();
        ClientRelationVersionInfo = CreateVersionInfoByReflection();
        _relationDbManipulatorType = typeof(RelationDBManipulator<>).MakeGenericType(ItemType);
        LoadTypes.Add(ItemType);
        DelegateCreator = Build();
    }

    object CreatePristineInstance()
    {
        var container = RelationInfoResolver.Container;
        var res = container?.ResolveOptional(ItemType);
        return (res ??
                (ItemType.GetDefaultConstructor() != null
                    ? Activator.CreateInstance(ItemType, nonPublic: true)
                    : RuntimeHelpers.GetUninitializedObject(ItemType)))!;
    }

    RelationVersionInfo CreateVersionInfoByReflection()
    {
        var props = ItemType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        var primaryKeys = new Dictionary<uint, TableFieldInfo>(1); //PK order->fieldInfo
        var secondaryKeyFields = new List<TableFieldInfo>();
        var secondaryKeys =
            new List<Tuple<int, IList<SecondaryKeyAttribute>>>(); //positive: sec key field idx, negative: pk order, attrs

        var publicFields = ItemType.GetFields(BindingFlags.Public | BindingFlags.Instance);
        foreach (var field in publicFields)
        {
            if (field.GetCustomAttribute<NotStoredAttribute>(true) != null) continue;
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Public field {_name}.{field.Name} must have NotStoredAttribute. It is just intermittent, until they can start to be supported.");
        }

        var fields = new List<TableFieldInfo>(props.Length);
        foreach (var pi in props)
        {
            if (pi.GetCustomAttribute<NotStoredAttribute>(true) != null) continue;
            if (pi.GetIndexParameters().Length != 0) continue;
            var pks = pi.GetCustomAttributes(typeof(PrimaryKeyAttribute), true);
            PrimaryKeyAttribute actualPKAttribute = null;
            if (pks.Length != 0)
            {
                actualPKAttribute = (PrimaryKeyAttribute)pks[0];
                var fieldInfo = TableFieldInfo.Build(_name, pi, RelationInfoResolver.FieldHandlerFactory,
                    FieldHandlerOptions.Orderable);
                if (fieldInfo.Handler!.NeedsCtx())
                    RelationInfoResolver.ActualOptions.ThrowBTDBException(
                        $"Unsupported key field {fieldInfo.Name} type.");
                primaryKeys.Add(actualPKAttribute.Order, fieldInfo);
            }

            var sks = pi.GetCustomAttributes(typeof(SecondaryKeyAttribute), true);
            var id = (int)(-actualPKAttribute?.Order ?? secondaryKeyFields.Count);
            List<SecondaryKeyAttribute> currentList = null;
            for (var i = 0; i < sks.Length; i++)
            {
                if (i == 0)
                {
                    currentList = new List<SecondaryKeyAttribute>(sks.Length);
                    secondaryKeys.Add(new Tuple<int, IList<SecondaryKeyAttribute>>(id, currentList));
                    if (actualPKAttribute == null)
                        secondaryKeyFields.Add(TableFieldInfo.Build(_name, pi,
                            RelationInfoResolver.FieldHandlerFactory, FieldHandlerOptions.Orderable));
                }

                var key = (SecondaryKeyAttribute)sks[i];
                if (key.Name == "Id")
                    RelationInfoResolver.ActualOptions.ThrowBTDBException(
                        "'Id' is invalid name for secondary key, it is reserved for primary key identification.");
                currentList!.Add(key);
            }

            if (actualPKAttribute == null)
                fields.Add(TableFieldInfo.Build(_name, pi, RelationInfoResolver.FieldHandlerFactory,
                    FieldHandlerOptions.None));
        }

        return new RelationVersionInfo(primaryKeys, secondaryKeys, secondaryKeyFields.ToArray(), fields.ToArray());
    }

    int RegisterLoadType(Type itemType)
    {
        for (var i = 0; i < LoadTypes.Count; i++)
        {
            if (LoadTypes[i] == itemType)
            {
                return i;
            }
        }

        LoadTypes.Add(itemType);
        return LoadTypes.Count - 1;
    }

    IILDynamicMethodWithThis Build()
    {
        var interfaceType = InterfaceType;
        var relationName = interfaceType!.ToSimpleName();
        var classImpl = ILBuilder.Instance.NewType("Relation" + relationName, _relationDbManipulatorType,
            new[] { interfaceType });
        var constructorMethod =
            classImpl.DefineConstructor(new[] { typeof(IObjectDBTransaction), typeof(RelationInfo) });
        var il = constructorMethod.Generator;
        // super.ctor(transaction, relationInfo);
        il.Ldarg(0).Ldarg(1).Ldarg(2)
            .Call(_relationDbManipulatorType.GetConstructor(new[]
                { typeof(IObjectDBTransaction), typeof(RelationInfo) })!)
            .Ret();
        var methods = RelationInfo.GetMethods(interfaceType);
        foreach (var method in methods)
        {
            if (method.Name.StartsWith("get_") || method.Name.StartsWith("set_"))
                continue;
            var reqMethod = classImpl.DefineMethod("_R_" + method.Name, method.ReturnType,
                method.GetParameters().Select(pi => pi.ParameterType).ToArray(),
                MethodAttributes.Virtual | MethodAttributes.Public);
            reqMethod.InitLocals = false;
            if (method.Name.StartsWith("RemoveBy") || method.Name.StartsWith("ShallowRemoveBy"))
            {
                var methodParameters = method.GetParameters();
                if (method.Name == "RemoveByIdPartial")
                    BuildRemoveByIdPartialMethod(method, methodParameters, reqMethod);
                else
                {
                    if (StripVariant(SubstringAfterBy(method.Name), false).IndexName == "Id")
                    {
                        if (ParametersEndsWithAdvancedEnumeratorParam(methodParameters))
                            BuildRemoveByIdAdvancedParamMethod(method, methodParameters, reqMethod);
                        else
                            BuildRemoveByMethod(method, methodParameters, reqMethod);
                    }
                    else
                    {
                        RelationInfoResolver.ActualOptions.ThrowBTDBException(
                            $"Remove by secondary key in {_name}.{method.Name} is unsupported. Instead use ListBy and remove enumerated.");
                    }
                }
            }
            else if (method.Name == "RemoveWithSizesById")
            {
                ForbidSecondaryKeys(method);
                CreateMethodRemoveWithSizesById(reqMethod.Generator, method.Name, method.GetParameters(),
                    method.ReturnType);
            }
            else if (method.Name.StartsWith("ScanBy", StringComparison.Ordinal))
            {
                BuildScanByMethod(method, reqMethod);
            }
            else if (method.Name.StartsWith("GatherBy", StringComparison.Ordinal))
            {
                BuildGatherByMethod(method, reqMethod);
            }
            else if (method.Name.StartsWith("FirstBy", StringComparison.Ordinal))
            {
                BuildFirstByMethod(method, reqMethod);
            }
            else if (method.Name.StartsWith("FindBy", StringComparison.Ordinal))
            {
                BuildFindByMethod(method, reqMethod);
            }
            else if (method.Name == "Contains")
            {
                BuildContainsMethod(method, reqMethod);
            }
            else if (method.Name == "CountById") //count by primary key
            {
                BuildCountByIdMethod(method, reqMethod);
            }
            else if (method.Name == "AnyById") //any by primary key
            {
                BuildAnyByIdMethod(method, reqMethod);
            }
            else if (method.Name.StartsWith("ListBy", StringComparison.Ordinal)
                    ) //ListBy{Name}(tenantId, .., AdvancedEnumeratorParam)
            {
                if (StripVariant(method.Name[6..], false).IndexName == "Id")
                {
                    // List by primary key
                    BuildListByIdMethod(method, reqMethod);
                }
                else
                {
                    BuildListByMethod(method, reqMethod);
                }
            }
            else if (method.Name.StartsWith("CountBy", StringComparison.Ordinal)
                    ) //CountBy{Name}(tenantId, ..[, AdvancedEnumeratorParam])
            {
                BuildCountByMethod(method, reqMethod);
            }
            else if (method.Name.StartsWith("AnyBy", StringComparison.Ordinal)
                    ) //AnyBy{Name}(tenantId, ..[, AdvancedEnumeratorParam])
            {
                BuildAnyByMethod(method, reqMethod);
            }
            else if (method.Name.StartsWith("UpdateById", StringComparison.Ordinal))
            {
                BuildUpdateByIdMethod(method, reqMethod);
            }
            else if (method.Name == "Insert")
            {
                BuildInsertMethod(method, reqMethod);
            }
            else
            {
                BuildManipulatorCallWithSameParameters(method, reqMethod);
            }

            reqMethod.Generator.Ret();
            classImpl.DefineMethodOverride(reqMethod, method);
        }

        var classImplType = classImpl.CreateType();

        var methodBuilder = ILBuilder.Instance.NewMethod("RelationFactory" + relationName,
            typeof(Func<IObjectDBTransaction, IRelation>), typeof(RelationInfo));
        var ilGenerator = methodBuilder.Generator;
        ilGenerator
            .Ldarg(1)
            .Ldarg(0)
            .Newobj(classImplType.GetConstructor(new[] { typeof(IObjectDBTransaction), typeof(RelationInfo) })!)
            .Castclass(typeof(IRelation))
            .Ret();
        return methodBuilder;
    }

    static string SubstringAfterBy(string name)
    {
        var byIndex = name.IndexOf("By", StringComparison.Ordinal);
        return name[(byIndex + 2)..];
    }

    static bool ParametersEndsWithAdvancedEnumeratorParam(ParameterInfo[] methodParameters)
    {
        return methodParameters.Length > 0 && methodParameters[^1].ParameterType
            .InheritsOrImplements(typeof(AdvancedEnumeratorParam<>));
    }

    void BuildContainsMethod(MethodInfo method, IILMethod reqMethod)
    {
        var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);

        WriteRelationPKPrefix(reqMethod.Generator, pushWriter);
        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields;

        var count = SaveMethodParameters(reqMethod.Generator, "Contains", method.GetParameters(),
            primaryKeyFields.Span, pushWriter, ctxLocFactory);
        if (count != primaryKeyFields.Length)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Number of parameters in Contains does not match primary key count {primaryKeyFields.Length}.");

        var localSpan = reqMethod.Generator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        //call manipulator.Contains
        reqMethod.Generator
            .Ldarg(0) //manipulator
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan)
            .Callvirt(_relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.Contains))!);
    }

    void BuildScanByMethod(MethodInfo method, IILMethod reqMethod)
    {
        var nameWithoutVariants = StripVariant(SubstringAfterBy(method.Name), false).IndexName;
        if (nameWithoutVariants is "Id")
        {
            CreateMethodScanById(reqMethod.Generator, method.Name,
                method.GetParameters(), method.ReturnType);
        }
        else
        {
            CreateMethodScanBy(reqMethod.Generator, method.Name,
                method.GetParameters(), method.ReturnType);
        }
    }

    void BuildGatherByMethod(MethodInfo method, IILMethod reqMethod)
    {
        CheckReturnType(method.Name, typeof(ulong), method.ReturnType);
        var indexName = method.Name[8..];
        if (indexName == "Id")
        {
            CreateMethodGatherById(reqMethod.Generator, method.Name, method.GetParameters());
        }
        else
        {
            CreateMethodGatherBy(reqMethod.Generator, method.Name, method.GetParameters(), indexName);
        }
    }

    void BuildFirstByMethod(MethodInfo method, IILMethod reqMethod)
    {
        if (!method.ReturnType.IsClass)
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {method.Name} must have class return type.");
        }

        var nameWithoutVariants = StripVariant(SubstringAfterBy(method.Name), true);
        if (nameWithoutVariants.IndexName is "Id")
        {
            CreateMethodFirstById(reqMethod.Generator, method.Name, method.ReturnType, method.GetParameters(),
                nameWithoutVariants.HasOrDefault);
        }
        else
        {
            CreateMethodFirstBy(reqMethod.Generator, method.Name, method.ReturnType, method.GetParameters(),
                nameWithoutVariants.HasOrDefault, nameWithoutVariants.IndexName);
        }
    }

    void CreateMethodFirstBy(IILGen ilGenerator, string methodName, Type itemType, ParameterInfo[] methodParameters,
        bool hasOrDefault, string skName)
    {
        var constraintsLocal = ilGenerator.DeclareLocal(typeof(ConstraintInfo[]));

        var skIndex = ClientRelationVersionInfo.GetSecondaryKeyIndex(skName);

        var secondaryKeyFields = ClientRelationVersionInfo.GetSecondaryKeyFields(skIndex);

        var constraintsParameters = methodParameters.AsSpan();
        var orderersLocal = DetectOrderers(ilGenerator, ref constraintsParameters, 0);
        SaveMethodConstraintParameters(ilGenerator, methodName, constraintsParameters, secondaryKeyFields,
            constraintsLocal);

        //call manipulator.FirstBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .LdcI4(RegisterLoadType(itemType!))
            .Ldloc(constraintsLocal)
            .LdcI4((int)skIndex)
            .Ldloc(orderersLocal)
            .LdcI4(hasOrDefault ? 1 : 0)
            .Callvirt(
                _relationDbManipulatorType.GetMethod(
                    nameof(RelationDBManipulator<IRelation>.FirstBySecondaryKey))!.MakeGenericMethod(itemType));
    }

    void CreateMethodFirstById(IILGen ilGenerator, string methodName, Type itemType, ParameterInfo[] methodParameters,
        bool hasOrDefault)
    {
        var constraintsLocal = ilGenerator.DeclareLocal(typeof(ConstraintInfo[]));
        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields;
        var constraintsParameters = methodParameters.AsSpan();
        var orderersLocal = DetectOrderers(ilGenerator, ref constraintsParameters, 0);

        SaveMethodConstraintParameters(ilGenerator, methodName, constraintsParameters, primaryKeyFields.Span,
            constraintsLocal);

        //call manipulator.FirstBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .LdcI4(RegisterLoadType(itemType))
            .Ldloc(constraintsLocal)
            .Ldloc(orderersLocal)
            .LdcI4(hasOrDefault ? 1 : 0)
            .Callvirt(
                _relationDbManipulatorType.GetMethod(
                    nameof(RelationDBManipulator<IRelation>.FirstByPrimaryKey))!.MakeGenericMethod(itemType));
    }

    void BuildFindByMethod(MethodInfo method, IILMethod reqMethod)
    {
        var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);

        var nameWithoutVariants = StripVariant(method.Name[6..], true);
        if (nameWithoutVariants.IndexName is "Id")
        {
            CreateMethodFindById(reqMethod.Generator, method.Name,
                method.GetParameters(), method.ReturnType, pushWriter, ctxLocFactory, nameWithoutVariants.HasOrDefault);
        }
        else
        {
            CreateMethodFindBy(reqMethod.Generator, method.Name, method.GetParameters(),
                method.ReturnType, pushWriter, ctxLocFactory, nameWithoutVariants.HasOrDefault,
                nameWithoutVariants.IndexName);
        }
    }

    void BuildRemoveByMethod(MethodInfo method, ParameterInfo[] methodParameters, IILMethod reqMethod)
    {
        var isPrefixBased = method.ReturnType == typeof(int); //returns number of removed items

        var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);

        WriteRelationPKPrefix(reqMethod.Generator, pushWriter);

        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields;

        var count = SaveMethodParameters(reqMethod.Generator, method.Name, methodParameters, primaryKeyFields.Span,
            pushWriter, ctxLocFactory);
        if (!isPrefixBased && count != primaryKeyFields.Length)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Number of parameters in {method.Name} does not match primary key count {primaryKeyFields.Length}.");

        //call manipulator.RemoveBy_
        reqMethod.Generator
            .Ldarg(0); //manipulator
        var localSpan = reqMethod.Generator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        reqMethod.Generator
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan);
        if (isPrefixBased)
        {
            reqMethod.Generator.Callvirt(
                (AllKeyPrefixesAreSame(ClientRelationVersionInfo, count)
                    ? _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>
                        .RemoveByKeyPrefixWithoutIterate))
                    : _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>
                        .RemoveByPrimaryKeyPrefix))!)!);
        }
        else
        {
            reqMethod.Generator.LdcI4(ShouldThrowWhenKeyNotFound(method.Name, method.ReturnType) ? 1 : 0);
            reqMethod.Generator.Callvirt(_relationDbManipulatorType.GetMethod(method.Name)!);
            if (method.ReturnType == typeof(void))
                reqMethod.Generator.Pop();
        }
    }

    void BuildRemoveByIdAdvancedParamMethod(MethodInfo method, ParameterInfo[] parameters, IILMethod reqMethod)
    {
        if (method.ReturnType != typeof(int))
            RelationInfoResolver.ActualOptions.ThrowBTDBException($"Return value in {method.Name} must be int.");

        var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);

        var advEnumParamOrder = (ushort)parameters.Length;
        var advEnumParam = parameters[advEnumParamOrder - 1].ParameterType;
        var advEnumParamType = advEnumParam.GenericTypeArguments[0];

        var prefixParamCount = parameters.Length - 1;

        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields.Span;
        var field = primaryKeyFields[prefixParamCount];

        if (parameters.Length != primaryKeyFields.Length)
            ForbidExcludePropositionInDebug(reqMethod.Generator, advEnumParamOrder, advEnumParam);
        ValidateAdvancedEnumParameter(field, advEnumParamType, method.Name);

        reqMethod.Generator.Ldarg(0); //manipulator for call RemoveByIdAdvancedParam

        WritePrimaryKeyPrefixFinishedByAdvancedEnumerator(method, parameters, reqMethod, prefixParamCount,
            advEnumParamOrder, advEnumParam, field, pushWriter, ctxLocFactory);
        reqMethod.Generator.Call(
            _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>
                .RemoveByIdAdvancedParam))!);
    }

    void WritePrimaryKeyPrefixFinishedByAdvancedEnumerator(MethodInfo method,
        ReadOnlySpan<ParameterInfo> parameters,
        IILMethod reqMethod, int prefixParamCount, ushort advEnumParamOrder, Type advEnumParam,
        TableFieldInfo field, Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory)
    {
        reqMethod.Generator
            .LdcI4(prefixParamCount)
            .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField(nameof(AdvancedEnumeratorParam<int>.Order))!);
        KeyPropositionStartBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
        SerializePKListPrefixBytes(reqMethod.Generator, method.Name,
            parameters[..^1], pushWriter, ctxLocFactory);
        KeyPropositionStartAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter);
        var label = KeyPropositionEndBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
        SerializePKListPrefixBytes(reqMethod.Generator, method.Name,
            parameters[..^1], pushWriter, ctxLocFactory);
        KeyPropositionEndAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter, label);
    }

    void WritePrimaryKeyPrefixFinishedByAdvancedEnumeratorWithoutOrder(MethodInfo method,
        ReadOnlySpan<ParameterInfo> parameters,
        IILMethod reqMethod, ushort advEnumParamOrder, Type advEnumParam, TableFieldInfo field)
    {
        var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
        reqMethod.Generator.Ldarg(0);
        KeyPropositionStartBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
        SerializePKListPrefixBytes(reqMethod.Generator, method.Name,
            parameters[..^1], pushWriter, ctxLocFactory);
        KeyPropositionStartAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter);
        var label = KeyPropositionEndBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
        SerializePKListPrefixBytes(reqMethod.Generator, method.Name,
            parameters[..^1], pushWriter, ctxLocFactory);
        KeyPropositionEndAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter, label);
    }

    void BuildRemoveByIdPartialMethod(MethodInfo method, ParameterInfo[] methodParameters, IILMethod reqMethod)
    {
        var isPrefixBased = method.ReturnType == typeof(int); //returns number of removed items

        if (!isPrefixBased || methodParameters.Length == 0 ||
            methodParameters[^1].ParameterType != typeof(int) ||
            methodParameters[^1].Name!
                .IndexOf("max", StringComparison.InvariantCultureIgnoreCase) == -1)
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException("Invalid shape of RemoveByIdPartial.");
        }

        var il = reqMethod.Generator;
        var (pushWriter, ctxLocFactory) = WriterPushers(il);

        WriteRelationPKPrefix(il, pushWriter);

        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields.Span;
        SaveMethodParameters(il, method.Name, methodParameters[..^1], primaryKeyFields, pushWriter, ctxLocFactory);

        var localSpan = il.DeclareLocal(typeof(ReadOnlySpan<byte>));

        il
            .Ldarg(0) //manipulator
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan)
            .Ldarg((ushort)methodParameters.Length)
            .Callvirt(_relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>
                .RemoveByPrimaryKeyPrefixPartial))!);
    }

    static bool AllKeyPrefixesAreSame(RelationVersionInfo relationInfo, ushort count)
    {
        foreach (var sk in relationInfo.SecondaryKeys)
        {
            var skFields = sk.Value;
            var idx = 0;
            foreach (var field in skFields.Fields)
            {
                if (!field.IsFromPrimaryKey)
                    return false;
                if (field.Index != idx)
                    return false;
                if (++idx == count)
                    break;
            }
        }

        return true;
    }

    void BuildListByIdMethod(MethodInfo method, IILMethod reqMethod)
    {
        var parameters = method.GetParameters();
        if (ParametersEndsWithAdvancedEnumeratorParam(parameters))
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);

            var advEnumParamOrder = (ushort)parameters.Length;
            var advEnumParam = parameters[advEnumParamOrder - 1].ParameterType;
            var advEnumParamType = advEnumParam.GenericTypeArguments[0];

            var prefixParamCount = parameters.Length - 1;

            var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields.Span;
            var field = primaryKeyFields[prefixParamCount];

            if (parameters.Length != primaryKeyFields.Length)
                ForbidExcludePropositionInDebug(reqMethod.Generator, advEnumParamOrder, advEnumParam);
            ValidateAdvancedEnumParameter(field, advEnumParamType, method.Name);

            reqMethod.Generator.Ldarg(0).Castclass(typeof(IRelationDbManipulator));

            WritePrimaryKeyPrefixFinishedByAdvancedEnumerator(method, parameters, reqMethod, prefixParamCount,
                advEnumParamOrder, advEnumParam, field, pushWriter, ctxLocFactory);

            if (ReturnTypeIsEnumeratorOrEnumerable(method, out var itemType))
            {
                //return new RelationAdvancedEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, loaderIndex);
                var enumType = typeof(RelationAdvancedEnumerator<>).MakeGenericType(itemType);
                var advancedEnumeratorCtor =
                    enumType.GetConstructors().Single(ci => ci.GetParameters().Length == 9);
                reqMethod.Generator
                    .LdcI4(RegisterLoadType(itemType))
                    .Newobj(advancedEnumeratorCtor);
            }
            else if (ReturnTypeIsIOrderedDictionaryEnumerator(method, advEnumParamType, out itemType))
            {
                reqMethod.Generator
                    .LdcI4(1); //init key reader

                //return new RelationAdvancedOrderedEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, initKeyReader, loaderIndex);
                var enumType =
                    typeof(RelationAdvancedOrderedEnumerator<,>).MakeGenericType(advEnumParamType,
                        itemType);
                var advancedEnumeratorCtor =
                    enumType.GetConstructors().Single(ci => ci.GetParameters().Length == 10);
                reqMethod.Generator
                    .LdcI4(RegisterLoadType(itemType))
                    .Newobj(advancedEnumeratorCtor);
            }
            else
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException("Invalid method " + method.Name);
            }
        }
        else
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
            reqMethod.Generator.Ldarg(0).Castclass(typeof(IRelationDbManipulator));
            SavePKListPrefixBytes(reqMethod.Generator, method.Name, parameters, pushWriter, ctxLocFactory);
            reqMethod.Generator.LdcI4(parameters.Length);

            if (ReturnTypeIsEnumeratorOrEnumerable(method, out var itemType))
            {
                //return new RelationAdvancedEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount, loaderIndex);
                var enumType = typeof(RelationAdvancedEnumerator<>).MakeGenericType(itemType);
                var advancedEnumeratorCtor =
                    enumType.GetConstructors().Single(ci => ci.GetParameters().Length == 4);
                reqMethod.Generator
                    .LdcI4(RegisterLoadType(itemType))
                    .Newobj(advancedEnumeratorCtor);
            }
            else
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException("Invalid method " + method.Name);
            }
        }
    }

    static bool TypeIsEnumeratorOrEnumerable(Type type, [NotNullWhen(true)] out Type? itemType)
    {
        itemType = type.SpecializationOf(typeof(IEnumerator<>)) ??
                   type.SpecializationOf(typeof(IEnumerable<>));

        if (itemType != null)
        {
            itemType = itemType.GenericTypeArguments[0];
        }

        return itemType != null;
    }

    static bool ReturnTypeIsEnumeratorOrEnumerable(MethodInfo method, [NotNullWhen(true)] out Type? itemType)
    {
        return TypeIsEnumeratorOrEnumerable(method.ReturnType, out itemType);
    }

    static bool ReturnTypeIsIOrderedDictionaryEnumerator(MethodInfo method, Type advEnumParamType,
        [NotNullWhen(true)] out Type? itemType)
    {
        itemType = method.ReturnType.SpecializationOf(typeof(IOrderedDictionaryEnumerator<,>));
        if (itemType != null)
        {
            var ta = itemType.GenericTypeArguments;
            if (ta[0] != advEnumParamType)
            {
                itemType = null;
                return false;
            }

            itemType = ta[1];
            return true;
        }

        return false;
    }


    void BuildCountByIdMethod(MethodInfo method, IILMethod reqMethod)
    {
        var parameters = method.GetParameters();
        var resultConversion = CheckLongLikeResult(method);
        if (ParametersEndsWithAdvancedEnumeratorParam(parameters))
        {
            PrepareAnyCountByIdWithAep(method, reqMethod, parameters);

            //return relationManipulator.CountWithProposition(prefixBytes,
            //    startKeyProposition, startKeyBytes, endKeyProposition, endKeyBytes);
            var calcCountMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.CountWithProposition));
            reqMethod.Generator.Call(calcCountMethod!);
        }
        else
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
            reqMethod.Generator.Ldarg(0);
            SavePKListPrefixBytes(reqMethod.Generator, method.Name, parameters, pushWriter, ctxLocFactory);

            //return relationManipulator.CountWithPrefix(prefixBytes);
            var calcCountMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.CountWithPrefix));
            reqMethod.Generator.Call(calcCountMethod!);
        }

        resultConversion(reqMethod.Generator);
    }

    void BuildAnyByIdMethod(MethodInfo method, IILMethod reqMethod)
    {
        var parameters = method.GetParameters();
        CheckReturnType(method.Name, typeof(bool), method.ReturnType);
        if (ParametersEndsWithAdvancedEnumeratorParam(parameters))
        {
            PrepareAnyCountByIdWithAep(method, reqMethod, parameters);

            //return relationManipulator.AnyWithProposition(prefixBytes,
            //    startKeyProposition, startKeyBytes, endKeyProposition, endKeyBytes);
            var calcCountMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.AnyWithProposition));
            reqMethod.Generator.Call(calcCountMethod!);
        }
        else
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
            reqMethod.Generator.Ldarg(0);
            SavePKListPrefixBytes(reqMethod.Generator, method.Name, parameters, pushWriter, ctxLocFactory);

            //return relationManipulator.AnyWithPrefix(prefixBytes);
            var calcCountMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.AnyWithPrefix));
            reqMethod.Generator.Call(calcCountMethod!);
        }
    }

    void PrepareAnyCountByIdWithAep(MethodInfo method, IILMethod reqMethod, ParameterInfo[] parameters)
    {
        var advEnumParamOrder = (ushort)parameters.Length;
        var advEnumParam = parameters[advEnumParamOrder - 1].ParameterType;
        var advEnumParamType = advEnumParam.GenericTypeArguments[0];

        var prefixParamCount = parameters.Length - 1;

        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields.Span;
        var field = primaryKeyFields[prefixParamCount];

        if (parameters.Length != primaryKeyFields.Length)
            ForbidExcludePropositionInDebug(reqMethod.Generator, advEnumParamOrder, advEnumParam);
        ValidateAdvancedEnumParameter(field, advEnumParamType, method.Name);

        WritePrimaryKeyPrefixFinishedByAdvancedEnumeratorWithoutOrder(method, parameters, reqMethod,
            advEnumParamOrder, advEnumParam, field);
    }

    void BuildListByMethod(MethodInfo method, IILMethod reqMethod)
    {
        var parameters = method.GetParameters();
        if (ParametersEndsWithAdvancedEnumeratorParam(parameters))
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
            var advEnumParamOrder = (ushort)parameters.Length;
            var advEnumParam = parameters[advEnumParamOrder - 1].ParameterType;
            var advEnumParamType = advEnumParam.GenericTypeArguments[0];

            var secondaryKeyIndex =
                ClientRelationVersionInfo.GetSecondaryKeyIndex(
                    StripVariant(method.Name[6..], false).IndexName);
            var prefixParamCount = parameters.Length - 1;

            var skFields = ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
            var field = skFields[prefixParamCount];

            if (parameters.Length != skFields.Length)
                ForbidExcludePropositionInDebug(reqMethod.Generator, advEnumParamOrder, advEnumParam);
            ValidateAdvancedEnumParameter(field, advEnumParamType, method.Name);

            reqMethod.Generator
                .Ldarg(0).Castclass(typeof(IRelationDbManipulator));

            reqMethod.Generator
                .LdcI4(prefixParamCount)
                .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField(nameof(AdvancedEnumeratorParam<int>.Order))!);

            var localRemapped = RemapSecondaryKeyIndex(reqMethod.Generator, secondaryKeyIndex);

            KeyPropositionStartBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
            SerializeListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
                parameters[..^1], pushWriter, ctxLocFactory, localRemapped);
            KeyPropositionStartAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter);

            var label = KeyPropositionEndBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
            SerializeListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
                parameters[..^1], pushWriter, ctxLocFactory, localRemapped);
            KeyPropositionEndAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter, label);

            reqMethod.Generator
                .Ldloc(localRemapped);

            if (ReturnTypeIsEnumeratorOrEnumerable(method, out var itemType))
            {
                //return new RelationAdvancedSecondaryKeyEnumerator<T>(relationManipulator,
                //    prefixLen, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, secondaryKeyIndex, loaderIndex);
                var enumType =
                    typeof(RelationAdvancedSecondaryKeyEnumerator<>).MakeGenericType(itemType);
                var advancedEnumeratorCtor =
                    enumType.GetConstructors().Single(ci => ci.GetParameters().Length == 10);
                reqMethod.Generator
                    .LdcI4(RegisterLoadType(itemType))
                    .Newobj(advancedEnumeratorCtor);
            }
            else if (ReturnTypeIsIOrderedDictionaryEnumerator(method, advEnumParamType, out itemType))
            {
                //return new RelationAdvancedOrderedSecondaryKeyEnumerator<T>(relationManipulator,
                //    prefixLen, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, secondaryKeyIndex, loaderIndex);
                var enumType =
                    typeof(RelationAdvancedOrderedSecondaryKeyEnumerator<,>).MakeGenericType(advEnumParamType,
                        itemType);
                var advancedEnumeratorCtor = enumType.GetConstructors()[0];
                reqMethod.Generator
                    .LdcI4(RegisterLoadType(itemType))
                    .Newobj(advancedEnumeratorCtor);
            }
            else
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException("Invalid method " + method.Name);
            }
        }
        else
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
            var secondaryKeyIndex =
                ClientRelationVersionInfo.GetSecondaryKeyIndex(
                    StripVariant(method.Name[6..], false).IndexName);

            reqMethod.Generator
                .Ldarg(0).Castclass(typeof(IRelationDbManipulator));
            var localRemapped = SaveListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
                parameters, pushWriter, ctxLocFactory);
            reqMethod.Generator
                .LdcI4(parameters.Length)
                .Ldloc(localRemapped);

            if (ReturnTypeIsEnumeratorOrEnumerable(method, out var itemType))
            {
                //return new RelationAdvancedSecondaryKeyEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount, secondaryKeyIndex, loaderIndex);
                var enumType =
                    typeof(RelationAdvancedSecondaryKeyEnumerator<>).MakeGenericType(itemType);
                var advancedEnumeratorCtor =
                    enumType.GetConstructors().Single(ci => ci.GetParameters().Length == 5);
                reqMethod.Generator
                    .LdcI4(RegisterLoadType(itemType))
                    .Newobj(advancedEnumeratorCtor);
            }
            else
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException("Invalid method " + method.Name);
            }
        }
    }

    [Conditional("DEBUG")]
    void ForbidExcludePropositionInDebug(IILGen ilGenerator, ushort advEnumParamOrder, Type advEnumParamType)
    {
        var propositionCheckFinished = ilGenerator.DefineLabel();
        ilGenerator
            .LdcI4((int)KeyProposition.Excluded)
            .Ldarg(advEnumParamOrder)
            .Ldfld(advEnumParamType.GetField(nameof(AdvancedEnumeratorParam<int>.StartProposition))!)
            .Ceq()
            .Brfalse(propositionCheckFinished)
            .Ldstr("Not supported Excluded proposition when listing by partial key.")
            .Newobj(() => new InvalidOperationException(null))
            .Throw()
            .Mark(propositionCheckFinished);
    }

    void BuildCountByMethod(MethodInfo method, IILMethod reqMethod)
    {
        var parameters = method.GetParameters();
        var secondaryKeyIndex =
            ClientRelationVersionInfo.GetSecondaryKeyIndex(method.Name.Substring(7));
        var resultConversion = CheckLongLikeResult(method);

        if (ParametersEndsWithAdvancedEnumeratorParam(parameters))
        {
            PrepareAnyCountByWithAep(method, reqMethod, parameters, secondaryKeyIndex);

            //return relationManipulator.CountWithProposition(prefixBytes,
            //    startKeyProposition, startKeyBytes, endKeyProposition, endKeyBytes);
            var calcCountMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.CountWithProposition));
            reqMethod.Generator.Call(calcCountMethod!);
        }
        else
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);

            reqMethod.Generator
                .Ldarg(0);
            var _ = SaveListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
                parameters, pushWriter, ctxLocFactory);

            //return relationManipulator.CountWithPrefix(prefixBytes);
            var calcCountMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.CountWithPrefix));
            reqMethod.Generator.Call(calcCountMethod!);
        }

        resultConversion(reqMethod.Generator);
    }

    void BuildAnyByMethod(MethodInfo method, IILMethod reqMethod)
    {
        var parameters = method.GetParameters();
        var secondaryKeyIndex =
            ClientRelationVersionInfo.GetSecondaryKeyIndex(method.Name.Substring(5));
        CheckReturnType(method.Name, typeof(bool), method.ReturnType);

        if (ParametersEndsWithAdvancedEnumeratorParam(parameters))
        {
            PrepareAnyCountByWithAep(method, reqMethod, parameters, secondaryKeyIndex);

            //return relationManipulator.AnyWithProposition(prefixBytes,
            //    startKeyProposition, startKeyBytes, endKeyProposition, endKeyBytes);
            var calcAnyMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.AnyWithProposition));
            reqMethod.Generator.Call(calcAnyMethod!);
        }
        else
        {
            var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
            reqMethod.Generator
                .Ldarg(0);
            var _ = SaveListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
                parameters, pushWriter, ctxLocFactory);

            //return relationManipulator.AnyWithPrefix(prefixBytes);
            var calcAnyMethod =
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.AnyWithPrefix));
            reqMethod.Generator.Call(calcAnyMethod!);
        }
    }

    void PrepareAnyCountByWithAep(MethodInfo method, IILMethod reqMethod, ParameterInfo[] parameters,
        uint secondaryKeyIndex)
    {
        var advEnumParamOrder = (ushort)parameters.Length;
        var advEnumParam = parameters[advEnumParamOrder - 1].ParameterType;
        var advEnumParamType = advEnumParam.GenericTypeArguments[0];

        var prefixParamCount = parameters.Length - 1;

        var skFields = ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
        var field = skFields[prefixParamCount];

        if (parameters.Length != skFields.Length)
            ForbidExcludePropositionInDebug(reqMethod.Generator, advEnumParamOrder, advEnumParam);
        ValidateAdvancedEnumParameter(field, advEnumParamType, method.Name);

        var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
        var localRemapped = RemapSecondaryKeyIndex(reqMethod.Generator, secondaryKeyIndex);
        reqMethod.Generator
            .Ldarg(0);

        KeyPropositionStartBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
        SerializeListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
            parameters[..^1], pushWriter, ctxLocFactory, localRemapped);
        KeyPropositionStartAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter);
        var label = KeyPropositionEndBefore(advEnumParamOrder, reqMethod.Generator, advEnumParam);
        SerializeListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
            parameters[..^1], pushWriter, ctxLocFactory, localRemapped);
        KeyPropositionEndAfter(advEnumParamOrder, reqMethod.Generator, advEnumParam, field, pushWriter, label);
    }

    Action<IILGen> CheckLongLikeResult(MethodInfo method)
    {
        var resultConversion =
            DefaultTypeConvertorGenerator.Instance.GenerateConversion(typeof(long), method.ReturnType);
        if (resultConversion == null)
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException("Invalid return type in method " + method.Name);
        }

        return resultConversion!;
    }

    void ValidateAdvancedEnumParameter(TableFieldInfo field, Type advEnumParamType, string methodName)
    {
        if (!field.Handler!.IsCompatibleWith(advEnumParamType, FieldHandlerOptions.Orderable))
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Parameter type mismatch in {methodName} (expected '{field.Handler.HandledType().ToSimpleName()}' but '{advEnumParamType.ToSimpleName()}' found).");
        }
    }

    void BuildUpdateByIdMethod(MethodInfo method, IILMethod reqMethod)
    {
        var returningBoolVariant = EnsureVoidOrBoolResult(method, method.Name);
        var parameters = method.GetParameters();
        var (pushWriter, ctxLocFactory) = WriterPushers(reqMethod.Generator);
        var valueSpan = StackAllocReadOnlySpan(reqMethod.Generator);
        var pkFields = ClientRelationVersionInfo.PrimaryKeyFields;
        if (parameters.Length < pkFields.Length)
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Not enough parameters in {method.Name} (expected at least {pkFields.Length}).");
        }

        SerializePKListPrefixBytes(reqMethod.Generator, method.Name, parameters.AsSpan(0, pkFields.Length), pushWriter,
            ctxLocFactory);
        var updateByIdStartMethod =
            _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.UpdateByIdStart));
        var keyBytesLocal = reqMethod.Generator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        reqMethod.Generator
            .Do(pushWriter)
            .Call(SpanWriterGetPersistentSpanAndResetMethodInfo)
            .Stloc(keyBytesLocal)
            .Ldarg(0)
            .Ldloc(keyBytesLocal)
            .Do(pushWriter)
            .Ldloca(valueSpan)
            .LdcI4(returningBoolVariant ? 0 : 1)
            .Call(updateByIdStartMethod!);
        if (returningBoolVariant)
        {
            var somethingToUpdateLabel = reqMethod.Generator.DefineLabel();
            reqMethod.Generator
                .BrtrueS(somethingToUpdateLabel)
                .LdcI4(0)
                .Ret()
                .Mark(somethingToUpdateLabel);
        }
        else
        {
            reqMethod.Generator.Pop();
        }

        var updateParams = parameters.AsSpan(pkFields.Length);
        // valueSpan contains oldValue
        // writer contains latest version id
        var readerLocal = reqMethod.Generator.DeclareLocal(typeof(SpanReader));
        var memoPosLocal = reqMethod.Generator.DeclareLocal(typeof(uint));
        IILLocal? ctxReaderLoc = null;
        reqMethod.Generator
            .Ldloc(valueSpan)
            .Newobj(typeof(SpanReader).GetConstructor(new[] { typeof(ReadOnlySpan<byte>) })!)
            .Stloc(readerLocal)
            .Ldloca(readerLocal)
            .Call(typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVUInt64))!);
        var valueFields = ClientRelationVersionInfo.Fields.Span;

        var copyMode = false;

        var usedParams = new HashSet<int>();

        foreach (var valueField in valueFields)
        {
            var paramIndex = -1;
            for (var j = 0; j < updateParams.Length; j++)
            {
                if (!string.Equals(updateParams[j].Name, valueField.Name, StringComparison.OrdinalIgnoreCase)) continue;
                paramIndex = j;
                break;
            }

            var newCopyMode = paramIndex == -1;
            if (copyMode != newCopyMode)
            {
                if (newCopyMode)
                {
                    RelationInfo.MemorizeCurrentPosition(reqMethod.Generator, il => il.Ldloca(readerLocal),
                        memoPosLocal);
                }
                else
                {
                    RelationInfo.CopyFromPos(reqMethod.Generator, il => il.Ldloca(readerLocal), memoPosLocal,
                        pushWriter);
                }

                copyMode = newCopyMode;
            }

            var handler = valueField.Handler!;
            if (!copyMode)
            {
                if (!usedParams.Add(paramIndex))
                {
                    RelationInfoResolver.ActualOptions.ThrowBTDBException(
                        $"Method {method.Name} matched parameter {updateParams[paramIndex].Name} more than once.");
                }

                var parameterType = updateParams[paramIndex].ParameterType;
                var specializedHandler = handler.SpecializeSaveForType(parameterType);
                var converter = RelationInfoResolver.TypeConvertorGenerator.GenerateConversion(parameterType,
                    specializedHandler.HandledType()!);
                if (converter == null)
                {
                    RelationInfoResolver.ActualOptions.ThrowBTDBException(
                        $"Method {method.Name} matched parameter {updateParams[paramIndex].Name} has wrong type {parameterType.ToSimpleName()} not convertible to {specializedHandler.HandledType().ToSimpleName()}");
                }

                var pushCtx = default(Action<IILGen>);
                if (specializedHandler.NeedsCtx())
                {
                    pushCtx = il => il.Ldloc(ctxLocFactory());
                }

                specializedHandler.Save(reqMethod.Generator, pushWriter, pushCtx, il =>
                {
                    il.Ldarg((ushort)(1 + pkFields.Length + paramIndex));
                    converter!(il);
                });
            }

            var pushReaderCtx = default(Action<IILGen>);
            if (handler.NeedsCtx())
            {
                if (ctxReaderLoc == null)
                {
                    ctxReaderLoc = reqMethod.Generator.DeclareLocal(typeof(IDBReaderCtx));
                    reqMethod.Generator
                        .Ldarg(0)
                        .Callvirt(() => ((IRelationDbManipulator)null)!.Transaction)
                        .Newobj(() => new DBReaderCtx(null))
                        .Stloc(ctxReaderLoc);
                }

                var loc = ctxReaderLoc;
                pushReaderCtx = il => il.Ldloc(loc);
            }

            handler.Skip(reqMethod.Generator, il => il.Ldloca(readerLocal), pushReaderCtx);
        }

        if (copyMode)
        {
            RelationInfo.CopyFromPos(reqMethod.Generator, il => il.Ldloca(readerLocal), memoPosLocal, pushWriter);
        }

        if (updateParams.Length != usedParams.Count)
        {
            var missing = new List<string>();
            for (var i = 0; i < updateParams.Length; i++)
            {
                if (!usedParams.Contains(i)) missing.Add(updateParams[i].Name);
            }

            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {method.Name} parameters {string.Join(", ", missing)} does not match any relation fields.");
        }

        var updateByIdFinishMethod =
            _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.UpdateByIdFinish));
        reqMethod.Generator
            .Ldarg(0)
            .Ldloc(keyBytesLocal)
            .Ldloc(valueSpan)
            .Do(pushWriter)
            .Call(SpanWriterGetPersistentSpanAndResetMethodInfo)
            .Call(updateByIdFinishMethod!);
        if (returningBoolVariant)
            reqMethod.Generator.LdcI4(1);
        reqMethod.Generator.Ret();
    }

    void BuildInsertMethod(MethodInfo method, IILMethod reqMethod)
    {
        var methodInfo = _relationDbManipulatorType.GetMethod(method.Name);
        var returningBoolVariant = EnsureVoidOrBoolResult(method, method.Name);

        var methodParams = method.GetParameters();
        CheckParameterCount(method.Name, 1, methodParams.Length);
        CheckParameterType(method.Name, 0, methodInfo!.GetParameters()[0].ParameterType,
            methodParams[0].ParameterType);
        reqMethod.Generator
            .Ldarg(0) //this
            .Ldarg(1)
            .Callvirt(methodInfo);
        if (returningBoolVariant) return;
        var returnedTrueLabel = reqMethod.Generator.DefineLabel("returnedTrueLabel");
        reqMethod.Generator
            .Brtrue(returnedTrueLabel)
            .Ldstr("Trying to insert duplicate key in " + _name)
            .Newobj(() => new BTDBException(null))
            .Throw()
            .Mark(returnedTrueLabel);
    }

    bool EnsureVoidOrBoolResult(MethodInfo method, string methodName)
    {
        var returningBoolVariant = false;
        var returnType = method.ReturnType;
        if (returnType == typeof(void))
        {
        }
        else if (returnType == typeof(bool))
            returningBoolVariant = true;
        else
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {methodName} should be defined with void or bool return type.");

        return returningBoolVariant;
    }

    void BuildManipulatorCallWithSameParameters(MethodInfo method, IILMethod reqMethod)
    {
        var methodParams = method.GetParameters();
        var paramCount = methodParams.Length;
        var methodInfo = _relationDbManipulatorType.GetMethod(method.Name);
        if (methodInfo == null)
            RelationInfoResolver.ActualOptions.ThrowBTDBException($"Method {method.Name} is not supported.");
        CheckReturnType(method.Name, methodInfo!.ReturnType, method.ReturnType);
        var calledMethodParams = methodInfo.GetParameters();
        CheckParameterCount(method.Name, calledMethodParams.Length, methodParams.Length);
        for (var i = 0; i < methodParams.Length; i++)
        {
            CheckParameterType(method.Name, i, calledMethodParams[i].ParameterType, methodParams[i].ParameterType);
        }

        if (method.Name == nameof(RelationDBManipulator<object>.ShallowUpsertWithSizes))
        {
            ForbidSecondaryKeys(method);
        }

        for (ushort i = 0; i <= paramCount; i++)
            reqMethod.Generator.Ldarg(i);
        reqMethod.Generator.Callvirt(methodInfo);
    }

    void ForbidSecondaryKeys(MethodInfo method)
    {
        if (ClientRelationVersionInfo.SecondaryKeys.Count > 0)
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {method.Name} cannot be used with relation with secondary indexes");
        }
    }

    void CheckParameterType(string name, int parIdx, Type expectedType, Type actualType)
    {
        if (expectedType != actualType)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {name} expects {parIdx}th parameter of type {expectedType.Name}.");
    }

    void CheckParameterCount(string name, int expectedParameterCount, int actualParameterCount)
    {
        if (expectedParameterCount != actualParameterCount)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {name} expects {expectedParameterCount} parameters count.");
    }

    void CheckReturnType(string name, Type expectedReturnType, Type returnType)
    {
        if (returnType != expectedReturnType)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {name} should be defined with {expectedReturnType.Name} return type.");
    }

    void CreateMethodRemoveWithSizesById(IILGen ilGenerator, string methodName,
        ParameterInfo[] methodParameters, Type returnType)
    {
        CheckReturnType(methodName, typeof((ulong Count, ulong KeySizes, ulong ValueSizes)), returnType);
        var constraintsLocal = ilGenerator.DeclareLocal(typeof(ConstraintInfo[]));
        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields;
        var constraintsParameters = methodParameters.AsSpan();

        SaveMethodConstraintParameters(ilGenerator, methodName, constraintsParameters, primaryKeyFields.Span,
            constraintsLocal);

        //call manipulator.RemoveWithSizesBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .Ldloc(constraintsLocal);
        ilGenerator.Callvirt(
            _relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.RemoveWithSizesByPrimaryKey))!);
    }

    void CreateMethodGatherById(IILGen ilGenerator, string methodName,
        ParameterInfo[] methodParameters)
    {
        var itemType = ParseGatherParams(methodParameters, methodName);
        var constraintsLocal = ilGenerator.DeclareLocal(typeof(ConstraintInfo[]));
        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields;
        var constraintsParameters = methodParameters.AsSpan(3..);
        var orderersLocal = DetectOrderers(ilGenerator, ref constraintsParameters, 3);

        SaveMethodConstraintParameters(ilGenerator, methodName, constraintsParameters, primaryKeyFields.Span,
            constraintsLocal, 3);

        //call manipulator.GatherBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .LdcI4(RegisterLoadType(itemType!))
            .Ldloc(constraintsLocal)
            .Ldarg(1)
            .Castclass(typeof(ICollection<>).MakeGenericType(itemType))
            .Ldarg(2)
            .Ldarg(3)
            .Ldloc(orderersLocal);
        ilGenerator.Callvirt(
            _relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.GatherByPrimaryKey))!.MakeGenericMethod(itemType));
    }

    static IILLocal DetectOrderers(IILGen ilGenerator, ref Span<ParameterInfo> constraintsParameters,
        ushort addParamIdx)
    {
        var orderersLocal = ilGenerator.DeclareLocal(typeof(IOrderer[]));
        if (constraintsParameters.Length > 0 && constraintsParameters[^1].ParameterType == typeof(IOrderer[]))
        {
            ilGenerator
                .Ldarg((ushort)(constraintsParameters.Length + addParamIdx))
                .Stloc(orderersLocal);
            constraintsParameters = constraintsParameters[..^1];
        }
        else
        {
            ilGenerator
                .Ldnull()
                .Stloc(orderersLocal);
        }

        return orderersLocal;
    }

    void CreateMethodGatherBy(IILGen ilGenerator, string methodName,
        ParameterInfo[] methodParameters, string skName)
    {
        var itemType = ParseGatherParams(methodParameters, methodName);
        var constraintsLocal = ilGenerator.DeclareLocal(typeof(ConstraintInfo[]));

        var skIndex = ClientRelationVersionInfo.GetSecondaryKeyIndex(skName);

        var secondaryKeyFields = ClientRelationVersionInfo.GetSecondaryKeyFields(skIndex);

        var constraintsParameters = methodParameters.AsSpan(3..);
        var orderersLocal = DetectOrderers(ilGenerator, ref constraintsParameters, 3);
        SaveMethodConstraintParameters(ilGenerator, methodName, constraintsParameters, secondaryKeyFields,
            constraintsLocal, 3);

        //call manipulator.GatherBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .LdcI4(RegisterLoadType(itemType!))
            .Ldloc(constraintsLocal)
            .Ldarg(1)
            .Castclass(typeof(ICollection<>).MakeGenericType(itemType))
            .Ldarg(2)
            .Ldarg(3)
            .LdcI4((int)skIndex)
            .Ldloc(orderersLocal);
        ilGenerator.Callvirt(
            _relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.GatherBySecondaryKey))!.MakeGenericMethod(itemType));
    }

    Type ParseGatherParams(ParameterInfo[] methodParameters, string methodName)
    {
        if (methodParameters.Length < 3)
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {methodName} expects at least 3 parameters.");
        }

        var collection = methodParameters[0].ParameterType.SpecializationOf(typeof(ICollection<>));

        if (collection == null)
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {methodName} first parameter must inherit from ICollection<>.");
        }

        if (methodParameters[1].Name != "skip" || methodParameters[1].ParameterType != typeof(long))
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {methodName} second parameter must be long type and named skip");
        }

        if (methodParameters[2].Name != "take" || methodParameters[2].ParameterType != typeof(long))
        {
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {methodName} second parameter must be long type and named take");
        }

        return collection!.GetGenericArguments()[0];
    }

    void CreateMethodScanById(IILGen ilGenerator, string methodName,
        ParameterInfo[] methodParameters, Type methodReturnType)
    {
        var constraintsLocal = ilGenerator.DeclareLocal(typeof(ConstraintInfo[]));
        var isPrefixBased = TypeIsEnumeratorOrEnumerable(methodReturnType, out var itemType);
        if (!isPrefixBased)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {methodName} must return IEnumerable<T> or IEnumerator<T> type.");

        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields;

        SaveMethodConstraintParameters(ilGenerator, methodName, methodParameters, primaryKeyFields.Span,
            constraintsLocal);

        //call manipulator.ScanBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .LdcI4(RegisterLoadType(itemType!))
            .Ldloc(constraintsLocal);
        ilGenerator.Callvirt(
            _relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.ScanByPrimaryKeyPrefix))!.MakeGenericMethod(itemType));
        ilGenerator.Castclass(methodReturnType);
    }

    void CreateMethodScanBy(IILGen ilGenerator, string methodName,
        ParameterInfo[] methodParameters, Type methodReturnType)
    {
        var constraintsLocal = ilGenerator.DeclareLocal(typeof(ConstraintInfo[]));
        var isPrefixBased = TypeIsEnumeratorOrEnumerable(methodReturnType, out var itemType);
        if (!isPrefixBased)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Method {methodName} must return IEnumerable<T> or IEnumerator<T> type.");

        var skName = StripVariant(methodName[6..], false).IndexName;
        var skIndex = ClientRelationVersionInfo.GetSecondaryKeyIndex(skName);

        var secondaryKeyFields = ClientRelationVersionInfo.GetSecondaryKeyFields(skIndex);

        SaveMethodConstraintParameters(ilGenerator, methodName, methodParameters, secondaryKeyFields, constraintsLocal);

        //call manipulator.ScanBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .LdcI4(RegisterLoadType(itemType!))
            .Ldloc(constraintsLocal)
            .LdcI4((int)skIndex);
        ilGenerator.Callvirt(
            _relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.ScanBySecondaryKeyPrefix))!.MakeGenericMethod(itemType));
        ilGenerator.Castclass(methodReturnType);
    }

    void SaveMethodConstraintParameters(IILGen ilGenerator, string methodName,
        ReadOnlySpan<ParameterInfo> methodParameters,
        ReadOnlySpan<TableFieldInfo> fields, IILLocal constraintsLocal, int addArg = 0)
    {
        if (methodParameters.Length > fields.Length)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Number of constraints parameters in {methodName} is bigger than key count {fields.Length}.");

        ilGenerator
            .LdcI4(methodParameters.Length)
            .Newarr(typeof(ConstraintInfo))
            .Stloc(constraintsLocal);

        var idx = 0;
        foreach (var field in fields)
        {
            if (idx == methodParameters.Length)
            {
                break;
            }

            var par = methodParameters[idx++];
            if (string.Compare(field.Name, par.Name, StringComparison.OrdinalIgnoreCase) != 0)
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException(
                    $"Parameter and key mismatch in {methodName}, {field.Name}!={par.Name}.");
            }

            var constraintType = par.ParameterType.SpecializationOf(typeof(Constraint<>));

            if (constraintType == null)
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException(
                    $"Parameter {par.Name} in {methodName} is not implementation of Constraint<T>.");
            }

            var constraintGenericType = constraintType!.GetGenericArguments()[0];

            if (!field.Handler!.IsCompatibleWith(constraintGenericType, FieldHandlerOptions.Orderable))
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException(
                    $"Parameter constraint type mismatch in {methodName} (expected '{field.Handler.HandledType().ToSimpleName()}' but '{constraintGenericType.ToSimpleName()}' found).");
            }

            ilGenerator
                .Ldloc(constraintsLocal)
                .LdcI4(idx - 1)
                .Ldelema(typeof(ConstraintInfo))
                .Ldarg((ushort)(idx + addArg))
                .Castclass(typeof(IConstraint))
                .Stfld(typeof(ConstraintInfo).GetField(nameof(ConstraintInfo.Constraint))!);
        }
    }

    void CreateMethodFindById(IILGen ilGenerator, string methodName,
        ParameterInfo[] methodParameters, Type methodReturnType,
        Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory, bool hasOrDefault)
    {
        var spanLocal = ilGenerator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        var isPrefixBased = TypeIsEnumeratorOrEnumerable(methodReturnType, out var itemType);
        WriteRelationPKPrefix(ilGenerator, pushWriter);

        var primaryKeyFields = ClientRelationVersionInfo.PrimaryKeyFields;

        var count = SaveMethodParameters(ilGenerator, methodName, methodParameters, primaryKeyFields.Span,
            pushWriter, ctxLocFactory);
        if (!isPrefixBased && count != primaryKeyFields.Length)
            RelationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Number of parameters in {methodName} does not match primary key count {primaryKeyFields.Length}.");

        //call manipulator.FindBy_
        ilGenerator
            .Ldarg(0) //manipulator
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(spanLocal)
            .Ldloca(spanLocal);

        if (isPrefixBased)
        {
            ilGenerator.LdcI4(RegisterLoadType(itemType));
            ilGenerator.Callvirt(
                _relationDbManipulatorType.GetMethod(
                    nameof(RelationDBManipulator<IRelation>.FindByPrimaryKeyPrefix))!.MakeGenericMethod(itemType));
            ilGenerator.Castclass(methodReturnType);
        }
        else
        {
            itemType = methodReturnType;
            ilGenerator.LdcI4(hasOrDefault ? 0 : 1);
            ilGenerator.LdcI4(RegisterLoadType(itemType));
            ilGenerator.Callvirt(
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.FindByIdOrDefault))!
                    .MakeGenericMethod(itemType));
        }
    }

    void CreateMethodFindBy(IILGen ilGenerator, string methodName,
        ParameterInfo[] methodParameters, Type methodReturnType,
        Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory, bool hasOrDefault, string skName)
    {
        var skIndex = ClientRelationVersionInfo.GetSecondaryKeyIndex(skName);
        var localRemapped = RemapSecondaryKeyIndex(ilGenerator, skIndex);
        WriteRelationSKPrefix(ilGenerator, pushWriter, localRemapped);

        var secondaryKeyFields = ClientRelationVersionInfo.GetSecondaryKeyFields(skIndex);
        SaveMethodParameters(ilGenerator, methodName, methodParameters, secondaryKeyFields, pushWriter,
            ctxLocFactory);

        var localSpan = ilGenerator.DeclareLocal(typeof(ReadOnlySpan<byte>));

        //call public T FindBySecondaryKeyOrDefault<T>(uint secondaryKeyIndex, ByteBuffer secKeyBytes, bool throwWhenNotFound, int loaderIndex)
        ilGenerator
            .Ldarg(0) //manipulator
            .Ldloc(localRemapped)
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan);

        if (TypeIsEnumeratorOrEnumerable(methodReturnType, out var itemType))
        {
            ilGenerator.LdcI4(RegisterLoadType(itemType));
            ilGenerator.Callvirt(
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>.FindBySecondaryKey))!
                    .MakeGenericMethod(itemType));
            ilGenerator.Castclass(methodReturnType);
        }
        else
        {
            ilGenerator.LdcI4(hasOrDefault ? 0 : 1); //? should throw
            ilGenerator.LdcI4(RegisterLoadType(methodReturnType));
            ilGenerator.Callvirt(
                _relationDbManipulatorType.GetMethod(nameof(RelationDBManipulator<IRelation>
                    .FindBySecondaryKeyOrDefault))!.MakeGenericMethod(
                    methodReturnType));
        }
    }

    (string IndexName, bool HasOrDefault) StripVariant(string name, bool withOrDefault)
    {
        (string IndexName, bool HasOrDefault) result = ("", false);

        void Check(string id)
        {
            if (!name.StartsWith(id)) return;
            if (withOrDefault)
            {
                if (name[id.Length..].StartsWith("OrDefault"))
                {
                    if (result.IndexName.Length < id.Length)
                    {
                        result = (id, true);
                        return;
                    }
                }
            }

            if (result.IndexName.Length < id.Length)
            {
                result = (id, false);
            }
        }

        Check("Id");
        foreach (var secondaryKeyName in ClientRelationVersionInfo.SecondaryKeys.Values.Select(s =>
                     s.Name))
        {
            Check(secondaryKeyName);
        }

        return result.IndexName.Length == 0 ? (name, false) : result;
    }

    ushort SaveMethodParameters(IILGen ilGenerator, string methodName,
        ReadOnlySpan<ParameterInfo> methodParameters,
        ReadOnlySpan<TableFieldInfo> fields, Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory)
    {
        var idx = 0;
        foreach (var field in fields)
        {
            if (idx == methodParameters.Length)
            {
                break;
            }

            var par = methodParameters[idx++];
            if (string.Compare(field.Name, par.Name!.ToLower(), StringComparison.OrdinalIgnoreCase) != 0)
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException(
                    $"Parameter and key mismatch in {methodName}, {field.Name}!={par.Name}.");
            }

            if (!field.Handler!.IsCompatibleWith(par.ParameterType, FieldHandlerOptions.Orderable))
            {
                RelationInfoResolver.ActualOptions.ThrowBTDBException(
                    $"Parameter type mismatch in {methodName} (expected '{field.Handler.HandledType().ToSimpleName()}' but '{par.ParameterType.ToSimpleName()}' found).");
            }

            SaveKeyFieldFromArgument(ilGenerator, field, idx, par.ParameterType, pushWriter, ctxLocFactory);
        }

        return (ushort)idx;
    }

    bool ShouldThrowWhenKeyNotFound(string methodName, Type methodReturnType)
    {
        if (methodName.StartsWith("RemoveBy") || methodName.StartsWith("ShallowRemoveBy"))
            return methodReturnType == typeof(void);
        if (StripVariant(methodName[6..], true) == ("Id", true))
            return false;
        return true;
    }

    static void KeyPropositionStartBefore(ushort advEnumParamOrder, IILGen ilGenerator, Type advEnumParamType)
    {
        ilGenerator
            .Ldarg(advEnumParamOrder)
            .Ldfld(advEnumParamType.GetField(nameof(AdvancedEnumeratorParam<int>.StartProposition))!);
    }

    static void KeyPropositionStartAfter(ushort advEnumParamOrder, IILGen ilGenerator, Type advEnumParamType,
        TableFieldInfo field, Action<IILGen> pushWriter)
    {
        var instField = advEnumParamType.GetField(nameof(AdvancedEnumeratorParam<int>.Start));
        var ignoreLabel = ilGenerator.DefineLabel("start_ignore");
        var localSpan = ilGenerator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        ilGenerator
            .Do(pushWriter)
            .Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.GetCurrentPosition))!)
            .ConvI4()
            .Ldarg(advEnumParamOrder)
            .Ldfld(advEnumParamType.GetField(nameof(AdvancedEnumeratorParam<int>.StartProposition))!)
            .LdcI4((int)KeyProposition.Ignored)
            .BeqS(ignoreLabel);
        field.Handler!.SpecializeSaveForType(instField!.FieldType).Save(ilGenerator,
            pushWriter, null,
            il => il.Ldarg(advEnumParamOrder).Ldfld(instField));
        ilGenerator
            .Mark(ignoreLabel)
            .Do(pushWriter)
            .Call(SpanWriterGetPersistentSpanAndResetMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan);
    }

    static IILLabel KeyPropositionEndBefore(ushort advEnumParamOrder, IILGen ilGenerator, Type advEnumParamType)
    {
        var ignoreLabel = ilGenerator.DefineLabel("end_ignore");
        ilGenerator
            .Ldarg(advEnumParamOrder)
            .Ldfld(advEnumParamType.GetField(nameof(AdvancedEnumeratorParam<int>.EndProposition))!)
            .Dup()
            .LdcI4((int)KeyProposition.Ignored)
            .Beq(ignoreLabel);
        return ignoreLabel;
    }

    static void KeyPropositionEndAfter(ushort advEnumParamOrder, IILGen ilGenerator, Type advEnumParamType,
        TableFieldInfo field, Action<IILGen> pushWriter, IILLabel ignoreLabel)
    {
        var instField = advEnumParamType.GetField(nameof(AdvancedEnumeratorParam<int>.End));
        var localSpan = ilGenerator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        field.Handler!.SpecializeSaveForType(instField!.FieldType).Save(ilGenerator,
            pushWriter, null,
            il => il.Ldarg(advEnumParamOrder).Ldfld(instField));
        ilGenerator
            .Mark(ignoreLabel)
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan);
    }

    IILLocal SaveListPrefixBytes(uint secondaryKeyIndex, IILGen ilGenerator, string methodName,
        ReadOnlySpan<ParameterInfo> methodParameters, Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory)
    {
        var localRemapped = RemapSecondaryKeyIndex(ilGenerator, secondaryKeyIndex);

        SerializeListPrefixBytes(secondaryKeyIndex, ilGenerator, methodName, methodParameters, pushWriter,
            ctxLocFactory, localRemapped);

        var localSpan = ilGenerator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        ilGenerator
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan);
        return localRemapped;
    }

    void SerializeListPrefixBytes(uint secondaryKeyIndex, IILGen ilGenerator, string methodName,
        ReadOnlySpan<ParameterInfo> methodParameters,
        Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory, IILLocal localRemapped)
    {
        WriteRelationSKPrefix(ilGenerator, pushWriter, localRemapped);

        var secondaryKeyFields = ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
        SaveMethodParameters(ilGenerator, methodName, methodParameters,
            secondaryKeyFields, pushWriter, ctxLocFactory);
    }

    static IILLocal StackAllocReadOnlySpan(IILGen ilGenerator, int len = 1024)
    {
        var spanLoc = ilGenerator.DeclareLocal(typeof(ReadOnlySpan<byte>));

        ilGenerator
            .Localloc((uint)len)
            .LdcI4(len)
            .Newobj(typeof(ReadOnlySpan<byte>).GetConstructor(new[] { typeof(void*), typeof(int) })!)
            .Stloc(spanLoc);

        return spanLoc;
    }

    static (Action<IILGen>, Func<IILLocal>) WriterPushers(IILGen ilGenerator)
    {
        var bufPtrLoc = ilGenerator.DeclareLocal(typeof(byte*));
        var writerLoc = ilGenerator.DeclareLocal(typeof(SpanWriter));

        ilGenerator
            .Localloc(512)
            .Stloc(bufPtrLoc)
            .Ldloca(writerLoc)
            .Ldloc(bufPtrLoc)
            .LdcI4(512)
            .Call(typeof(SpanWriter).GetConstructor(new[] { typeof(void*), typeof(int) })!);

        void PushWriter(IILGen il) => il.Ldloca(writerLoc);

        IILLocal? ctxLoc = null;

        IILLocal PushCtx()
        {
            if (ctxLoc == null)
            {
                ctxLoc = ilGenerator.DeclareLocal(typeof(IDBWriterCtx));
                ilGenerator
                    .Ldarg(0)
                    .Callvirt(() => ((IRelationDbManipulator)null).Transaction)
                    .Newobj(() => new DBWriterCtx(null))
                    .Stloc(ctxLoc);
            }

            return ctxLoc;
        }

        return (PushWriter, PushCtx);
    }

    void SavePKListPrefixBytes(IILGen ilGenerator, string methodName, ReadOnlySpan<ParameterInfo> methodParameters,
        Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory)
    {
        SerializePKListPrefixBytes(ilGenerator, methodName, methodParameters, pushWriter, ctxLocFactory);

        var localSpan = ilGenerator.DeclareLocal(typeof(ReadOnlySpan<byte>));
        ilGenerator
            .Do(pushWriter)
            .Call(SpanWriterGetSpanMethodInfo)
            .Stloc(localSpan)
            .Ldloca(localSpan);
    }

    void SerializePKListPrefixBytes(IILGen ilGenerator, string methodName,
        ReadOnlySpan<ParameterInfo> methodParameters, Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory)
    {
        WriteRelationPKPrefix(ilGenerator, pushWriter);

        var keyFields = ClientRelationVersionInfo.PrimaryKeyFields;
        SaveMethodParameters(ilGenerator, methodName, methodParameters,
            keyFields.Span, pushWriter, ctxLocFactory);
    }

    static void SaveKeyFieldFromArgument(IILGen ilGenerator, TableFieldInfo field, int parameterId,
        Type parameterType, Action<IILGen> pushWriter, Func<IILLocal> ctxLocFactory)
    {
        var specialized = field.Handler!.SpecializeSaveForType(parameterType);
        specialized
            .Save(ilGenerator,
                pushWriter, il => il.Ldloc(ctxLocFactory()),
                il => il.Ldarg((ushort)parameterId));
    }

    void WriteRelationPKPrefix(IILGen ilGenerator, Action<IILGen> pushWriter)
    {
        ilGenerator
            .Ldarg(0)
            .Do(pushWriter)
            .Call(_relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.WriteRelationPKPrefix))!);
    }

    IILLocal RemapSecondaryKeyIndex(IILGen ilGenerator, uint secondaryKeyIndex)
    {
        var local = ilGenerator.DeclareLocal(typeof(uint));
        ilGenerator
            .Ldarg(0)
            .LdcI4((int)secondaryKeyIndex)
            .Call(_relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.RemapPrimeSK))!)
            .Stloc(local);
        return local;
    }

    void WriteRelationSKPrefix(IILGen ilGenerator, Action<IILGen> pushWriter, IILLocal localRemapped)
    {
        ilGenerator
            .Ldarg(0)
            .Do(pushWriter)
            .Ldloc(localRemapped)
            .Call(_relationDbManipulatorType.GetMethod(
                nameof(RelationDBManipulator<IRelation>.WriteRelationSKPrefix))!);
    }
}
