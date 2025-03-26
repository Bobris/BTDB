using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.IOC;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Extensions = BTDB.FieldHandler.Extensions;

namespace BTDB.ODBLayer;

delegate void RelationLoader(IInternalObjectDBTransaction transaction, ref MemReader reader, object value);

delegate object RelationLoaderFunc(IInternalObjectDBTransaction transaction, ref MemReader reader);

delegate int RelationSaver(IInternalObjectDBTransaction transaction, ref MemWriter writer, object value);

delegate bool RelationBeforeRemove(IInternalObjectDBTransaction transaction, IContainer container, object value);

public class RelationInfo
{
    public readonly uint _id;
    public readonly string _name;
    readonly IRelationInfoResolver _relationInfoResolver;
    public readonly Type _interfaceType;
    public readonly Type _clientType;
    readonly object _defaultClientObject;

    RelationVersionInfo?[] _relationVersions = [];
    RelationSaver _primaryKeysSaver;
    RelationSaver _valueSaver;
    RelationBeforeRemove? _beforeRemove;
    bool _hasInKeyValue;

    internal StructList<ItemLoaderInfo> ItemLoaderInfos;

    public class ItemLoaderInfo
    {
        readonly RelationInfo _owner;
        readonly Type _itemType;
        readonly ReadOnlyMemory<TableFieldInfo> _pkFields;

        public ItemLoaderInfo(RelationInfo owner, Type itemType)
        {
            _owner = owner;
            _itemType = itemType;
            _pkFields = _owner.ClientRelationVersionInfo.PrimaryKeyFields;
            _valueLoaders = new RelationLoader?[_owner._relationVersions.Length];
            _primaryKeysLoader = CreatePkLoader(itemType, _owner.ClientRelationVersionInfo.PrimaryKeyFields.Span,
                $"RelationKeyLoader_{_owner.Name}_{itemType.ToSimpleName()}", out _primaryKeyIsEnough,
                out _loadAsMemory);
        }

        public ItemLoaderInfo(RelationInfo owner, Type itemType, ReadOnlyMemory<TableFieldInfo> pkFields)
        {
            _owner = owner;
            _itemType = itemType;
            _pkFields = pkFields;
            _valueLoaders = new RelationLoader?[_owner._relationVersions.Length];
            _primaryKeysLoader = CreatePkLoader(itemType, pkFields.Span,
                $"RelationKeyLoader_{_owner.Name}_{itemType.ToSimpleName()}", out _primaryKeyIsEnough,
                out _loadAsMemory);
        }

        [SkipLocalsInit]
        internal unsafe object CreateInstance(IInternalObjectDBTransaction tr, IKeyValueDBCursor cursor,
            scoped in ReadOnlySpan<byte> keyBytes)
        {
            fixed (void* pKeyBytes = keyBytes)
            {
                var reader = new MemReader(pKeyBytes, keyBytes.Length);
                reader.SkipBlock((uint)_owner.Prefix.Length);
                var obj = _primaryKeysLoader(tr, ref reader);
                if (_primaryKeyIsEnough) return obj;
                if (_loadAsMemory)
                {
                    var buffer = new Memory<byte>();
                    var valueBytes = cursor.GetValueMemory(ref buffer);
                    reader = MemReader.CreateFromReadOnlyMemory(valueBytes);
                    var version = reader.ReadVUInt32();
                    GetValueLoader(version)(tr, ref reader, obj);
                    return obj;
                }
                else
                {
                    Span<byte> buf = stackalloc byte[4096];
                    var valueSpan = cursor.GetValueSpan(ref buf);
                    fixed (void* _ = valueSpan)
                    {
                        reader = MemReader.CreateFromPinnedSpan(valueSpan);
                        var version = reader.ReadVUInt32();
                        GetValueLoader(version)(tr, ref reader, obj);
                        return obj;
                    }
                }
            }
        }

        internal unsafe object CreateInstance(IInternalObjectDBTransaction tr, in ReadOnlySpan<byte> keyBytes,
            in ReadOnlySpan<byte> valueBytes)
        {
            fixed (void* pKeyBytes = keyBytes)
            {
                var reader = new MemReader(pKeyBytes, keyBytes.Length);
                reader.Skip1Byte(); // 3
                reader.SkipVUInt64(); // RelationId
                var obj = _primaryKeysLoader(tr, ref reader);
                if (_primaryKeyIsEnough) return obj;
                fixed (void* _ = valueBytes)
                {
                    reader = MemReader.CreateFromPinnedSpan(valueBytes);
                    var version = reader.ReadVUInt32();
                    GetValueLoader(version)(tr, ref reader, obj);
                    return obj;
                }
            }
        }

        internal readonly RelationLoaderFunc _primaryKeysLoader;
        internal readonly bool _primaryKeyIsEnough;
        readonly RelationLoader?[] _valueLoaders;
        internal readonly bool _loadAsMemory;

        internal RelationLoader GetValueLoader(uint version)
        {
            RelationLoader? res;
            do
            {
                res = _valueLoaders[version];
                if (res != null) return res;
                StructList<TableFieldInfo> filteredFields = new();
                foreach (var tableFieldInfo in _owner._relationVersions[version]!.Fields.Span)
                {
                    if (tableFieldInfo.Computed) continue;
                    filteredFields.Add(tableFieldInfo);
                }

                res = CreateLoader(_itemType,
                    filteredFields,
                    $"RelationValueLoader_{_owner.Name}_{version}_{_itemType.ToSimpleName()}");
            } while (Interlocked.CompareExchange(ref _valueLoaders[version], res, null) != null);

            return res;
        }

        RelationLoaderFunc CreatePkLoader(Type instanceType, ReadOnlySpan<TableFieldInfo> fields, string loaderName,
            out bool primaryKeyIsEnough, out bool loadAsMemory)
        {
            loadAsMemory = false;
            var thatType = typeof(Func<>).MakeGenericType(instanceType);
            var method = ILBuilder.Instance.NewMethod(
                loaderName, typeof(RelationLoaderFunc), typeof(Func<object>));
            var ilGenerator = method.Generator;
            var container = _owner._relationInfoResolver.Container;
            object that = null;
            if (container != null)
            {
                that = container.ResolveOptional(thatType);
            }

            ilGenerator.DeclareLocal(instanceType);
            if (that == null)
            {
                var defaultConstructor = instanceType.GetDefaultConstructor();
                if (defaultConstructor == null)
                {
                    ilGenerator
                        .Ldtoken(instanceType)
                        .Call(() => Type.GetTypeFromHandle(new()))
                        .Call(() => RuntimeHelpers.GetUninitializedObject(null));
                }
                else
                {
                    ilGenerator
                        .Newobj(defaultConstructor);
                }
            }
            else
            {
                ilGenerator
                    .Ldarg(0)
                    .Callvirt(thatType.GetMethod(nameof(Func<object>.Invoke))!);
            }

            ilGenerator
                .Stloc(0);

            var loadInstructions = new StructList<(IFieldHandler, Action<IILGen>?, MethodInfo?)>();
            var props = instanceType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic |
                                                   BindingFlags.Instance).Where(pi =>
                pi.GetCustomAttribute<NotStoredAttribute>(true) == null &&
                pi.GetIndexParameters().Length == 0).ToList();
            var usedFields = 0;
            foreach (var srcFieldInfo in fields)
            {
                var fieldInfo = props.FirstOrDefault(p => GetPersistentName(p) == srcFieldInfo.Name);
                if (fieldInfo != null)
                {
                    usedFields++;
                    var setterMethod = fieldInfo.GetAnySetMethod();
                    var fieldType = setterMethod!.GetParameters()[0].ParameterType;
                    var specializedSrcHandler =
                        srcFieldInfo.Handler!.SpecializeLoadForType(fieldType, null,
                            _owner._relationInfoResolver.FieldHandlerLogger);
                    loadAsMemory |= specializedSrcHandler.DoesPreferLoadAsMemory();
                    var willLoad = specializedSrcHandler.HandledType();
                    var converterGenerator =
                        _owner._relationInfoResolver.TypeConvertorGenerator
                            .GenerateConversion(willLoad!, fieldType);
                    if (converterGenerator != null)
                    {
                        loadInstructions.Add((specializedSrcHandler, converterGenerator, setterMethod));
                        continue;
                    }

                    _owner._relationInfoResolver.FieldHandlerLogger?.ReportTypeIncompatibility(willLoad,
                        specializedSrcHandler, fieldType, null);
                }

                loadInstructions.Add((srcFieldInfo.Handler!, null, null));
            }

            primaryKeyIsEnough = props.Count == usedFields;
            // Remove useless skips from end
            while (loadInstructions is { Count: > 0, Last.Item2: null })
            {
                loadInstructions.RemoveAt(^1);
            }

            var anyNeedsCtx = false;
            for (var i = 0; i < loadInstructions.Count; i++)
            {
                if (!loadInstructions[i].Item1.NeedsCtx()) continue;
                anyNeedsCtx = true;
                break;
            }

            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(1)
                    .Newobj(() => new DBReaderCtx(null))
                    .Stloc(1);
            }

            for (var i = 0; i < loadInstructions.Count; i++)
            {
                ref var loadInstruction = ref loadInstructions[i];
                var readerOrCtx = loadInstruction.Item1.NeedsCtx() ? (Action<IILGen>?)(il => il.Ldloc(1)) : null;
                if (loadInstruction.Item2 != null)
                {
                    ilGenerator.Ldloc(0);
                    loadInstruction.Item1.Load(ilGenerator, il => il.Ldarg(2), readerOrCtx);
                    loadInstruction.Item2(ilGenerator);
                    ilGenerator.Call(loadInstruction.Item3!);
                    continue;
                }

                loadInstruction.Item1.Skip(ilGenerator, il => il.Ldarg(2), readerOrCtx);
            }

            ilGenerator.Ldloc(0).Ret();
            return (RelationLoaderFunc)method.Create(that);
        }

        RelationLoader CreateLoader(Type instanceType,
            ReadOnlySpan<TableFieldInfo> fields, string loaderName)
        {
            var method = ILBuilder.Instance.NewMethod<RelationLoader>(loaderName);
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(instanceType);
            ilGenerator
                .Ldarg(2)
                .Castclass(instanceType)
                .Stloc(0);

            var instanceTableFieldInfos = new StructList<TableFieldInfo>();
            var loadInstructions =
                new StructList<(IFieldHandler, Action<IILGen>?, MethodInfo?, bool Init, Type ToType)>();
            var props = instanceType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic |
                                                   BindingFlags.Instance);
            var persistentNameToPropertyInfo = new RefDictionary<string, PropertyInfo>();

            var publicFields = instanceType.GetFields(BindingFlags.Public | BindingFlags.Instance);
            foreach (var field in publicFields)
            {
                if (field.GetCustomAttribute<NotStoredAttribute>(true) != null) continue;
                throw new BTDBException(
                    $"Public field {instanceType.ToSimpleName()}.{field.Name} must have NotStoredAttribute. It is just intermittent, until they can start to be supported.");
            }

            foreach (var pi in props)
            {
                if (pi.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) continue;
                if (pi.GetIndexParameters().Length != 0) continue;
                var tfi = TableFieldInfo.Build(_owner.Name, pi, _owner._relationInfoResolver.FieldHandlerFactory,
                    FieldHandlerOptions.None, false);
                instanceTableFieldInfos.Add(tfi);
                persistentNameToPropertyInfo.GetOrAddValueRef(tfi.Name) = pi;
            }

            foreach (var srcFieldInfo in fields)
            {
                var fieldInfo = persistentNameToPropertyInfo.GetOrFakeValueRef(srcFieldInfo.Name);
                if (fieldInfo != null)
                {
                    foreach (var pkField in _pkFields.Span)
                    {
                        if (pkField.Name != srcFieldInfo.Name) continue;
                        fieldInfo = null;
                        break;
                    }
                }

                if (fieldInfo != null)
                {
                    var setterMethod = fieldInfo.GetAnySetMethod();
                    var fieldType = setterMethod!.GetParameters()[0].ParameterType;
                    var specializedSrcHandler =
                        srcFieldInfo.Handler!.SpecializeLoadForType(fieldType, null,
                            _owner._relationInfoResolver.FieldHandlerLogger);
                    var willLoad = specializedSrcHandler.HandledType();
                    var converterGenerator =
                        _owner._relationInfoResolver.TypeConvertorGenerator
                            .GenerateConversion(willLoad!, fieldType);
                    if (converterGenerator != null)
                    {
                        for (var i = 0; i < instanceTableFieldInfos.Count; i++)
                        {
                            if (instanceTableFieldInfos[i].Name != srcFieldInfo.Name) continue;
                            instanceTableFieldInfos.RemoveAt(i);
                            break;
                        }

                        loadInstructions.Add(
                            (specializedSrcHandler, converterGenerator, setterMethod, false, fieldType));
                        continue;
                    }

                    _owner._relationInfoResolver.FieldHandlerLogger?.ReportTypeIncompatibility(willLoad,
                        specializedSrcHandler, fieldType, null);
                }

                loadInstructions.Add((srcFieldInfo.Handler!, null, null, false, null));
            }

            // Remove useless skips from end
            while (loadInstructions is { Count: > 0, Last.Item2: null })
            {
                loadInstructions.RemoveAt(^1);
            }

            foreach (var srcFieldInfo in instanceTableFieldInfos)
            {
                var iFieldHandlerWithInit = srcFieldInfo.Handler as IFieldHandlerWithInit;
                if (iFieldHandlerWithInit == null) continue;
                var specializedSrcHandler = srcFieldInfo.Handler;
                var willLoad = specializedSrcHandler.HandledType();
                var fieldInfo = persistentNameToPropertyInfo.GetOrFakeValueRef(srcFieldInfo.Name);
                var setterMethod = fieldInfo.GetAnySetMethod();
                var toType = setterMethod!.GetParameters()[0].ParameterType;
                var converterGenerator =
                    _owner._relationInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad!,
                        toType);
                if (converterGenerator == null) continue;
                if (!iFieldHandlerWithInit.NeedInit()) continue;
                loadInstructions.Add((specializedSrcHandler, converterGenerator, setterMethod, true, toType));
            }

            var anyNeedsCtx = false;
            for (var i = 0; i < loadInstructions.Count; i++)
            {
                if (!loadInstructions[i].Item1.NeedsCtx()) continue;
                anyNeedsCtx = true;
                break;
            }

            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Newobj(() => new DBReaderCtx(null))
                    .Stloc(1);
            }

            for (var i = 0; i < loadInstructions.Count; i++)
            {
                ref var loadInstruction = ref loadInstructions[i];
                var readerOrCtx = loadInstruction.Item1.NeedsCtx() ? (Action<IILGen>?)(il => il.Ldloc(1)) : null;
                if (loadInstruction.Item2 != null)
                {
                    var loc = ilGenerator.DeclareLocal(loadInstruction.ToType);
                    if (loadInstruction.Init)
                    {
                        ((IFieldHandlerWithInit)loadInstruction.Item1).Init(ilGenerator, readerOrCtx);
                    }
                    else
                    {
                        loadInstruction.Item1.Load(ilGenerator, il => il.Ldarg(1), readerOrCtx);
                    }

                    loadInstruction.Item2(ilGenerator);
                    ilGenerator
                        .Stloc(loc)
                        .Ldloc(0)
                        .Ldloc(loc)
                        .Call(loadInstruction.Item3!);
                    continue;
                }

                loadInstruction.Item1.Skip(ilGenerator, il => il.Ldarg(1), readerOrCtx);
            }

            ilGenerator.Ret();
            return method.Create();
        }
    }

    FreeContentFun?[] _valueIDictFinders = Array.Empty<FreeContentFun?>();

    RelationSaver[] _secondaryKeysSavers; //secondary key idx => sk key saver

    internal delegate void SecondaryKeyConvertSaver(IInternalObjectDBTransaction tr, ref MemWriter writer,
        ref MemReader keyReader, ref MemReader valueReader, object emptyValue);

    readonly ConcurrentDictionary<ulong, SecondaryKeyConvertSaver> _secondaryKeysConvertSavers = new();

    public delegate void SecondaryKeyValueToPKLoader(ref MemReader readerKey, ref MemWriter writer);

    readonly ConcurrentDictionary<ulong, SecondaryKeyValueToPKLoader> _secondaryKeyValueToPKLoader = new();

    public readonly struct SimpleLoaderType : IEquatable<SimpleLoaderType>
    {
        public IFieldHandler FieldHandler { get; }

        public Type RealType { get; }

        public SimpleLoaderType(IFieldHandler fieldHandler, Type realType)
        {
            FieldHandler = fieldHandler;
            RealType = realType;
        }

        public bool Equals(SimpleLoaderType other)
        {
            return FieldHandler == other.FieldHandler && RealType == other.RealType;
        }
    }

    readonly
        ConcurrentDictionary<SimpleLoaderType, object> //object is of type Action<AbstractBufferedReader, IReaderCtx, (object or value type same as in conc. dic. key)>
        _simpleLoader = new();

    internal readonly List<ulong> FreeContentOldDict = new();
    internal readonly List<ulong> FreeContentNewDict = new();
    public byte[] Prefix;
    internal byte[] PrefixSecondary;

    bool? _needImplementFreeContent;
    internal byte[]? PrimeSK2Real;

    // ReSharper disable once NotNullMemberIsNotInitialized - not true
    public RelationInfo(uint id, string name, RelationBuilder builder, IInternalObjectDBTransaction tr)
    {
        _id = id;
        _name = name;
        _relationInfoResolver = builder.RelationInfoResolver;
        _interfaceType = builder.InterfaceType;
        _clientType = builder.ItemType;
        _defaultClientObject = builder.PristineItemInstance;

        CalculatePrefix();
        LoadUnresolvedVersionInfos(tr.KeyValueDBTransaction);
        ResolveVersionInfos();
        ClientRelationVersionInfo = CreateVersionInfoFromPrime(builder.ClientRelationVersionInfo);
        _hasInKeyValue = false;
        foreach (var fieldInfo in ClientRelationVersionInfo.PrimaryKeyFields.Span)
        {
            if (!fieldInfo.InKeyValue) continue;
            _hasInKeyValue = true;
            break;
        }

        Extensions.RegisterFieldHandlers(ClientRelationVersionInfo.GetAllFields().ToArray().Select(a => a.Handler),
            tr.Owner);
        foreach (var loadType in builder.LoadTypes)
        {
            ItemLoaderInfos.Add(new(this, loadType));
        }

        if (LastPersistedVersion > 0 &&
            RelationVersionInfo.Equal(_relationVersions[LastPersistedVersion]!, ClientRelationVersionInfo))
        {
            _relationVersions[LastPersistedVersion] = ClientRelationVersionInfo;
            ClientTypeVersion = LastPersistedVersion;
            CreateCreatorLoadersAndSavers();
            CheckSecondaryKeys(tr, ClientRelationVersionInfo);
        }
        else
        {
            ClientTypeVersion = LastPersistedVersion + 1;
            _relationVersions[ClientTypeVersion] = ClientRelationVersionInfo;
            WriteRelationMetadata(tr);

            CreateCreatorLoadersAndSavers();
            if (LastPersistedVersion > 0)
            {
                CheckThatPrimaryKeyHasNotChanged(tr, name, ClientRelationVersionInfo,
                    _relationVersions[LastPersistedVersion]!);
                UpdateSecondaryKeys(tr, ClientRelationVersionInfo, _relationVersions[LastPersistedVersion]!);
            }
        }
    }

    [SkipLocalsInit]
    void WriteRelationMetadata(IObjectDBTransaction tr)
    {
        Span<byte> buffer = stackalloc byte[16];
        var writerKey = MemWriter.CreateFromStackAllocatedSpan(buffer);
        writerKey.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
        writerKey.WriteVUInt32(_id);
        writerKey.WriteVUInt32(ClientTypeVersion);
        Span<byte> buffer2 = stackalloc byte[4096];
        var writerValue = MemWriter.CreateFromStackAllocatedSpan(buffer2);
        ClientRelationVersionInfo.Save(ref writerValue);
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue(writerKey.GetSpan(), writerValue.GetSpan());
    }

    void CheckThatPrimaryKeyHasNotChanged(IInternalObjectDBTransaction tr, string name,
        RelationVersionInfo info, RelationVersionInfo previousInfo)
    {
        var db = tr.Owner;
        var pkFields = info.PrimaryKeyFields;
        var prevPkFields = previousInfo.PrimaryKeyFields;
        var pkFieldsUnique = CountNonInKeyValues(pkFields);
        var prevPkFieldsUnique = CountNonInKeyValues(prevPkFields);

        if (pkFieldsUnique != prevPkFieldsUnique)
        {
            if (db.ActualOptions.SelfHealing)
            {
                db.Logger?.ReportIncompatiblePrimaryKey(name, $"{pkFields.Length}!={prevPkFields.Length}");
                ClearRelationData(tr, previousInfo);
                return;
            }

            throw new BTDBException(
                $"Change of primary key in relation '{name}' is not allowed. Field count {pkFields.Length} != {prevPkFields.Length}.");
        }

        for (var i = 0; i < pkFieldsUnique; i++)
        {
            if (ArePrimaryKeyFieldsCompatible(pkFields.Span[i].Handler!, prevPkFields.Span[i].Handler!)) continue;
            db.Logger?.ReportIncompatiblePrimaryKey(name, pkFields.Span[i].Name);
            if (db.ActualOptions.SelfHealing)
            {
                ClearRelationData(tr, previousInfo);
                return;
            }

            throw new BTDBException(
                $"Change of primary key in relation '{name}' is not allowed. Field '{pkFields.Span[i].Name}' is not compatible.");
        }

        if (pkFields.Length == prevPkFields.Length)
        {
            for (int i = pkFieldsUnique; i < pkFields.Length; i++)
            {
                if (!ArePrimaryKeyFieldsCompatible(pkFields.Span[i].Handler!, prevPkFields.Span[i].Handler!))
                    goto NeedRebuild;
            }

            return;
        }

        NeedRebuild:

        ItemLoaderInfos.Add(new(this, _clientType, prevPkFields));
        var enumeratorType = typeof(RelationEnumerator<>).MakeGenericType(_clientType);
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        var keyWriter = new MemWriter();

        foreach (var obj in (IEnumerable)Activator.CreateInstance(enumeratorType, tr, this, Prefix,
                     (int)ItemLoaderInfos.Count - 1)!)
        {
            keyWriter.WriteBlock(Prefix);
            var lenOfPkWoInKeyValues = PrimaryKeysSaver(tr, ref keyWriter, obj);
            var keyBytes = keyWriter.GetSpan();
            cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
            keyWriter.Reset();
        }

        ItemLoaderInfos.RemoveAt(^1);
        return;

        int CountNonInKeyValues(ReadOnlyMemory<TableFieldInfo> fields)
        {
            var res = 0;
            foreach (var field in fields.Span)
            {
                if (field.InKeyValue) continue;
                res++;
            }

            return res;
        }
    }

    static bool ArePrimaryKeyFieldsCompatible(IFieldHandler newHandler, IFieldHandler previousHandler)
    {
        var newHandledType = newHandler.HandledType();
        var previousHandledType = previousHandler.HandledType();
        if (newHandledType == previousHandledType)
            return true;
        if (newHandledType!.IsEnum && previousHandledType!.IsEnum)
        {
            var prevEnumCfg =
                new EnumFieldHandler.EnumConfiguration(((EnumFieldHandler)previousHandler).Configuration);
            var newEnumCfg = new EnumFieldHandler.EnumConfiguration(((EnumFieldHandler)newHandler).Configuration);

            return prevEnumCfg.IsBinaryRepresentationSubsetOf(newEnumCfg);
        }

        return false;
    }

    public bool NeedImplementFreeContent()
    {
        if (!_needImplementFreeContent.HasValue)
        {
            CalcNeedImplementFreeContent();
        }

        return _needImplementFreeContent!.Value;
    }

    void CalcNeedImplementFreeContent()
    {
        for (var i = 0; i < _relationVersions.Length; i++)
        {
            if (_relationVersions[i] == null) continue;
            GetIDictFinder((uint)i);
            if (_needImplementFreeContent.HasValue)
                return;
        }

        _needImplementFreeContent = false;
    }

    void CheckSecondaryKeys(IInternalObjectDBTransaction tr, RelationVersionInfo info)
    {
        var count = GetRelationCount(tr);
        var secKeysToAdd = new StructList<KeyValuePair<uint, SecondaryKeyInfo>>();
        foreach (var sk in info.SecondaryKeys)
        {
            if (WrongCountInSecondaryKey(tr.KeyValueDBTransaction, count, sk.Key))
            {
                DeleteSecondaryKey(tr.KeyValueDBTransaction, sk.Key);
                secKeysToAdd.Add(sk);
            }
        }

        if (secKeysToAdd.Count > 0)
            CalculateSecondaryKey(tr, secKeysToAdd);
    }

    long GetRelationCount(IInternalObjectDBTransaction tr)
    {
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        return cursor.GetKeyValueCount(Prefix);
    }

    void UpdateSecondaryKeys(IInternalObjectDBTransaction tr, RelationVersionInfo info,
        RelationVersionInfo previousInfo)
    {
        var count = GetRelationCount(tr);
        foreach (var prevIdx in previousInfo.SecondaryKeys.Keys)
        {
            if (!info.SecondaryKeys.ContainsKey(prevIdx))
                DeleteSecondaryKey(tr.KeyValueDBTransaction, prevIdx);
        }

        var secKeysToAdd = new StructList<KeyValuePair<uint, SecondaryKeyInfo>>();
        foreach (var sk in info.SecondaryKeys)
        {
            if (!previousInfo.SecondaryKeys.ContainsKey(sk.Key))
            {
                secKeysToAdd.Add(sk);
            }
            else if (WrongCountInSecondaryKey(tr.KeyValueDBTransaction, count, sk.Key))
            {
                DeleteSecondaryKey(tr.KeyValueDBTransaction, sk.Key);
                secKeysToAdd.Add(sk);
            }
        }

        if (secKeysToAdd.Count > 0)
            CalculateSecondaryKey(tr, secKeysToAdd);
    }

    bool WrongCountInSecondaryKey(IKeyValueDBTransaction tr, long count, uint index)
    {
        using var cursor = tr.CreateCursor();
        return count != cursor.GetKeyValueCount(GetPrefixToSecondaryKey(index, stackalloc byte[8]));
    }

    void ClearRelationData(IInternalObjectDBTransaction tr, RelationVersionInfo info)
    {
        foreach (var prevIdx in info.SecondaryKeys.Keys)
        {
            DeleteSecondaryKey(tr.KeyValueDBTransaction, prevIdx);
        }

        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[8]);
        writer.WriteBlock(ObjectDB.AllRelationsPKPrefix);
        writer.WriteVUInt32(Id);

        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        cursor.EraseAll(writer.GetSpan());
    }

    void DeleteSecondaryKey(IKeyValueDBTransaction keyValueTr, uint index)
    {
        using var cursor = keyValueTr.CreateCursor();
        cursor.EraseAll(GetPrefixToSecondaryKey(index, stackalloc byte[8]));
    }

    ReadOnlySpan<byte> GetPrefixToSecondaryKey(uint index, Span<byte> buf)
    {
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteBlock(PrefixSecondary);
        writer.WriteUInt8((byte)index);
        return writer.GetSpan();
    }

    void CalculateSecondaryKey(IInternalObjectDBTransaction tr,
        ReadOnlySpan<KeyValuePair<uint, SecondaryKeyInfo>> indexes)
    {
        var enumeratorType = typeof(RelationEnumerator<>).MakeGenericType(_clientType);

        var keySavers = new RelationSaver[indexes.Length];

        for (var i = 0; i < indexes.Length; i++)
        {
            keySavers[i] = CreateSaver(ClientRelationVersionInfo.GetSecondaryKeyFields(indexes[i].Key),
                $"Relation_{Name}_Upgrade_SK_{indexes[i].Value.Name}_KeySaver", false);
        }

        var keyWriter = new MemWriter();
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();

        foreach (var obj in (IEnumerable)Activator.CreateInstance(enumeratorType, tr, this, Prefix, 0)!)
        {
            for (var i = 0; i < indexes.Length; i++)
            {
                keyWriter.WriteBlock(PrefixSecondary);
                keyWriter.WriteUInt8((byte)indexes[i].Key);
                keySavers[i](tr, ref keyWriter, obj!);
                var keyBytes = keyWriter.GetSpanAndReset();

                if (!cursor.CreateOrUpdateKeyValue(keyBytes, new()))
                    throw new BTDBException("Internal error, secondary key bytes must be always unique.");
            }
        }
    }

    [SkipLocalsInit]
    unsafe void LoadUnresolvedVersionInfos(IKeyValueDBTransaction tr)
    {
        LastPersistedVersion = 0;
        Span<byte> buf = stackalloc byte[8];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
        writer.WriteVUInt32(_id);
        var prefix = writer.GetSpan();
        var relationVersions = new Dictionary<uint, RelationVersionInfo>();
        using var cursor = tr.CreateCursor();
        Span<byte> keyBuffer = stackalloc byte[16];
        Span<byte> valueBuffer = stackalloc byte[4096];
        while (cursor.FindNextKey(prefix))
        {
            var valueSpan = cursor.GetValueSpan(ref valueBuffer);
            fixed (void* _ = valueSpan)
            {
                var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                LastPersistedVersion = (uint)PackUnpack.UnpackVUInt(cursor.GetKeySpan(ref keyBuffer)[prefix.Length..]);
                var relationVersionInfo = RelationVersionInfo.LoadUnresolved(ref valueReader, _name);
                relationVersions[LastPersistedVersion] = relationVersionInfo;
            }
        }

        _relationVersions = new RelationVersionInfo[LastPersistedVersion + 2];
        foreach (var (key, value) in relationVersions)
        {
            _relationVersions[key] = value;
        }

        _valueIDictFinders = new FreeContentFun?[_relationVersions.Length];
    }

    void ResolveVersionInfos()
    {
        foreach (var versionInfo in _relationVersions)
        {
            versionInfo?.ResolveFieldHandlers(_relationInfoResolver.FieldHandlerFactory);
        }
    }

    internal uint Id => _id;

    internal string Name => _name;

    internal Type ClientType => _clientType;

    internal Type? InterfaceType => _interfaceType;

    internal object DefaultClientObject => _defaultClientObject;

    internal RelationVersionInfo ClientRelationVersionInfo { get; }

    internal uint LastPersistedVersion { get; set; }

    internal uint ClientTypeVersion { get; }

    void CreateCreatorLoadersAndSavers()
    {
        StructList<TableFieldInfo> filteredFields = new();
        foreach (var tableFieldInfo in ClientRelationVersionInfo.Fields.Span)
        {
            if (tableFieldInfo.Computed) continue;
            filteredFields.Add(tableFieldInfo);
        }

        _valueSaver = CreateSaver(filteredFields, $"RelationValueSaver_{Name}", false);
        _primaryKeysSaver = CreateSaver(ClientRelationVersionInfo.PrimaryKeyFields.Span,
            $"RelationKeySaver_{Name}", true);
        _beforeRemove = CreateBeforeRemove($"RelationBeforeRemove_{Name}");
        if (ClientRelationVersionInfo.SecondaryKeys.Count > 0)
        {
            _secondaryKeysSavers = new RelationSaver[ClientRelationVersionInfo.SecondaryKeys.Keys.Max() + 1];
            foreach (var (idx, secondaryKeyInfo) in ClientRelationVersionInfo.SecondaryKeys)
            {
                _secondaryKeysSavers[idx] = CreateSaver(
                    ClientRelationVersionInfo.GetSecondaryKeyFields(idx),
                    $"Relation_{Name}_SK_{secondaryKeyInfo.Name}_KeySaver", false);
            }
        }
    }

    internal RelationSaver ValueSaver => _valueSaver;

    internal RelationSaver PrimaryKeysSaver => _primaryKeysSaver;

    internal RelationBeforeRemove? BeforeRemove => _beforeRemove;
    internal bool HasInKeyValue => _hasInKeyValue;

    void CreateSaverIl(IILGen ilGen, ReadOnlySpan<TableFieldInfo> fields,
        Action<IILGen> pushInstance,
        Action<IILGen> pushWriter, Action<IILGen> pushTransaction, Action<IILGen>? pushLenOfKey)
    {
        var writerCtxLocal = CreateWriterCtx(ilGen, fields, pushTransaction);
        var props = ClientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        var wasInKeyValueField = false;
        foreach (var field in fields)
        {
            if (field.InKeyValue && pushLenOfKey != null)
            {
                if (!wasInKeyValueField)
                {
                    wasInKeyValueField = true;
                    pushWriter(ilGen);
                    ilGen.Call(typeof(MemWriter).GetMethod(nameof(MemWriter.NoControllerGetCurrentPosition))!);
                    ilGen.ConvI4();
                    pushLenOfKey(ilGen);
                }
            }

            var getter = props.First(p => GetPersistentName(p) == field.Name).GetAnyGetMethod();
            var handler = field.Handler!.SpecializeSaveForType(getter!.ReturnType);
            handler.Save(ilGen, pushWriter, il => il.Ldloc(writerCtxLocal!), il =>
            {
                il.Do(pushInstance);
                il.Callvirt(getter);
                _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(getter.ReturnType,
                    handler.HandledType()!)!(il);
            });
        }
    }

    static IILLocal? CreateWriterCtx(IILGen ilGenerator, ReadOnlySpan<TableFieldInfo> fields,
        Action<IILGen> pushTransaction)
    {
        var anyNeedsCtx = false;
        foreach (var field in fields)
        {
            if (field.Handler!.NeedsCtx())
            {
                anyNeedsCtx = true;
                break;
            }
        }

        IILLocal writerCtxLocal = null;
        if (anyNeedsCtx)
        {
            writerCtxLocal = ilGenerator.DeclareLocal(typeof(IWriterCtx));
            ilGenerator
                .Do(pushTransaction)
                .Newobj(() => new DBWriterCtx(null))
                .Stloc(writerCtxLocal);
        }

        return writerCtxLocal;
    }

    static void StoreNthArgumentOfTypeIntoLoc(IILGen il, ushort argIdx, Type type, ushort locIdx)
    {
        il
            .Ldarg(argIdx)
            .Castclass(type)
            .Stloc(locIdx);
    }

    struct LocalAndHandler
    {
        public IILLocal Local;
        public IFieldHandler Handler;
    }

    SecondaryKeyConvertSaver CreateBytesToSKSaver(uint version, uint secondaryKeyIndex, string saverName)
    {
        var method = ILBuilder.Instance.NewMethod<SecondaryKeyConvertSaver>(saverName);
        var ilGenerator = method.Generator;
        IILLocal defaultObjectLoc = null;
        static void PushWriter(IILGen il) => il.Ldarg(1);

        var firstBuffer = new BufferInfo(); //pk's
        var secondBuffer = new BufferInfo(); //values
        var outOfOrderSkParts = new Dictionary<int, LocalAndHandler>(); //local and specialized saver

        var pks = ClientRelationVersionInfo.PrimaryKeyFields.Span;
        var skFieldIds = ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Fields;
        var skFields = ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex).ToArray();
        var valueFields = _relationVersions[version]!.Fields.Span;
        var writerCtxLocal = CreateWriterCtx(ilGenerator, skFields, il => il.Ldarg(0));
        for (var skFieldIdx = 0; skFieldIdx < skFieldIds.Count; skFieldIdx++)
        {
            if (outOfOrderSkParts.TryGetValue(skFieldIdx, out var saveLocalInfo))
            {
                var pushCtx = WriterOrContextForHandler(writerCtxLocal);
                saveLocalInfo.Handler.Save(ilGenerator, PushWriter, pushCtx, il => il.Ldloc(saveLocalInfo.Local));
                continue;
            }

            var skf = skFieldIds[skFieldIdx];
            if (skf.IsFromPrimaryKey)
            {
                InitializeBuffer(2, ref firstBuffer, ilGenerator, pks, true);
                //firstBuffer.ActualFieldIdx == number of processed PK's
                for (var pkIdx = firstBuffer.ActualFieldIdx; pkIdx < skf.Index; pkIdx++)
                {
                    //all PK parts are contained in SK
                    FindPosition(pkIdx, skFieldIds, out var skFieldIdxForPk);
                    StoreIntoLocal(ilGenerator, pks[pkIdx].Handler!, firstBuffer, outOfOrderSkParts,
                        skFieldIdxForPk,
                        skFields[skFieldIdxForPk].Handler!);
                }

                CopyToOutput(ilGenerator, pks[(int)skf.Index].Handler!, writerCtxLocal!, PushWriter,
                    skFields[skFieldIdx].Handler!, firstBuffer);
                firstBuffer.ActualFieldIdx = (int)skf.Index + 1;
            }
            else
            {
                InitializeBuffer(3, ref secondBuffer, ilGenerator, valueFields, false);

                var valueFieldIdx = -1;
                for (var i = 0; i < valueFields.Length; i++)
                {
                    if (valueFields[i].Name == skFields[skFieldIdx].Name)
                    {
                        valueFieldIdx = i;
                        break;
                    }
                }

                if (valueFieldIdx >= 0)
                {
                    for (var valueIdx = secondBuffer.ActualFieldIdx; valueIdx < valueFieldIdx; valueIdx++)
                    {
                        var valueField = valueFields[valueIdx];
                        var storeForSkIndex = -1;
                        for (var i = 0; i < skFields.Length; i++)
                        {
                            if (skFields[i].Name == valueField.Name)
                            {
                                storeForSkIndex = i;
                                break;
                            }
                        }

                        if (storeForSkIndex == -1)
                            valueField.Handler!.Skip(ilGenerator, secondBuffer.PushReader, secondBuffer.PushCtx);
                        else
                            StoreIntoLocal(ilGenerator, valueField.Handler!, secondBuffer, outOfOrderSkParts,
                                storeForSkIndex, skFields[storeForSkIndex].Handler!);
                    }

                    CopyToOutput(ilGenerator, valueFields[valueFieldIdx].Handler!, writerCtxLocal!, PushWriter,
                        skFields[skFieldIdx].Handler!, secondBuffer);
                    secondBuffer.ActualFieldIdx = valueFieldIdx + 1;
                }
                else
                {
                    //older version of value does not contain sk field - store field from default value (can be initialized in constructor)
                    if (defaultObjectLoc == null)
                    {
                        defaultObjectLoc = ilGenerator.DeclareLocal(ClientType);
                        ilGenerator.Ldarg(4)
                            .Castclass(ClientType)
                            .Stloc(defaultObjectLoc);
                    }

                    var loc = defaultObjectLoc;
                    CreateSaverIl(ilGenerator,
                        new[] { ClientRelationVersionInfo.GetSecondaryKeyField((int)skf.Index) },
                        il => il.Ldloc(loc), PushWriter, il => il.Ldarg(0), null);
                }
            }
        }

        ilGenerator.Ret();
        return method.Create();
    }

    static void CopyToOutput(IILGen ilGenerator, IFieldHandler valueHandler, IILLocal writerCtxLocal,
        Action<IILGen> pushWriter,
        IFieldHandler skHandler, BufferInfo buffer)

    {
        var pushCtx = WriterOrContextForHandler(writerCtxLocal);
        var sourceType = valueHandler.HandledType()!;
        var targetType = skHandler.HandledType()!;
        if (sourceType == targetType)
            skHandler.Save(ilGenerator, pushWriter, pushCtx,
                il => { valueHandler.Load(il, buffer.PushReader, buffer.PushCtx); });
        else
        {
            var converter = DefaultTypeConvertorGenerator.Instance.GenerateConversion(sourceType, targetType);
            if (converter == null)
                throw new BTDBException($"Cannot convert {sourceType.ToSimpleName()} to {targetType.ToSimpleName()}");
            skHandler.Save(ilGenerator, pushWriter, pushCtx,
                il =>
                {
                    valueHandler.Load(il, buffer.PushReader, buffer.PushCtx);
                    il.Do(converter);
                });
        }
    }

    static void StoreIntoLocal(IILGen ilGenerator, IFieldHandler valueHandler, BufferInfo bufferInfo,
        Dictionary<int, LocalAndHandler> outOfOrderSkParts, int skFieldIdx, IFieldHandler skFieldHandler)
    {
        var local = ilGenerator.DeclareLocal(valueHandler.HandledType()!);
        valueHandler.Load(ilGenerator, bufferInfo.PushReader, bufferInfo.PushCtx);
        ilGenerator.Stloc(local);
        outOfOrderSkParts[skFieldIdx] = new LocalAndHandler
        {
            Handler = skFieldHandler.SpecializeSaveForType(valueHandler.HandledType()!),
            Local = local
        };
    }

    static Action<IILGen> WriterOrContextForHandler(IILLocal? writerCtxLocal)
    {
        return il => il.Ldloc(writerCtxLocal!);
    }

    static void InitializeBuffer(ushort bufferArgIdx, ref BufferInfo bufferInfo, IILGen ilGenerator,
        ReadOnlySpan<TableFieldInfo> fields, bool skipAllRelationsPKPrefix)
    {
        if (bufferInfo.ReaderCreated) return;
        bufferInfo.ReaderCreated = true;
        bufferInfo.PushReader = il => il.Ldarg(bufferArgIdx);

        if (skipAllRelationsPKPrefix)
            ilGenerator
                .Do(bufferInfo.PushReader)
                .Call(typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!); //ObjectDB.AllRelationsPKPrefix
        ilGenerator
            .Do(bufferInfo.PushReader).Call(typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt64))!);

        var anyNeedsCtx = false;
        foreach (var fieldInfo in fields)
        {
            if (fieldInfo.Handler!.NeedsCtx())
            {
                anyNeedsCtx = true;
                break;
            }
        }

        if (anyNeedsCtx)
        {
            var readerCtxLocal = ilGenerator.DeclareLocal(typeof(IReaderCtx));
            ilGenerator
                .Ldarg(0) // tr
                .Newobj(() => new DBReaderCtx(null))
                .Stloc(readerCtxLocal);
            bufferInfo.PushCtx = il => il.Ldloc(readerCtxLocal);
        }
    }

    RelationBeforeRemove? CreateBeforeRemove(string functionName)
    {
        var hasBeforeRemove = false;
        foreach (var methodInfo in ClientType.GetMethods(BindingFlags.Instance | BindingFlags.Public |
                                                         BindingFlags.NonPublic))
        {
            if (methodInfo.GetCustomAttribute<OnBeforeRemoveAttribute>() == null) continue;
            if (methodInfo.ReturnType != typeof(void) && methodInfo.ReturnType != typeof(bool))
                throw new BTDBException("OnBeforeRemove method " + ClientType.ToSimpleName() + "." + methodInfo.Name +
                                        " must return void or bool.");
            hasBeforeRemove = true;
        }

        if (!hasBeforeRemove) return null;
        var method = ILBuilder.Instance.NewMethod<RelationBeforeRemove>(functionName);
        var ilGenerator = method.Generator;
        var clientTypeLoc = ilGenerator.DeclareLocal(ClientType);
        var resultLoc = ilGenerator.DeclareLocal(typeof(bool));
        StoreNthArgumentOfTypeIntoLoc(ilGenerator, 2, ClientType, (ushort)clientTypeLoc.Index);
        ilGenerator
            .LdcI4(0)
            .Stloc(resultLoc);
        foreach (var methodInfo in ClientType.GetMethods(BindingFlags.Instance | BindingFlags.Public |
                                                         BindingFlags.NonPublic))
        {
            if (methodInfo.GetCustomAttribute<OnBeforeRemoveAttribute>() == null) continue;
            var parameters = methodInfo.GetParameters();
            var paramLocs = new IILLocal[parameters.Length];
            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];
                if (parameter.ParameterType == typeof(IObjectDBTransaction)) continue;
                paramLocs[i] = ilGenerator.DeclareLocal(parameter.ParameterType);
                ilGenerator.Ldarg(1);
                ilGenerator.Ldstr(parameter.Name!);
                ilGenerator.Ldtoken(parameter.ParameterType);
                ilGenerator.Call(() => Type.GetTypeFromHandle(new()));
                ilGenerator.Callvirt(() => default(IContainer).ResolveNamed("", null));
                ilGenerator.UnboxAny(parameter.ParameterType);
                ilGenerator.Stloc(paramLocs[i]);
            }

            var withBoolResult = methodInfo.ReturnType == typeof(bool);
            if (withBoolResult)
            {
                ilGenerator
                    .Ldloc(resultLoc);
            }

            ilGenerator
                .Ldloc(clientTypeLoc);
            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];
                if (parameter.ParameterType == typeof(IObjectDBTransaction))
                {
                    ilGenerator.Ldarg(0).Castclass(typeof(IObjectDBTransaction));
                    continue;
                }

                ilGenerator.Ldloc(paramLocs[i]);
            }

            ilGenerator
                .Call(methodInfo);
            if (withBoolResult)
            {
                ilGenerator.Or().Stloc(resultLoc);
            }
        }

        ilGenerator
            .Ldloc(resultLoc)
            .Ret();
        return method.Create();
    }

    RelationSaver CreateSaver(ReadOnlySpan<TableFieldInfo> fields, string saverName, bool forPrimaryKey)
    {
        var method = ILBuilder.Instance.NewMethod<RelationSaver>(saverName);
        var ilGenerator = method.Generator;
        StoreNthArgumentOfTypeIntoLoc(ilGenerator, 2, ClientType, (ushort)ilGenerator.DeclareLocal(ClientType).Index);
        var result = ilGenerator.DeclareLocal(typeof(int));
        ilGenerator
            .LdcI4(0)
            .Stloc(result);
        if (forPrimaryKey)
        {
            foreach (var methodInfo in ClientType.GetMethods(BindingFlags.Instance | BindingFlags.Public |
                                                             BindingFlags.NonPublic))
            {
                if (methodInfo.GetCustomAttribute<OnSerializeAttribute>() == null) continue;
                if (methodInfo.GetParameters().Length != 0)
                    throw new BTDBException("OnSerialize method " + ClientType.ToSimpleName() + "." + methodInfo.Name +
                                            " must have zero parameters.");
                if (methodInfo.ReturnType != typeof(void))
                    throw new BTDBException("OnSerialize method " + ClientType.ToSimpleName() + "." + methodInfo.Name +
                                            " must return void.");
                ilGenerator.Ldloc(0).Callvirt(methodInfo);
            }
        }

        CreateSaverIl(ilGenerator, fields,
            il => il.Ldloc(0), il => il.Ldarg(1), il => il.Ldarg(0), forPrimaryKey ? (il => il.Stloc(result)) : null);
        ilGenerator.Ldloc(result);
        ilGenerator.Ret();
        return method.Create();
    }

    static bool SecondaryIndexHasSameDefinition(ReadOnlySpan<TableFieldInfo> currFields,
        ReadOnlySpan<TableFieldInfo> prevFields)
    {
        if (currFields.Length != prevFields.Length)
            return false;
        for (var i = 0; i < currFields.Length; i++)
        {
            if (!TableFieldInfo.Equal(currFields[i], prevFields[i]))
                return false;
        }

        return true;
    }

    RelationVersionInfo CreateVersionInfoFromPrime(RelationVersionInfo prime)
    {
        var secondaryKeys = new Dictionary<uint, SecondaryKeyInfo>();
        PrimeSK2Real = new byte[prime.SecondaryKeys.Count];
        if (LastPersistedVersion > 0)
        {
            var prevVersion = _relationVersions[LastPersistedVersion];
            foreach (var primeSecondaryKey in prime.SecondaryKeys)
            {
                if (prevVersion!.SecondaryKeysNames.TryGetValue(primeSecondaryKey.Value.Name, out var index))
                {
                    var prevFields = prevVersion.GetSecondaryKeyFields(index);
                    var currFields = prime.GetSecondaryKeyFields(primeSecondaryKey.Key);
                    if (SecondaryIndexHasSameDefinition(currFields, prevFields))
                        goto existing;
                }

                while (prevVersion.SecondaryKeys.ContainsKey(index) || secondaryKeys.ContainsKey(index))
                    index++;
                existing:
                PrimeSK2Real[primeSecondaryKey.Key] = (byte)index;
                secondaryKeys.Add(index, primeSecondaryKey.Value);
            }
        }
        else
        {
            foreach (var primeSecondaryKey in prime.SecondaryKeys)
            {
                PrimeSK2Real[primeSecondaryKey.Key] = (byte)primeSecondaryKey.Key;
                secondaryKeys.Add(primeSecondaryKey.Key, primeSecondaryKey.Value);
            }
        }

        return new RelationVersionInfo(prime.PrimaryKeyFields, secondaryKeys, prime.SecondaryKeyFields,
            prime.Fields);
    }

    static bool IsIgnoredType(Type type)
    {
        if (type.IsGenericType)
        {
            var genericTypeDefinition = type.GetGenericTypeDefinition();
            if (genericTypeDefinition == typeof(IEnumerable<>) ||
                genericTypeDefinition == typeof(IReadOnlyCollection<>))
                return true;
        }
        else
        {
            if (type == typeof(IEnumerable))
                return true;
        }

        return false;
    }

    public static IEnumerable<MethodInfo> GetMethods(Type interfaceType)
    {
        if (IsIgnoredType(interfaceType)) yield break;
        foreach (var methodInfo in GetAbstractMethods(interfaceType)) yield return methodInfo;
        foreach (var iface in interfaceType.GetInterfaces())
        {
            if (IsIgnoredType(iface)) continue;
            foreach (var methodInfo in GetAbstractMethods(iface)) yield return methodInfo;
        }

        static IEnumerable<MethodInfo> GetAbstractMethods(Type type)
        {
            return type.GetMethods().Where(x => x.IsAbstract);
        }
    }

    public static IEnumerable<PropertyInfo> GetProperties(Type interfaceType)
    {
        if (IsIgnoredType(interfaceType)) yield break;
        var properties = interfaceType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
        foreach (var property in properties)
        {
            if (property.Name == nameof(IRelation.BtdbInternalNextInChain)) continue;
            yield return property;
        }

        foreach (var iface in interfaceType.GetInterfaces())
        {
            if (IsIgnoredType(iface)) continue;
            var inheritedProperties = iface.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            foreach (var property in inheritedProperties)
            {
                if (property.Name == nameof(IRelation.BtdbInternalNextInChain)) continue;
                yield return property;
            }
        }
    }

    FreeContentFun GetIDictFinder(uint version)
    {
        FreeContentFun? res;
        do
        {
            res = _valueIDictFinders[version];
            if (res != null) return res;
            res = CreateIDictFinder(version);
        } while (Interlocked.CompareExchange(ref _valueIDictFinders[version], res, null) != null);

        return res;
    }

    internal RelationSaver GetSecondaryKeysKeySaver(uint secondaryKeyIndex)
    {
        return _secondaryKeysSavers[secondaryKeyIndex];
    }

    internal SecondaryKeyConvertSaver GetPKValToSKMerger(uint version, uint secondaryKeyIndex)
    {
        var h = secondaryKeyIndex + version * 10000ul;
        return _secondaryKeysConvertSavers.GetOrAdd(h,
            (_, ver, secKeyIndex, relationInfo) => CreateBytesToSKSaver(ver, secKeyIndex,
                $"Relation_{relationInfo.Name}_PkVal_to_SK_{relationInfo.ClientRelationVersionInfo.SecondaryKeys[secKeyIndex].Name}_v{ver}"),
            version, secondaryKeyIndex, this);
    }

    //takes secondaryKey key and restores primary key bytes
    public SecondaryKeyValueToPKLoader GetSKKeyValueToPKMerger(uint secondaryKeyIndex)
    {
        return _secondaryKeyValueToPKLoader.GetOrAdd(secondaryKeyIndex,
            (_, secKeyIndex, relationInfo) => relationInfo.CreatePrimaryKeyFromSKDataMerger(
                secKeyIndex,
                $"Relation_SK_to_PK_{relationInfo.ClientRelationVersionInfo.SecondaryKeys[secKeyIndex].Name}"),
            secondaryKeyIndex, this);
    }

    struct MemorizedPositionWithLength
    {
        public IILLocal Pos { get; set; } // IMemorizedPosition
        public IILLocal Length { get; set; } // int
    }

    struct BufferInfo
    {
        public bool ReaderCreated;
        public Action<IILGen> PushReader;
        public Action<IILGen> PushCtx;
        public int ActualFieldIdx;
    }

    SecondaryKeyValueToPKLoader CreatePrimaryKeyFromSKDataMerger(uint secondaryKeyIndex, string mergerName)
    {
        var method = ILBuilder.Instance.NewMethod<SecondaryKeyValueToPKLoader>(mergerName);
        var ilGenerator = method.Generator;

        void PushWriter(IILGen il) => il.Ldarg(1);
        var skFields = ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Fields;

        var memoPositionLoc = ilGenerator.DeclareLocal(typeof(uint));

        var bufferInfo = new BufferInfo();
        var outOfOrderPKParts =
            new Dictionary<int, MemorizedPositionWithLength>(); //index -> bufferIdx, pos, length

        var pks = ClientRelationVersionInfo.PrimaryKeyFields.Span;
        for (var pkIdx = 0; pkIdx < pks.Length; pkIdx++)
        {
            if (pks[pkIdx].InKeyValue) break;
            if (outOfOrderPKParts.TryGetValue(pkIdx, out var memo))
            {
                CopyFromMemorizedPosition(ilGenerator, bufferInfo.PushReader, PushWriter, memo);
                continue;
            }

            FindPosition(pkIdx, skFields, out var skFieldIdx);
            MergerInitializeBufferReader(ilGenerator, ref bufferInfo,
                ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex));
            for (var idx = bufferInfo.ActualFieldIdx; idx < skFieldIdx; idx++)
            {
                var field = skFields[idx];
                if (field.IsFromPrimaryKey && !pks[(int)field.Index].InKeyValue)
                {
                    outOfOrderPKParts[(int)field.Index] = SkipWithMemorizing(ilGenerator, bufferInfo.PushReader,
                        pks[(int)field.Index].Handler!);
                }
                else
                {
                    var f = ClientRelationVersionInfo.GetSecondaryKeyField((int)field.Index);
                    f.Handler!.Skip(ilGenerator, bufferInfo.PushReader, bufferInfo.PushCtx);
                }
            }

            var skField = skFields[skFieldIdx];
            GenerateCopyFieldFromByteBufferToWriterIl(ilGenerator, pks[(int)skField.Index].Handler!,
                bufferInfo.PushReader,
                PushWriter, memoPositionLoc);

            bufferInfo.ActualFieldIdx = skFieldIdx + 1;
        }

        ilGenerator.Ret();
        return method.Create();
    }

    static void FindPosition(int pkIdx, IList<FieldId> skFields, out int skFieldIdx)
    {
        for (var i = 0; i < skFields.Count; i++)
        {
            var field = skFields[i];
            if (!field.IsFromPrimaryKey) continue;
            if (field.Index != pkIdx) continue;
            skFieldIdx = i;
            return;
        }

        throw new BTDBException("Secondary key relation processing error.");
    }

    void MergerInitializeBufferReader(IILGen ilGenerator, ref BufferInfo bi,
        in ReadOnlySpan<TableFieldInfo> tableSecondaryKeyFields)
    {
        if (bi.ReaderCreated)
            return;
        bi.ReaderCreated = true;
        bi.PushReader = il => il.Ldarg(0);

        ilGenerator
            //skip all relations
            .Do(bi.PushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!) // ObjectDB.AllRelationsSKPrefix
            //skip relation id (it is just 32bit, but 64bit skip is faster)
            .Do(bi.PushReader).Call(typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt64))!)
            //skip secondary key index
            .Do(bi.PushReader).Call(typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!);

        var anyNeedsCtx = false;
        foreach (var fieldInfo in tableSecondaryKeyFields)
        {
            if (fieldInfo.Handler!.NeedsCtx())
            {
                anyNeedsCtx = true;
                break;
            }
        }

        if (anyNeedsCtx)
        {
            var readerCtxLocal = ilGenerator.DeclareLocal(typeof(IReaderCtx));
            ilGenerator
                .Ldnull() // ctx is needed only for skipping encrypted strings, so it does not need valid transaction
                .Newobj(() => new DBReaderCtx(null))
                .Stloc(readerCtxLocal);
            bi.PushCtx = il => il.Ldloc(readerCtxLocal);
        }
    }

    static MemorizedPositionWithLength SkipWithMemorizing(IILGen ilGenerator, Action<IILGen> pushReader,
        IFieldHandler handler)
    {
        var memoPos = ilGenerator.DeclareLocal(typeof(uint));
        var memoLen = ilGenerator.DeclareLocal(typeof(uint));
        var position = new MemorizedPositionWithLength
            { Pos = memoPos, Length = memoLen };
        MemorizeCurrentPosition(ilGenerator, pushReader, memoPos);
        handler.Skip(ilGenerator, pushReader, null);
        ilGenerator
            .Do(pushReader) //[VR]
            .Call(typeof(MemReader).GetMethod(nameof(MemReader
                .GetCurrentPositionWithoutController))!) //[posNew(ulong)]
            .ConvU4() //[posNew(uint)]
            .Ldloc(memoPos) //[posNew(uint), posOld(uint)]
            .Sub() //[readLen(uint)]
            .Stloc(memoLen); //[]
        return position;
    }

    static void CopyFromMemorizedPosition(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushWriter,
        MemorizedPositionWithLength memo)
    {
        ilGenerator
            .Do(pushReader) //[reader]
            .Ldloc(memo.Pos) //[reader, pos]
            .Ldloc(memo.Length) //[reader, pos, readLen]
            .Do(pushWriter) //[reader, pos, readLen, writer]
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.CopyAbsoluteToWriter))!); //[]
    }

    public static void CopyFromPos(IILGen ilGenerator, Action<IILGen> pushReader, IILLocal posLocal,
        Action<IILGen> pushWriter)
    {
        ilGenerator
            .Do(pushReader) //[reader]
            .Ldloc(posLocal) //[reader, pos]
            .Do(pushWriter) //[reader, pos, writer]
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.CopyFromPosToWriter))!); //[]
    }

    public static void MemorizeCurrentPosition(IILGen ilGenerator, Action<IILGen> pushReader, IILLocal memoPositionLoc)
    {
        ilGenerator
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.GetCurrentPositionWithoutController))!)
            .ConvU4()
            .Stloc(memoPositionLoc);
    }

    void GenerateCopyFieldFromByteBufferToWriterIl(IILGen ilGenerator, IFieldHandler handler,
        Action<IILGen> pushReader,
        Action<IILGen> pushWriter, IILLocal memoPositionLoc)
    {
        MemorizeCurrentPosition(ilGenerator, pushReader, memoPositionLoc);

        handler.Skip(ilGenerator, pushReader, null);

        CopyFromPos(ilGenerator, pushReader, memoPositionLoc, pushWriter);
    }

    public object GetSimpleLoader(SimpleLoaderType handler)
    {
        return _simpleLoader.GetOrAdd(handler, CreateSimpleLoader);
    }

    object CreateSimpleLoader(SimpleLoaderType loaderType)
    {
        var delegateType = typeof(ReaderFun<>).MakeGenericType(loaderType.RealType);
        var dm = ILBuilder.Instance.NewMethod(loaderType.FieldHandler.Name + "SimpleReader", delegateType);
        var ilGenerator = dm.Generator;
        loaderType.FieldHandler.Load(ilGenerator, il => il.Ldarg(0), il => il.Ldarg(1));
        ilGenerator
            .Do(_relationInfoResolver.TypeConvertorGenerator.GenerateConversion(
                loaderType.FieldHandler.HandledType()!,
                loaderType.RealType)!)
            .Ret();
        return dm.Create();
    }

    static string GetPersistentName(PropertyInfo p)
    {
        var a = p.GetCustomAttribute<PersistedNameAttribute>();
        return a != null ? a.Name : p.Name;
    }

    internal static string GetPersistentName(string name, PropertyInfo[] properties)
    {
        foreach (var prop in properties)
        {
            if (prop.Name == name)
                return GetPersistentName(prop);
        }

        return name;
    }

    public void FreeContent(IInternalObjectDBTransaction tr, in ReadOnlySpan<byte> valueBytes)
    {
        FreeContentOldDict.Clear();
        FindUsedObjectsToFree(tr, valueBytes, FreeContentOldDict);

        foreach (var dictId in FreeContentOldDict)
        {
            FreeIDictionary(tr, dictId);
        }
    }

    internal static void FreeIDictionary(IInternalObjectDBTransaction tr, ulong dictId)
    {
        var len = PackUnpack.LengthVUInt(dictId);
        Span<byte> prefix = stackalloc byte[1 + (int)len];
        prefix[0] = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(prefix[1..]), dictId, len);
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        cursor.EraseAll(prefix);
    }

    public unsafe void FindUsedObjectsToFree(IInternalObjectDBTransaction tr, in ReadOnlySpan<byte> valueBytes,
        IList<ulong> dictionaries)
    {
        fixed (void* _ = valueBytes)
        {
            var valueReader = MemReader.CreateFromPinnedSpan(valueBytes);
            var version = valueReader.ReadVUInt32();
            GetIDictFinder(version).Invoke(tr, ref valueReader, dictionaries);
        }
    }

    FreeContentFun CreateIDictFinder(uint version)
    {
        var method = ILBuilder.Instance.NewMethod<FreeContentFun>($"Relation{Name}_IDictFinder");
        var ilGenerator = method.Generator;

        var relationVersionInfo = _relationVersions[version];
        var needGenerateFreeFor = 0;
        var fakeMethod = ILBuilder.Instance.NewMethod<Action>("Relation_fake");
        var fakeGenerator = fakeMethod.Generator;
        var valueFields = relationVersionInfo!.Fields.ToArray();
        for (var i = 0; i < valueFields.Length; i++)
        {
            if (valueFields[i].Computed || valueFields[i].InKeyValue) continue;
            var needsFreeContent = valueFields[i].Handler!.FreeContent(fakeGenerator, _ => { }, _ => { });
            if (needsFreeContent != NeedsFreeContent.No)
                needGenerateFreeFor = i + 1;
        }

        if (needGenerateFreeFor == 0)
        {
            return (IInternalObjectDBTransaction _, ref MemReader _, IList<ulong> _) => { };
        }

        _needImplementFreeContent = true;

        if (relationVersionInfo.NeedsCtx())
        {
            ilGenerator.DeclareLocal(typeof(IReaderCtx)); //loc 0
            ilGenerator
                .Ldarg(0)
                .Ldarg(2)
                .Newobj(() => new DBReaderWithFreeInfoCtx(null, null))
                .Stloc(0);
        }

        for (var i = 0; i < needGenerateFreeFor; i++)
        {
            valueFields[i].Handler!.FreeContent(ilGenerator, il => il.Ldarg(1), il => il.Ldloc(0));
        }

        ilGenerator.Ret();
        return method.Create();
    }

    void CalculatePrefix()
    {
        var len = PackUnpack.LengthVUInt(Id);
        var prefix = new byte[1 + len];
        prefix[0] = ObjectDB.AllRelationsPKPrefixByte;
        PackUnpack.UnsafePackVUInt(ref prefix[1], Id, len);
        Prefix = prefix;
        prefix = new byte[1 + len];
        prefix[0] = ObjectDB.AllRelationsSKPrefixByte;
        PackUnpack.UnsafePackVUInt(ref prefix[1], Id, len);
        PrefixSecondary = prefix;
    }

    public override string ToString()
    {
        return $"{Name} {ClientType} Id:{Id}";
    }
}

public class DBReaderWithFreeInfoCtx : DBReaderCtx
{
    readonly IList<ulong> _freeDictionaries;
    StructList<bool> _seenObjects;

    public DBReaderWithFreeInfoCtx(IInternalObjectDBTransaction transaction, IList<ulong> freeDictionaries)
        : base(transaction)
    {
        _freeDictionaries = freeDictionaries;
    }

    public IList<ulong> DictIds => _freeDictionaries;

    public override void RegisterDict(ulong dictId)
    {
        _freeDictionaries.Add(dictId);
    }

    [SkipLocalsInit]
    public override unsafe void FreeContentInNativeObject(ref MemReader outsideReader)
    {
        var id = outsideReader.ReadVInt64();
        if (id == 0)
        {
        }
        else if (id is <= int.MinValue or > 0)
        {
            using var cursor = Transaction!.KeyValueDBTransaction.CreateCursor();
            if (!cursor.FindExactKey(
                    ObjectDBTransaction.BuildKeyFromOidWithAllObjectsPrefix((ulong)id, stackalloc byte[10])))
                return;
            Span<byte> buffer = stackalloc byte[2048];
            var valueSpan = cursor.GetValueSpan(ref buffer);
            fixed (void* _ = valueSpan)
            {
                var reader = MemReader.CreateFromPinnedSpan(valueSpan);
                var tableId = reader.ReadVUInt32();
                var tableInfo = ((ObjectDB)Transaction.Owner).TablesInfo.FindById(tableId);
                if (tableInfo == null)
                    return;
                var tableVersion = reader.ReadVUInt32();
                var freeContentTuple = tableInfo.GetFreeContent(tableVersion);
                if (freeContentTuple.Item1 != NeedsFreeContent.No)
                {
                    freeContentTuple.Item2(Transaction, null, ref reader, _freeDictionaries);
                }
            }
        }
        else
        {
            var ido = (int)-id - 1;
            if (!AlreadyProcessedInstance(ido))
                Transaction!.FreeContentInNativeObject(ref outsideReader, this);
        }
    }

    bool AlreadyProcessedInstance(int ido)
    {
        while (_seenObjects.Count <= ido) _seenObjects.Add(false);
        var res = _seenObjects[ido];
        _seenObjects[ido] = true;
        return res;
    }
}
