using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;
using Extensions = BTDB.FieldHandler.Extensions;

namespace BTDB.ODBLayer;

delegate void ObjectSaver(IInternalObjectDBTransaction transaction, DBObjectMetadata? metadata,
    ref MemWriter writer, object value);

delegate void ObjectLoader(IInternalObjectDBTransaction transaction, DBObjectMetadata metadata,
    ref MemReader reader, object value);

delegate void ObjectFreeContent(IInternalObjectDBTransaction transaction, DBObjectMetadata? metadata,
    ref MemReader reader, IList<ulong> dictIds);

public class TableInfo
{
    readonly ITableInfoResolver _tableInfoResolver;
    public uint ClientTypeVersion;
    public uint LastPersistedVersion;
    Type? _clientType;

    public readonly ConcurrentDictionary<uint, TableVersionInfo> TableVersions = new();

    Func<IInternalObjectDBTransaction, DBObjectMetadata, object>? _creator;
    Action<IInternalObjectDBTransaction, DBObjectMetadata, object>? _initializer;
    ObjectSaver? _saver;

    readonly ConcurrentDictionary<uint, ObjectLoader> _loaders = new();
    readonly ConcurrentDictionary<uint, ObjectLoader> _skippers = new();

    readonly ConcurrentDictionary<uint, Tuple<bool, ObjectFreeContent>> _freeContent = new();

    readonly ConcurrentDictionary<uint, bool> _freeContentNeedDetectionInProgress = new();

    long _singletonOid;
    long _cachedSingletonTrNum;
    ReadOnlyMemory<byte> _cachedSingletonContent;
    readonly object _cachedSingletonLock = new();

    internal TableInfo(uint id, string name, ITableInfoResolver tableInfoResolver)
    {
        Id = id;
        Name = name;
        _tableInfoResolver = tableInfoResolver;
        NeedStoreSingletonOid = false;
    }

    public uint Id { get; }

    public string Name { get; }

    public Type? ClientType
    {
        get => _clientType;
        set
        {
            if (_clientType != null && _clientType != value)
            {
                throw new BTDBException("Name " + Name + " has already assigned type " +
                                        _clientType.ToSimpleName() +
                                        ", but " + value!.ToSimpleName() + " want to be stored under same name");
            }

            _clientType = value;
            if (value?.GetCustomAttribute(typeof(RequireContentFreeAttribute)) != null) _freeContentRequired = true;
        }
    }

    bool _freeContentRequired;

    internal TableVersionInfo? ClientTableVersionInfo
    {
        get
        {
            if (TableVersions.TryGetValue(ClientTypeVersion, out var tvi)) return tvi;
            return null;
        }
    }

    internal Func<IInternalObjectDBTransaction, DBObjectMetadata, object> Creator
    {
        get
        {
            if (_creator == null) CreateCreator();
            return _creator!;
        }
    }

    void CreateCreator()
    {
        var factoryType = typeof(Func<>).MakeGenericType(_clientType!);
        var container = _tableInfoResolver.Container;
        object? factory = null;
        if (container != null)
        {
            factory = container.ResolveOptional(factoryType);
        }


        if (factory != null)
        {
            factoryType = factory.GetType();
            // ReSharper disable once EqualExpressionComparison intentional
            if (((Func<object>)factory)() == ((Func<object>)factory)())
            {
                _tableInfoResolver.ActualOptions.ThrowBTDBException(_clientType.ToSimpleName() +
                                                                    " cannot be registered as singleton");
            }

            var method = ILBuilder.Instance.NewMethod(
                $"Creator_{Name}", typeof(Func<IInternalObjectDBTransaction, DBObjectMetadata, object>),
                factoryType);
            var ilGenerator = method.Generator;
            ilGenerator
                .Ldarg(0)
                .Callvirt(factoryType.GetMethod(nameof(Func<object>.Invoke))!)
                .Ret();
            var creator = (Func<IInternalObjectDBTransaction, DBObjectMetadata, object>)method.Create(factory);
            Interlocked.CompareExchange(ref _creator, creator, null);
        }
        else
        {
            var method = ILBuilder.Instance.NewMethod<Func<IInternalObjectDBTransaction, DBObjectMetadata, object>>(
                $"Creator_{Name}");
            var ilGenerator = method.Generator;
            var defaultConstructor = _clientType.GetDefaultConstructor();
            if (defaultConstructor == null)
            {
                ilGenerator
                    .Ldtoken(_clientType)
                    .Call(() => Type.GetTypeFromHandle(new()))
                    .Call(() => RuntimeHelpers.GetUninitializedObject(null));
            }
            else
            {
                ilGenerator
                    .Newobj(defaultConstructor);
            }

            ilGenerator
                .Ret();
            var creator = method.Create();
            Interlocked.CompareExchange(ref _creator, creator, null);
        }
    }

    internal Action<IInternalObjectDBTransaction, DBObjectMetadata, object> Initializer
    {
        get
        {
            if (_initializer == null) CreateInitializer();
            return _initializer!;
        }
    }

    void CreateInitializer()
    {
        EnsureClientTypeVersion();
        var tableVersionInfo = ClientTableVersionInfo;
        var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, object>>(
            $"Initializer_{Name}");
        var ilGenerator = method.Generator;
        if (tableVersionInfo!.NeedsInit())
        {
            ilGenerator.DeclareLocal(ClientType!);
            ilGenerator
                .Ldarg(2)
                .Castclass(ClientType)
                .Stloc(0);
            var anyNeedsCtx = tableVersionInfo.NeedsCtx();
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Newobj(() => new DBReaderCtx(null))
                    .Stloc(1);
            }

            var props = _clientType!.GetProperties(BindingFlags.Public | BindingFlags.NonPublic |
                                                   BindingFlags.Instance);
            for (var fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                var iFieldHandlerWithInit = srcFieldInfo.Handler as IFieldHandlerWithInit;
                if (iFieldHandlerWithInit == null) continue;
                Action<IILGen> readerOrCtx;
                if (srcFieldInfo.Handler.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(1);
                else
                    readerOrCtx = il => il.Ldnull();
                var specializedSrcHandler = srcFieldInfo.Handler;
                var willLoad = specializedSrcHandler.HandledType();
                var setterMethod = props.First(p => GetPersistentName(p) == srcFieldInfo.Name).GetAnySetMethod();
                var converterGenerator =
                    _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad,
                        setterMethod!.GetParameters()[0].ParameterType);
                if (converterGenerator == null) continue;
                if (!iFieldHandlerWithInit.NeedInit()) continue;
                ilGenerator.Ldloc(0);
                iFieldHandlerWithInit.Init(ilGenerator, readerOrCtx);
                converterGenerator(ilGenerator);
                ilGenerator.Call(setterMethod);
            }
        }

        ilGenerator.Ret();
        var initializer = method.Create();
        Interlocked.CompareExchange(ref _initializer, initializer, null);
    }

    internal ObjectSaver Saver
    {
        get
        {
            if (_saver == null) CreateSaver();
            return _saver!;
        }
    }

    public long LazySingletonOid
    {
        get
        {
            var singletonOid = Interlocked.Read(ref _singletonOid);
            if (singletonOid != 0) return singletonOid;
            Interlocked.CompareExchange(ref _singletonOid, _tableInfoResolver.GetSingletonOid(Id), 0);
            singletonOid = Interlocked.Read(ref _singletonOid);
            return singletonOid;
        }
    }

    public long SingletonOid
    {
        get
        {
            var singletonOid = Interlocked.Read(ref _singletonOid);
            if (singletonOid != 0) return singletonOid;
            singletonOid =
                Interlocked.CompareExchange(ref _singletonOid, _tableInfoResolver.GetSingletonOid(Id), 0);
            if (singletonOid != 0) return singletonOid;
            singletonOid = Interlocked.Read(ref _singletonOid);
            if (singletonOid != 0) return singletonOid;
            NeedStoreSingletonOid = true;
            var newSingletonOid = (long)_tableInfoResolver.AllocateNewOid();
            singletonOid = Interlocked.CompareExchange(ref _singletonOid, newSingletonOid, 0);
            if (singletonOid == 0) singletonOid = newSingletonOid;
            return singletonOid;
        }
    }

    public bool NeedStoreSingletonOid { get; private set; }

    public void ResetNeedStoreSingletonOid()
    {
        NeedStoreSingletonOid = false;
    }

    void CreateSaver()
    {
        var method = ILBuilder.Instance
            .NewMethod<ObjectSaver>(
                $"Saver_{Name}");
        var ilGenerator = method.Generator;
        ilGenerator.DeclareLocal(ClientType!);
        ilGenerator
            .Ldarg(3)
            .Castclass(ClientType)
            .Stloc(0);
        var anyNeedsCtx = ClientTableVersionInfo!.NeedsCtx();
        if (anyNeedsCtx)
        {
            ilGenerator.DeclareLocal(typeof(IWriterCtx));
            ilGenerator
                .Ldarg(0)
                .Newobj(() => new DBWriterCtx(null))
                .Stloc(1);
        }

        var props = ClientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        for (var i = 0; i < ClientTableVersionInfo.FieldCount; i++)
        {
            var field = ClientTableVersionInfo[i];
            var getter = props.First(p => GetPersistentName(p) == field.Name).GetAnyGetMethod();
            var handler = field.Handler!.SpecializeSaveForType(getter!.ReturnType);
            var writerOrCtx = handler.NeedsCtx() ? (Action<IILGen>?)(il => il.Ldloc(1)) : null;
            handler.Save(ilGenerator, il => il.Ldarg(2), writerOrCtx, il =>
            {
                il.Ldloc(0).Callvirt(getter);
                _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(getter.ReturnType,
                    handler.HandledType())!(il);
            });
        }

        ilGenerator
            .Ret();
        var saver = method.Create();
        Interlocked.CompareExchange(ref _saver, saver, null);
    }

    internal void EnsureClientTypeVersion()
    {
        if (ClientTypeVersion != 0) return;
        EnsureKnownLastPersistedVersion();

        var publicFields = _clientType!.GetFields(BindingFlags.Public | BindingFlags.Instance);
        foreach (var field in publicFields)
        {
            if (field.GetCustomAttribute<NotStoredAttribute>(true) != null) continue;
            throw new BTDBException(
                $"Public field {_clientType.ToSimpleName()}.{field.Name} must have NotStoredAttribute. It is just intermittent, until they can start to be supported.");
        }

        var props = _clientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        var fields = new StructList<TableFieldInfo>();
        fields.Reserve((uint)props.Length);
        foreach (var pi in props)
        {
            if (pi.GetCustomAttribute<NotStoredAttribute>(true) != null) continue;
            if (pi.GetIndexParameters().Length != 0) continue;

            if (pi.GetAnyGetMethod() == null)
                throw new InvalidOperationException("Trying to serialize type " + _clientType.ToSimpleName() +
                                                    " and property " + pi.Name +
                                                    " does not have getter. If you don't want to serialize this property add [NotStored] attribute.");
            if (pi.GetAnySetMethod() == null)
            {
                if (pi.GetCustomAttribute<CompilerGeneratedAttribute>() is not null) continue;
                throw new InvalidOperationException("Trying to serialize type " + _clientType.ToSimpleName() +
                                                    " and property " + pi.Name +
                                                    " does not have setter. If you don't want to serialize this property add [NotStored] attribute.");
            }

            fields.Add(TableFieldInfo.Build(Name, pi, _tableInfoResolver.FieldHandlerFactory,
                FieldHandlerOptions.None, pi.GetCustomAttribute<PrimaryKeyAttribute>()?.InKeyValue ?? false));
        }

        var tvi = new TableVersionInfo(fields.ToArray());
        if (LastPersistedVersion == 0)
        {
            TableVersions.TryAdd(1, tvi);
            ClientTypeVersion = 1;
        }
        else
        {
            var last = TableVersions.GetOrAdd(LastPersistedVersion,
                (ver, tableInfo) =>
                    tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
            if (TableVersionInfo.Equal(last, tvi))
            {
                TableVersions[LastPersistedVersion] =
                    tvi; // tvi was build from real types and not loaded so it is more exact
                ClientTypeVersion = LastPersistedVersion;
            }
            else
            {
                TableVersions.TryAdd(LastPersistedVersion + 1, tvi);
                ClientTypeVersion = LastPersistedVersion + 1;
            }
        }
    }

    void EnsureKnownLastPersistedVersion()
    {
        if (LastPersistedVersion != 0) return;
        LastPersistedVersion = _tableInfoResolver.GetLastPersistedVersion(Id);
    }

    internal ObjectLoader GetLoader(uint version)
    {
        return _loaders.GetOrAdd(version, CreateLoader);
    }

    delegate void LoadFunc(object obj, ref MemReader reader, IReaderCtx? ctx);

    ref struct FieldLoaderCtx
    {
        internal nint StoragePtr;
        internal object Object;
        internal FieldHandlerLoad Loader;
        internal IReaderCtx? Ctx;
        internal ref MemReader Reader;
        internal unsafe delegate*<object, ref byte, void> Setter;
        internal FieldHandlerInit Init;
    }

    unsafe ObjectLoader CreateLoader(uint version)
    {
        EnsureClientTypeVersion();
        var tableVersionInfo = TableVersions.GetOrAdd(version,
            (ver, tableInfo) =>
                tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
        var clientTableVersionInfo = ClientTableVersionInfo;
        var anyNeedsCtx = tableVersionInfo.NeedsCtx() || clientTableVersionInfo!.NeedsCtx();
        StructList<LoadFunc> loaders = new();

#pragma warning disable CS0162 // Unreachable code detected
        if (IFieldHandler.UseNoEmit)
        {
            var metadata = ReflectionMetadata.FindByType(_clientType);
            if (metadata == null)
                throw new BTDBException("Type " + _clientType.ToSimpleName() + " does not have [Generate] attribute");
            var fieldsMetadata = metadata.Fields;
            var setFields = new HashSet<string>();
            for (var fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                var destFieldInfo = clientTableVersionInfo![srcFieldInfo.Name];
                if (destFieldInfo != null)
                {
                    setFields.Add(destFieldInfo.Name);
                    var fieldInfo = fieldsMetadata.FirstOrDefault(p => p.Name == destFieldInfo.Name);
                    if (fieldInfo == null || fieldInfo.PropRefSetter == null && fieldInfo.ByteOffset == null)
                    {
                        throw new InvalidOperationException($"Cannot field metadata for {destFieldInfo.Name} in " +
                                                            _clientType.ToSimpleName());
                    }

                    try
                    {
                        var handlerLoad =
                            srcFieldInfo.Handler!.Load(fieldInfo.Type, _tableInfoResolver.TypeConverterFactory);
                        if (fieldInfo.PropRefSetter == null)
                        {
                            var offset = fieldInfo.ByteOffset.Value;
                            loaders.Add((object obj, ref MemReader reader, IReaderCtx? ctx) =>
                            {
                                handlerLoad(ref reader, ctx, ref RawData.Ref(obj, offset));
                            });
                            continue;
                        }

                        var propRefSetter = fieldInfo.PropRefSetter;
                        if (!fieldInfo.Type.IsValueType)
                        {
                            loaders.Add((object obj, ref MemReader reader, IReaderCtx? ctx) =>
                            {
                                object? value = null;
                                handlerLoad(ref reader, ctx, ref Unsafe.As<object, byte>(ref value));
                                propRefSetter(obj, ref Unsafe.As<object, byte>(ref value));
                            });
                            continue;
                        }

                        if (!RawData.MethodTableOf(fieldInfo.Type).ContainsGCPointers &&
                            RawData.GetSizeAndAlign(fieldInfo.Type).Size <= 16)
                        {
                            loaders.Add((object obj, ref MemReader reader, IReaderCtx? ctx) =>
                            {
                                Int128 value = 0;
                                handlerLoad(ref reader, ctx, ref Unsafe.As<Int128, byte>(ref value));
                                propRefSetter(obj, ref Unsafe.As<Int128, byte>(ref value));
                            });
                        }

                        var stackAllocator = ReflectionMetadata.FindStackAllocatorByType(fieldInfo.Type);
                        loaders.Add((object obj, ref MemReader reader, IReaderCtx? ctx) =>
                        {
                            FieldLoaderCtx fieldLoaderCtx = new FieldLoaderCtx()
                            {
                                Object = obj,
                                Loader = handlerLoad,
                                Setter = propRefSetter,
                                Ctx = ctx,
                                Reader = reader
                            };

                            stackAllocator(ref Unsafe.As<FieldLoaderCtx, byte>(ref fieldLoaderCtx),
                                ref fieldLoaderCtx.StoragePtr, &Nested);

                            static void Nested(ref byte value)
                            {
                                ref var context = ref Unsafe.As<byte, FieldLoaderCtx>(ref value);
                                context.Loader(ref context.Reader, context.Ctx,
                                    ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                                context.Setter(context.Object, ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                            }
                        });

                        continue;
                    }
                    catch (Exception)
                    {
                        _tableInfoResolver.FieldHandlerLogger?.ReportTypeIncompatibility(
                            srcFieldInfo.Handler.HandledType(),
                            srcFieldInfo.Handler, fieldInfo.Type, destFieldInfo.Handler);
                    }
                }

                var handler = srcFieldInfo.Handler!;
                loaders.Add((object _, ref MemReader reader, IReaderCtx? ctx) => { handler.Skip(ref reader, ctx); });
                if (destFieldInfo != null) setFields.Remove(destFieldInfo.Name);
            }

            for (var fi = 0; fi < clientTableVersionInfo!.FieldCount; fi++)
            {
                var srcFieldInfo = clientTableVersionInfo[fi];
                if (setFields.Contains(srcFieldInfo.Name)) continue;
                var handler = srcFieldInfo.Handler!;
                var iFieldHandlerWithInit = handler as IFieldHandlerWithInit;
                if (iFieldHandlerWithInit == null) continue;
                if (!iFieldHandlerWithInit.NeedInit()) continue;
                var fieldInfo = fieldsMetadata.FirstOrDefault(p => p.Name == srcFieldInfo.Name);
                if (fieldInfo == null)
                {
                    throw new InvalidOperationException($"Cannot field metadata for {srcFieldInfo.Name} in " +
                                                        _clientType.ToSimpleName());
                }

                var init = iFieldHandlerWithInit.Init();
                var propRefSetter = fieldInfo.PropRefSetter;
                if (propRefSetter == null)
                {
                    var offset = fieldInfo.ByteOffset.Value;
                    loaders.Add((object obj, ref MemReader _, IReaderCtx? ctx) =>
                    {
                        init(ctx, ref RawData.Ref(obj, offset));
                    });
                    continue;
                }

                if (!fieldInfo.Type.IsValueType)
                {
                    loaders.Add((object obj, ref MemReader _, IReaderCtx? ctx) =>
                    {
                        object? value = null;
                        init(ctx, ref Unsafe.As<object, byte>(ref value));
                        propRefSetter(obj, ref Unsafe.As<object, byte>(ref value));
                    });
                    continue;
                }

                if (!RawData.MethodTableOf(fieldInfo.Type).ContainsGCPointers &&
                    RawData.GetSizeAndAlign(fieldInfo.Type).Size <= 16)
                {
                    loaders.Add((object obj, ref MemReader _, IReaderCtx? ctx) =>
                    {
                        Int128 value = 0;
                        init(ctx, ref Unsafe.As<Int128, byte>(ref value));
                        propRefSetter(obj, ref Unsafe.As<Int128, byte>(ref value));
                    });
                }

                var stackAllocator = ReflectionMetadata.FindStackAllocatorByType(fieldInfo.Type);
                loaders.Add((object obj, ref MemReader _, IReaderCtx? ctx) =>
                {
                    FieldLoaderCtx fieldLoaderCtx = new FieldLoaderCtx()
                    {
                        Object = obj,
                        Init = init,
                        Setter = propRefSetter,
                        Ctx = ctx,
                        Reader = default
                    };

                    stackAllocator(ref Unsafe.As<FieldLoaderCtx, byte>(ref fieldLoaderCtx),
                        ref fieldLoaderCtx.StoragePtr, &Nested);

                    static void Nested(ref byte value)
                    {
                        ref var context = ref Unsafe.As<byte, FieldLoaderCtx>(ref value);
                        context.Init(context.Ctx, ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                        context.Setter(context.Object, ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                    }
                });
            }

            var loadersArray = loaders.ToArray();
            return (IInternalObjectDBTransaction transaction, DBObjectMetadata metadata, ref MemReader reader,
                object value) =>
            {
                var ctx = anyNeedsCtx ? new DBReaderCtx(transaction) : null;
                foreach (var loadFunc in loadersArray)
                {
                    loadFunc(value, ref reader, ctx);
                }
            };
        }
        else
        {
            var method = ILBuilder.Instance
                .NewMethod<ObjectLoader>(
                    $"Loader_{Name}_{version}");
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType!);
            ilGenerator
                .Ldarg(3)
                .Castclass(ClientType)
                .Stloc(0);
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Newobj(() => new DBReaderCtx(null))
                    .Stloc(1);
            }

            var props = _clientType!.GetProperties(BindingFlags.Public | BindingFlags.NonPublic |
                                                   BindingFlags.Instance);
            for (var fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                var readerOrCtx = srcFieldInfo.Handler!.NeedsCtx() ? (Action<IILGen>?)(il => il.Ldloc(1)) : null;
                var destFieldInfo = clientTableVersionInfo![srcFieldInfo.Name];
                if (destFieldInfo != null)
                {
                    var fieldInfo = props.First(p => GetPersistentName(p) == destFieldInfo.Name).GetAnySetMethod();
                    if (fieldInfo == null)
                    {
                        throw new InvalidOperationException($"Cannot find setter for {destFieldInfo.Name}");
                    }

                    var fieldType = fieldInfo.GetParameters()[0].ParameterType;
                    var specializedSrcHandler =
                        srcFieldInfo.Handler.SpecializeLoadForType(fieldType, destFieldInfo.Handler,
                            _tableInfoResolver.FieldHandlerLogger);
                    var willLoad = specializedSrcHandler.HandledType();
                    var converterGenerator =
                        _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, fieldType);
                    if (converterGenerator != null)
                    {
                        if (willLoad != fieldType)
                        {
                            specializedSrcHandler.Load(ilGenerator, il => il.Ldarg(2), readerOrCtx);
                            converterGenerator(ilGenerator);
                            var local = ilGenerator.DeclareLocal(fieldType);
                            ilGenerator.Stloc(local).Ldloc(0).Ldloc(local);
                        }
                        else
                        {
                            ilGenerator.Ldloc(0);
                            specializedSrcHandler.Load(ilGenerator, il => il.Ldarg(2), readerOrCtx);
                        }

                        ilGenerator.Call(fieldInfo);
                        continue;
                    }

                    _tableInfoResolver.FieldHandlerLogger?.ReportTypeIncompatibility(willLoad,
                        srcFieldInfo.Handler, fieldType, destFieldInfo.Handler);
                }

                srcFieldInfo.Handler.Skip(ilGenerator, il => il.Ldarg(2), readerOrCtx);
            }

            if (ClientTypeVersion != version)
            {
                for (var fi = 0; fi < clientTableVersionInfo!.FieldCount; fi++)
                {
                    var srcFieldInfo = clientTableVersionInfo[fi];
                    var iFieldHandlerWithInit = srcFieldInfo.Handler as IFieldHandlerWithInit;
                    if (iFieldHandlerWithInit == null) continue;
                    if (tableVersionInfo[srcFieldInfo.Name] != null) continue;
                    Action<IILGen> readerOrCtx;
                    if (srcFieldInfo.Handler.NeedsCtx())
                        readerOrCtx = il => il.Ldloc(1);
                    else
                        readerOrCtx = il => il.Ldnull();
                    var specializedSrcHandler = srcFieldInfo.Handler;
                    var willLoad = specializedSrcHandler.HandledType();
                    var setterMethod = props.First(p => GetPersistentName(p) == srcFieldInfo.Name).GetAnySetMethod();
                    var converterGenerator =
                        _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad,
                            setterMethod!.GetParameters()[0].ParameterType);
                    if (converterGenerator == null) continue;
                    if (!iFieldHandlerWithInit.NeedInit()) continue;
                    ilGenerator.Ldloc(0);
                    iFieldHandlerWithInit.Init(ilGenerator, readerOrCtx);
                    converterGenerator(ilGenerator);
                    ilGenerator.Call(setterMethod);
                }
            }

            ilGenerator.Ret();
            return method.Create();
        }
#pragma warning restore CS0162 // Unreachable code detected
    }

    internal ObjectLoader GetSkipper(uint version)
    {
        return _skippers.GetOrAdd(version, CreateSkipper);
    }

    ObjectLoader CreateSkipper(uint version)
    {
        var method = ILBuilder.Instance
            .NewMethod<ObjectLoader>(
                $"Skipper_{Name}_{version}");
        var ilGenerator = method.Generator;
        var tableVersionInfo = TableVersions.GetOrAdd(version, static (ver, tableInfo) =>
            tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
        var anyNeedsCtx = tableVersionInfo.NeedsCtx();
        IILLocal? ctxLocal = null;
        if (anyNeedsCtx)
        {
            ctxLocal = ilGenerator.DeclareLocal(typeof(IReaderCtx));
            ilGenerator
                .Ldarg(0)
                .Newobj(() => new DBReaderCtx(null))
                .Stloc(ctxLocal);
        }

        for (var fi = 0; fi < tableVersionInfo.FieldCount; fi++)
        {
            var srcFieldInfo = tableVersionInfo[fi];
            var readerOrCtx = srcFieldInfo.Handler!.NeedsCtx() ? (Action<IILGen>?)(il => il.Ldloc(ctxLocal!)) : null;
            srcFieldInfo.Handler.Skip(ilGenerator, il => il.Ldarg(2), readerOrCtx);
        }

        ilGenerator.Ret();
        return method.Create();
    }

    internal bool IsFreeContentNeeded(uint version)
    {
        if (_freeContentRequired) return true;
        if (_freeContent.TryGetValue(version, out var freeContent))
            return freeContent.Item1;
        if (_freeContentNeedDetectionInProgress.ContainsKey(version))
            return false; //if needed is reported by the other detection in progress
        _freeContentNeedDetectionInProgress[version] = true;
        var result = GetFreeContent(version).Item1;
        _freeContentNeedDetectionInProgress.TryRemove(version);
        return result;
    }

    internal Tuple<bool, ObjectFreeContent> GetFreeContent(uint version)
    {
        return _freeContent.GetOrAdd(version, CreateFreeContent);
    }

    Tuple<bool, ObjectFreeContent> CreateFreeContent(uint version)
    {
        var tableVersionInfo = TableVersions.GetOrAdd(version, static (ver, tableInfo) =>
            tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
        var anyNeedsCtx = tableVersionInfo.NeedsCtx();

        var doesNeedFreeContent = false;
        var visitedTypes = new HashSet<Type>();
        var handlerList = new List<IFieldHandler>();
        for (var fi = 0; fi < tableVersionInfo.FieldCount; fi++)
        {
            var srcFieldInfo = tableVersionInfo[fi];
            doesNeedFreeContent |= srcFieldInfo.Handler!.DoesNeedFreeContent(visitedTypes);
            handlerList.Add(srcFieldInfo.Handler);
        }

        var handlers = handlerList.ToArray();
        return anyNeedsCtx
            ? Tuple.Create(doesNeedFreeContent, (ObjectFreeContent)((IInternalObjectDBTransaction transaction,
                DBObjectMetadata? metadata,
                ref MemReader reader, IList<ulong> dictIds) =>
            {
                var ctx = new DBReaderWithFreeInfoCtx(transaction, dictIds);
                foreach (var handler in handlers)
                {
                    handler.FreeContent(ref reader, ctx);
                }
            }))
            : Tuple.Create(doesNeedFreeContent, (ObjectFreeContent)((IInternalObjectDBTransaction transaction,
                DBObjectMetadata? metadata,
                ref MemReader reader, IList<ulong> dictIds) =>
            {
                foreach (var handler in handlers)
                {
                    handler.FreeContent(ref reader, null);
                }
            }));
    }

    static string GetPersistentName(PropertyInfo p)
    {
        var a = p.GetCustomAttribute<PersistedNameAttribute>();
        return a != null ? a.Name : p.Name;
    }

    internal static byte[] BuildKeyForTableVersions(uint tableId, uint tableVersion)
    {
        Span<byte> buf = stackalloc byte[2 + 5 + 5];
        var writer = MemWriter.CreateFromPinnedSpan(buf);
        writer.WriteBlock(ObjectDB.TableVersionsPrefix);
        writer.WriteVUInt32(tableId);
        writer.WriteVUInt32(tableVersion);
        return writer.GetSpan().ToArray();
    }

    public ReadOnlyMemory<byte> SingletonContent(long transactionNumber)
    {
        lock (_cachedSingletonLock)
        {
            if (_cachedSingletonTrNum - transactionNumber > 0) return default;
            return _cachedSingletonContent;
        }
    }

    public void CacheSingletonContent(long transactionNumber, ReadOnlyMemory<byte> content)
    {
        lock (_cachedSingletonLock)
        {
            if (transactionNumber - _cachedSingletonTrNum < 0) return;
            _cachedSingletonTrNum = transactionNumber;
            _cachedSingletonContent = content;
        }
    }

    public bool IsSingletonOid(ulong id)
    {
        return (ulong)Interlocked.Read(ref _singletonOid) == id;
    }
}
