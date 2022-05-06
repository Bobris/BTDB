using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.StreamLayer;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.Collections;
using BTDB.KVDBLayer;
using Extensions = BTDB.FieldHandler.Extensions;

namespace BTDB.ODBLayer;

delegate void ObjectSaver(IInternalObjectDBTransaction transaction, DBObjectMetadata? metadata,
    ref SpanWriter writer, object value);

delegate void ObjectLoader(IInternalObjectDBTransaction transaction, DBObjectMetadata metadata,
    ref SpanReader reader, object value);

delegate void ObjectFreeContent(IInternalObjectDBTransaction transaction, DBObjectMetadata? metadata,
    ref SpanReader reader, IList<ulong> dictIds);

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

    readonly ConcurrentDictionary<uint, Tuple<NeedsFreeContent, ObjectFreeContent>> _freeContent = new();

    readonly ConcurrentDictionary<uint, bool> _freeContentNeedDetectionInProgress = new();

    long _singletonOid;
    long _cachedSingletonTrNum;
    byte[]? _cachedSingletonContent;
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

    bool _freeContentRequired = false;
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
            fields.Add(TableFieldInfo.Build(Name, pi, _tableInfoResolver.FieldHandlerFactory,
                FieldHandlerOptions.None));
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

    ObjectLoader CreateLoader(uint version)
    {
        EnsureClientTypeVersion();
        var method = ILBuilder.Instance
            .NewMethod<ObjectLoader>(
                $"Loader_{Name}_{version}");
        var ilGenerator = method.Generator;
        ilGenerator.DeclareLocal(ClientType!);
        ilGenerator
            .Ldarg(3)
            .Castclass(ClientType)
            .Stloc(0);
        var tableVersionInfo = TableVersions.GetOrAdd(version,
            (ver, tableInfo) =>
                tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
        var clientTableVersionInfo = ClientTableVersionInfo;
        var anyNeedsCtx = tableVersionInfo.NeedsCtx() || clientTableVersionInfo!.NeedsCtx();
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

    internal NeedsFreeContent IsFreeContentNeeded(uint version)
    {
        if (_freeContentRequired) return NeedsFreeContent.Yes;
        if (_freeContent.TryGetValue(version, out var freeContent))
            return freeContent.Item1;
        if (_freeContentNeedDetectionInProgress.ContainsKey(version))
            return NeedsFreeContent.No; //if needed is reported by the other detection in progress
        _freeContentNeedDetectionInProgress[version] = true;
        var result = GetFreeContent(version).Item1;
        _freeContentNeedDetectionInProgress.TryRemove(version);
        return result;
    }

    internal Tuple<NeedsFreeContent, ObjectFreeContent> GetFreeContent(uint version)
    {
        return _freeContent.GetOrAdd(version, CreateFreeContent);
    }

    Tuple<NeedsFreeContent, ObjectFreeContent> CreateFreeContent(uint version)
    {
        var method = ILBuilder.Instance.NewMethod<ObjectFreeContent>($"FreeContent_{Name}_{version}");
        var ilGenerator = method.Generator;
        var tableVersionInfo = TableVersions.GetOrAdd(version,
            (ver, tableInfo) =>
                tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
        var needsFreeContent = NeedsFreeContent.No;
        var anyNeedsCtx = tableVersionInfo.NeedsCtx();
        if (anyNeedsCtx)
        {
            ilGenerator.DeclareLocal(typeof(IReaderCtx));
            ilGenerator
                .Ldarg(0)
                .Ldarg(3)
                .Newobj(() => new DBReaderWithFreeInfoCtx(null, null))
                .Stloc(0);
        }

        for (var fi = 0; fi < tableVersionInfo.FieldCount; fi++)
        {
            var srcFieldInfo = tableVersionInfo[fi];
            Extensions.UpdateNeedsFreeContent(
                srcFieldInfo.Handler!.FreeContent(ilGenerator, il => il.Ldarg(2), il => il.Ldloc(0)),
                ref needsFreeContent);
        }

        ilGenerator.Ret();
        return Tuple.Create(needsFreeContent, method.Create());
    }

    static string GetPersistentName(PropertyInfo p)
    {
        var a = p.GetCustomAttribute<PersistedNameAttribute>();
        return a != null ? a.Name : p.Name;
    }

    internal static byte[] BuildKeyForTableVersions(uint tableId, uint tableVersion)
    {
        Span<byte> buf = stackalloc byte[2 + 5 + 5];
        var writer = new SpanWriter(buf);
        writer.WriteBlock(ObjectDB.TableVersionsPrefix);
        writer.WriteVUInt32(tableId);
        writer.WriteVUInt32(tableVersion);
        return writer.GetSpan().ToArray();
    }

    public byte[]? SingletonContent(long transactionNumber)
    {
        lock (_cachedSingletonLock)
        {
            if (_cachedSingletonTrNum - transactionNumber > 0) return null;
            return _cachedSingletonContent;
        }
    }

    public void CacheSingletonContent(long transactionNumber, byte[]? content)
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
