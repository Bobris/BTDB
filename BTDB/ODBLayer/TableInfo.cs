using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.StreamLayer;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.Collections;
using BTDB.KVDBLayer;
using Extensions = BTDB.FieldHandler.Extensions;

namespace BTDB.ODBLayer
{
    class TableInfo
    {
        readonly ITableInfoResolver _tableInfoResolver;
        uint _clientTypeVersion;
        Type? _clientType;

        readonly ConcurrentDictionary<uint, TableVersionInfo> _tableVersions =
            new ConcurrentDictionary<uint, TableVersionInfo>();

        Func<IInternalObjectDBTransaction, DBObjectMetadata, object>? _creator;
        Action<IInternalObjectDBTransaction, DBObjectMetadata, object>? _initializer;
        Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedWriter, object>? _saver;

        readonly
            ConcurrentDictionary<uint,
                Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>> _loaders =
                new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, DBObjectMetadata,
                    AbstractBufferedReader, object>>();

        readonly
            ConcurrentDictionary<uint, Tuple<NeedsFreeContent, Action<IInternalObjectDBTransaction, DBObjectMetadata,
                AbstractBufferedReader, IList<ulong>>>> _freeContent =
                new ConcurrentDictionary<uint, Tuple<NeedsFreeContent, Action<IInternalObjectDBTransaction,
                    DBObjectMetadata, AbstractBufferedReader, IList<ulong>>>>();

        readonly Dictionary<uint, bool> _freeContentNeedDetectionInProgress = new Dictionary<uint, bool>();
        long _singletonOid;
        long _cachedSingletonTrNum;
        byte[]? _cachedSingletonContent;
        readonly object _cachedSingletonLock = new object();

        internal TableInfo(uint id, string name, ITableInfoResolver tableInfoResolver)
        {
            Id = id;
            Name = name;
            _tableInfoResolver = tableInfoResolver;
            NeedStoreSingletonOid = false;
        }

        internal uint Id { get; }

        internal string Name { get; }

        internal Type? ClientType
        {
            get => _clientType;
            set
            {
                if (_clientType != null && _clientType != value)
                {
                    throw new BTDBException("ClientType could be changed only once " + _clientType.ToSimpleName() +
                                            " vs " + value!.ToSimpleName());
                }

                _clientType = value;
            }
        }

        internal TableVersionInfo? ClientTableVersionInfo
        {
            get
            {
                if (_tableVersions.TryGetValue(_clientTypeVersion, out var tvi)) return tvi;
                return null;
            }
        }

        internal uint LastPersistedVersion { get; set; }

        internal uint ClientTypeVersion
        {
            get => _clientTypeVersion;
            private set => _clientTypeVersion = value;
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
                var method = ILBuilder.Instance.NewMethod(
                    $"Creator_{Name}", typeof(Func<IInternalObjectDBTransaction, DBObjectMetadata, object>), factoryType);
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
                var defaultConstructor = _clientType.GetConstructor(Type.EmptyTypes);
                if (defaultConstructor == null)
                {
                    ilGenerator
                        .Ldtoken(_clientType)
                        .Call(() => Type.GetTypeFromHandle(new RuntimeTypeHandle()))
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

        internal Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedWriter, object> Saver
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
                var newSingletonOid = (long) _tableInfoResolver.AllocateNewOid();
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
                .NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedWriter, object>>(
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
                    .Ldarg(2)
                    .Newobj(() => new DBWriterCtx(null, null))
                    .Stloc(1);
            }

            var props = ClientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            for (var i = 0; i < ClientTableVersionInfo.FieldCount; i++)
            {
                var field = ClientTableVersionInfo[i];
                var getter = props.First(p => GetPersistentName(p) == field.Name).GetAnyGetMethod();
                Action<IILGen> writerOrCtx;
                var handler = field.Handler!.SpecializeSaveForType(getter!.ReturnType);
                if (handler.NeedsCtx())
                    writerOrCtx = il => il.Ldloc(1);
                else
                    writerOrCtx = il => il.Ldarg(2);
                handler.Save(ilGenerator, writerOrCtx, il =>
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
            fields.Reserve((uint) props.Length);
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
                _tableVersions.TryAdd(1, tvi);
                ClientTypeVersion = 1;
            }
            else
            {
                var last = _tableVersions.GetOrAdd(LastPersistedVersion,
                    (ver, tableInfo) =>
                        tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
                if (TableVersionInfo.Equal(last, tvi))
                {
                    _tableVersions[LastPersistedVersion] =
                        tvi; // tvi was build from real types and not loaded so it is more exact
                    ClientTypeVersion = LastPersistedVersion;
                }
                else
                {
                    _tableVersions.TryAdd(LastPersistedVersion + 1, tvi);
                    ClientTypeVersion = LastPersistedVersion + 1;
                }
            }
        }

        void EnsureKnownLastPersistedVersion()
        {
            if (LastPersistedVersion != 0) return;
            LastPersistedVersion = _tableInfoResolver.GetLastPersistedVersion(Id);
        }

        internal Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> GetLoader(
            uint version)
        {
            return _loaders.GetOrAdd(version, CreateLoader);
        }

        Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> CreateLoader(
            uint version)
        {
            EnsureClientTypeVersion();
            var method = ILBuilder.Instance
                .NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>>(
                    $"Loader_{Name}_{version}");
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType!);
            ilGenerator
                .Ldarg(3)
                .Castclass(ClientType)
                .Stloc(0);
            var tableVersionInfo = _tableVersions.GetOrAdd(version,
                (ver, tableInfo) =>
                    tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
            var clientTableVersionInfo = ClientTableVersionInfo;
            var anyNeedsCtx = tableVersionInfo.NeedsCtx() || clientTableVersionInfo!.NeedsCtx();
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(2)
                    .Newobj(() => new DBReaderCtx(null, null))
                    .Stloc(1);
            }

            var props = _clientType!.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            for (int fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                Action<IILGen> readerOrCtx;
                if (srcFieldInfo.Handler!.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(1);
                else
                    readerOrCtx = il => il.Ldarg(2);
                var destFieldInfo = clientTableVersionInfo![srcFieldInfo.Name];
                if (destFieldInfo != null)
                {
                    var fieldInfo = props.First(p => GetPersistentName(p) == destFieldInfo.Name).GetAnySetMethod();
                    var fieldType = fieldInfo!.GetParameters()[0].ParameterType;
                    var specializedSrcHandler =
                        srcFieldInfo.Handler.SpecializeLoadForType(fieldType, destFieldInfo.Handler);
                    var willLoad = specializedSrcHandler.HandledType();
                    var converterGenerator =
                        _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, fieldType);
                    if (converterGenerator != null)
                    {
                        ilGenerator.Ldloc(0);
                        specializedSrcHandler.Load(ilGenerator, readerOrCtx);
                        converterGenerator(ilGenerator);
                        ilGenerator.Call(fieldInfo);
                        continue;
                    }
                }

                srcFieldInfo.Handler.Skip(ilGenerator, readerOrCtx);
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

        internal NeedsFreeContent IsFreeContentNeeded(uint version)
        {
            if (_freeContent.TryGetValue(version, out var freeContent))
                return freeContent.Item1;
            if (_freeContentNeedDetectionInProgress.ContainsKey(version))
                return NeedsFreeContent.No; //when needed then is reported by the other detection in progress
            _freeContentNeedDetectionInProgress[version] = true;
            var result = GetFreeContent(version).Item1;
            _freeContentNeedDetectionInProgress.Remove(version);
            return result;
        }

        internal Tuple<NeedsFreeContent,
                Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, IList<ulong>>>
            GetFreeContent(uint version)
        {
            return _freeContent.GetOrAdd(version, CreateFreeContent);
        }

        Tuple<NeedsFreeContent,
                Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, IList<ulong>>>
            CreateFreeContent(uint version)
        {
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata,
                AbstractBufferedReader, IList<ulong>>>($"FreeContent_{Name}_{version}");
            var ilGenerator = method.Generator;
            var tableVersionInfo = _tableVersions.GetOrAdd(version,
                (ver, tableInfo) =>
                    tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo.Id, ver, tableInfo.Name), this);
            var needsFreeContent = NeedsFreeContent.No;
            var anyNeedsCtx = tableVersionInfo.NeedsCtx();
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(2)
                    .Ldarg(3)
                    .Newobj(() => new DBReaderWithFreeInfoCtx(null, null, null))
                    .Stloc(0);
            }

            for (var fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                Action<IILGen> readerOrCtx;
                if (srcFieldInfo.Handler!.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(0);
                else
                    readerOrCtx = il => il.Ldarg(2);
                Extensions.UpdateNeedsFreeContent(srcFieldInfo.Handler.FreeContent(ilGenerator, readerOrCtx),
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
            var key = new byte[PackUnpack.LengthVUInt(tableId) + PackUnpack.LengthVUInt(tableVersion)];
            var ofs = 0;
            PackUnpack.PackVUInt(key, ref ofs, tableId);
            PackUnpack.PackVUInt(key, ref ofs, tableVersion);
            return key;
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
            return (ulong) Interlocked.Read(ref _singletonOid) == id;
        }
    }
}
