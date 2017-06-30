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

namespace BTDB.ODBLayer
{
    class TableInfo
    {
        readonly uint _id;
        readonly string _name;
        readonly ITableInfoResolver _tableInfoResolver;
        uint _clientTypeVersion;
        Type _clientType;
        readonly ConcurrentDictionary<uint, TableVersionInfo> _tableVersions = new ConcurrentDictionary<uint, TableVersionInfo>();
        Func<IInternalObjectDBTransaction, DBObjectMetadata, object> _creator;
        Action<IInternalObjectDBTransaction, DBObjectMetadata, object> _initializer;
        Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedWriter, object> _saver;
        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>> _loaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>>();
        readonly ConcurrentDictionary<uint, Tuple<bool, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, IList<ulong>, IList<ulong>>>> _freeContent = new ConcurrentDictionary<uint, Tuple<bool, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, IList<ulong>, IList<ulong>>>>();
        readonly Dictionary<uint, bool> _freeContentNeedDetectionInProgress = new Dictionary<uint, bool>();
        long _singletonOid;
        long _cachedSingletonTrNum;
        byte[] _cachedSingletonContent;
        readonly object _cachedSingletonLock = new object();

        internal TableInfo(uint id, string name, ITableInfoResolver tableInfoResolver)
        {
            _id = id;
            _name = name;
            _tableInfoResolver = tableInfoResolver;
            NeedStoreSingletonOid = false;
        }

        internal uint Id => _id;

        internal string Name => _name;

        internal Type ClientType
        {
            get { return _clientType; }
            set
            {
                _clientType = value;
                ClientTypeVersion = 0;
            }
        }

        internal TableVersionInfo ClientTableVersionInfo
        {
            get
            {
                TableVersionInfo tvi;
                if (_tableVersions.TryGetValue(_clientTypeVersion, out tvi)) return tvi;
                return null;
            }
        }

        internal uint LastPersistedVersion { get; set; }

        internal uint ClientTypeVersion
        {
            get { return _clientTypeVersion; }
            private set { _clientTypeVersion = value; }
        }

        internal Func<IInternalObjectDBTransaction, DBObjectMetadata, object> Creator
        {
            get
            {
                if (_creator == null) CreateCreator();
                return _creator;
            }
        }

        void CreateCreator()
        {
            var method = ILBuilder.Instance.NewMethod<Func<IInternalObjectDBTransaction, DBObjectMetadata, object>>(
                $"Creator_{Name}");
            var ilGenerator = method.Generator;
            ilGenerator
                .Newobj(_clientType.GetConstructor(Type.EmptyTypes))
                .Ret();
            var creator = method.Create();
            Interlocked.CompareExchange(ref _creator, creator, null);
        }

        internal Action<IInternalObjectDBTransaction, DBObjectMetadata, object> Initializer
        {
            get
            {
                if (_initializer == null) CreateInitializer();
                return _initializer;
            }
        }

        void CreateInitializer()
        {
            EnsureClientTypeVersion();
            var tableVersionInfo = ClientTableVersionInfo;
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, object>>(
                $"Initializer_{Name}");
            var ilGenerator = method.Generator;
            if (tableVersionInfo.NeedsInit())
            {
                ilGenerator.DeclareLocal(ClientType);
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
                var props = _clientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                for (int fi = 0; fi < tableVersionInfo.FieldCount; fi++)
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
                    var setterMethod = props.First(p => GetPersistantName(p) == srcFieldInfo.Name).GetSetMethod(true);
                    var converterGenerator = _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, setterMethod.GetParameters()[0].ParameterType);
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
                return _saver;
            }
        }

        public long LazySingletonOid
        {
            get
            {
                var soid = Interlocked.Read(ref _singletonOid);
                if (soid != 0) return soid;
                Interlocked.CompareExchange(ref _singletonOid, _tableInfoResolver.GetSingletonOid(_id), 0);
                soid = Interlocked.Read(ref _singletonOid);
                return soid;
            }
        }

        public long SingletonOid
        {
            get
            {
                var soid = Interlocked.Read(ref _singletonOid);
                if (soid != 0) return soid;
                soid = Interlocked.CompareExchange(ref _singletonOid, _tableInfoResolver.GetSingletonOid(_id), 0);
                if (soid != 0) return soid;
                soid = Interlocked.Read(ref _singletonOid);
                if (soid != 0) return soid;
                NeedStoreSingletonOid = true;
                var newsoid = (long)_tableInfoResolver.AllocateNewOid();
                soid = Interlocked.CompareExchange(ref _singletonOid, newsoid, 0);
                if (soid == 0) soid = newsoid;
                return soid;
            }
        }

        public bool NeedStoreSingletonOid { get; private set; }

        public void ResetNeedStoreSingletonOid()
        {
            NeedStoreSingletonOid = false;
        }

        void CreateSaver()
        {
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedWriter, object>>(
                $"Saver_{Name}");
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            ilGenerator
                .Ldarg(3)
                .Castclass(ClientType)
                .Stloc(0);
            var anyNeedsCtx = ClientTableVersionInfo.NeedsCtx();
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
            for (int i = 0; i < ClientTableVersionInfo.FieldCount; i++)
            {
                var field = ClientTableVersionInfo[i];
                var getter = props.First(p => GetPersistantName(p) == field.Name).GetGetMethod(true);
                Action<IILGen> writerOrCtx;
                var handler = field.Handler.SpecializeSaveForType(getter.ReturnType);
                if (handler.NeedsCtx())
                    writerOrCtx = il => il.Ldloc(1);
                else
                    writerOrCtx = il => il.Ldarg(2);
                handler.Save(ilGenerator, writerOrCtx, il =>
                    {
                        il.Ldloc(0).Callvirt(getter);
                        _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(getter.ReturnType,
                                                                                     handler.HandledType())(il);
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
            var props = _clientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            var fields = new List<TableFieldInfo>(props.Length);
            foreach (var pi in props)
            {
                if (pi.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) continue;
                if (pi.GetIndexParameters().Length != 0) continue;
                fields.Add(TableFieldInfo.Build(Name, pi, _tableInfoResolver.FieldHandlerFactory, FieldHandlerOptions.None));
            }
            var tvi = new TableVersionInfo(fields.ToArray());
            if (LastPersistedVersion == 0)
            {
                _tableVersions.TryAdd(1, tvi);
                ClientTypeVersion = 1;
            }
            else
            {
                var last = _tableVersions.GetOrAdd(LastPersistedVersion, (ver, tableInfo) => tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo._id, ver, tableInfo.Name), this);
                if (TableVersionInfo.Equal(last, tvi))
                {
                    _tableVersions[LastPersistedVersion] = tvi; // tvi was build from real types and not loaded so it is more exact
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
            LastPersistedVersion = _tableInfoResolver.GetLastPersistedVersion(_id);
        }

        internal Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> GetLoader(uint version)
        {
            return _loaders.GetOrAdd(version, CreateLoader);
        }

        Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> CreateLoader(uint version)
        {
            EnsureClientTypeVersion();
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>>(
                $"Loader_{Name}_{version}");
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            ilGenerator
                .Ldarg(3)
                .Castclass(ClientType)
                .Stloc(0);
            var tableVersionInfo = _tableVersions.GetOrAdd(version, (ver, tableInfo) => tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo._id, ver, tableInfo.Name), this);
            var clientTableVersionInfo = ClientTableVersionInfo;
            var anyNeedsCtx = tableVersionInfo.NeedsCtx() || clientTableVersionInfo.NeedsCtx();
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(2)
                    .Newobj(() => new DBReaderCtx(null, null))
                    .Stloc(1);
            }
            var props = _clientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            for (int fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                Action<IILGen> readerOrCtx;
                if (srcFieldInfo.Handler.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(1);
                else
                    readerOrCtx = il => il.Ldarg(2);
                var destFieldInfo = clientTableVersionInfo[srcFieldInfo.Name];
                if (destFieldInfo != null)
                {

                    var fieldInfo = props.First(p => GetPersistantName(p) == destFieldInfo.Name).GetSetMethod(true);
                    var fieldType = fieldInfo.GetParameters()[0].ParameterType;
                    var specializedSrcHandler = srcFieldInfo.Handler.SpecializeLoadForType(fieldType, destFieldInfo.Handler);
                    var willLoad = specializedSrcHandler.HandledType();
                    var converterGenerator = _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, fieldType);
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
                for (int fi = 0; fi < clientTableVersionInfo.FieldCount; fi++)
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
                    var setterMethod = props.First(p => GetPersistantName(p) == srcFieldInfo.Name).GetSetMethod(true);
                    var converterGenerator = _tableInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, setterMethod.GetParameters()[0].ParameterType);
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

        internal bool IsFreeContentNeeded(uint version)
        {
            Tuple<bool, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, IList<ulong>, IList<ulong>>> freeContent;
            if (_freeContent.TryGetValue(version, out freeContent))
                return freeContent.Item1;
            if (_freeContentNeedDetectionInProgress.ContainsKey(version))
                return false; //when needed then is reported by the other detection in progress
            _freeContentNeedDetectionInProgress[version] = true;
            var result = GetFreeContent(version).Item1;
            _freeContentNeedDetectionInProgress.Remove(version);
            return result;
        }

        internal Tuple<bool, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, IList<ulong>, IList<ulong>>> GetFreeContent(uint version)
        {
            return _freeContent.GetOrAdd(version, CreateFreeContent);
        }

        Tuple<bool, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, IList<ulong>, IList<ulong>>> CreateFreeContent(uint version)
        {
            EnsureClientTypeVersion();
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, 
                AbstractBufferedReader, IList<ulong>, IList<ulong>>>($"FreeContent_{Name}_{version}");
            var ilGenerator = method.Generator;
            var tableVersionInfo = _tableVersions.GetOrAdd(version, (ver, tableInfo) => tableInfo._tableInfoResolver.LoadTableVersionInfo(tableInfo._id, ver, tableInfo.Name), this);
            var needsFreeContent = false;
            var anyNeedsCtx = tableVersionInfo.NeedsCtx();
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(2)
                    .Ldarg(3)
                    .Ldarg(4)
                    .Newobj(() => new DBReaderWithFreeInfoCtx(null, null, null, null))
                    .Stloc(0);
            }
            for (int fi = 0; fi < tableVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = tableVersionInfo[fi];
                Action<IILGen> readerOrCtx;
                if (srcFieldInfo.Handler.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(0);
                else
                    readerOrCtx = il => il.Ldarg(2);
                var needsFree = srcFieldInfo.Handler.FreeContent(ilGenerator, readerOrCtx);
                needsFreeContent |= needsFree;
            }
            ilGenerator.Ret();
            return Tuple.Create(needsFreeContent, method.Create());
        }

        static string GetPersistantName(PropertyInfo p)
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

        public byte[] SingletonContent(long transactionNumber)
        {
            lock (_cachedSingletonLock)
            {
                if (_cachedSingletonTrNum - transactionNumber > 0) return null;
                return _cachedSingletonContent;
            }
        }

        public void CacheSingletonContent(long transactionNumber, byte[] content)
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
}