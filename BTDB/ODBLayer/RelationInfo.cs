using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class RelationInfo
    {
        uint _id;
        string _name;
        IRelationInfoResolver _relationInfoResolver;
        uint _clientTypeVersion;
        Type _clientType;
        readonly ConcurrentDictionary<uint, RelationVersionInfo> _relationVersions = new ConcurrentDictionary<uint, RelationVersionInfo>();
        Func<IInternalObjectDBTransaction, DBObjectMetadata, object> _creator;
        Action<IInternalObjectDBTransaction, DBObjectMetadata, object> _initializer;
        Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedWriter, object> _saver;
        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>> _loaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>>();

        public RelationInfo(uint id, string name, IRelationInfoResolver relationInfoResolver)
        {
            _id = id;
            _name = name;
            _relationInfoResolver = relationInfoResolver;
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

        internal RelationVersionInfo ClientRelationVersionInfo
        {
            get
            {
                RelationVersionInfo tvi;
                if (_relationVersions.TryGetValue(_clientTypeVersion, out tvi)) return tvi;
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
                $"RelCreator_{Name}");
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
            var relationVersionInfo = ClientRelationVersionInfo;
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, object>>(
                $"RelInitializer_{Name}");
            var ilGenerator = method.Generator;
            if (relationVersionInfo.NeedsInit())
            {
                ilGenerator.DeclareLocal(ClientType);
                ilGenerator
                    .Ldarg(2)
                    .Castclass(ClientType)
                    .Stloc(0);
                var anyNeedsCtx = relationVersionInfo.NeedsCtx();
                if (anyNeedsCtx)
                {
                    ilGenerator.DeclareLocal(typeof(IReaderCtx));
                    ilGenerator
                        .Ldarg(0)
                        .Newobj(() => new DBReaderCtx(null))
                        .Stloc(1);
                }
                var props = _clientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                for (int fi = 0; fi < relationVersionInfo.FieldCount; fi++)
                {
                    var srcFieldInfo = relationVersionInfo[fi];
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
                    var converterGenerator = _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, setterMethod.GetParameters()[0].ParameterType);
                    if (converterGenerator == null) continue;
                    converterGenerator = _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, setterMethod.GetParameters()[0].ParameterType);
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

        void CreateSaver()
        {
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedWriter, object>>(
                $"RelSaver_{Name}");
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            ilGenerator
                .Ldarg(3)
                .Castclass(ClientType)
                .Stloc(0);
            var anyNeedsCtx = ClientRelationVersionInfo.NeedsCtx();
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
            for (int i = 0; i < ClientRelationVersionInfo.FieldCount; i++)
            {
                var field = ClientRelationVersionInfo[i];
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
                    _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(getter.ReturnType,
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
            var primaryKeys = new Dictionary<uint, TableFieldInfo>(1); //PK order->fieldInfo
            var secondaryKeysInfo = new Dictionary<uint, SecondaryKeyAttribute>(); //field idx->attribute info
            var fields = new List<TableFieldInfo>(props.Length);
            foreach (var pi in props)
            {
                if (pi.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) continue;
                if (pi.GetIndexParameters().Length != 0) continue;
                var pks = pi.GetCustomAttributes(typeof(PrimaryKeyAttribute), true);
                if (pks.Length != 0)
                {
                    var pkinfo = (PrimaryKeyAttribute)pks[0];
                    primaryKeys.Add(pkinfo.Order, TableFieldInfo.Build(Name, pi, _relationInfoResolver.FieldHandlerFactory));
                    continue;
                }
                var sks = pi.GetCustomAttributes(typeof(SecondaryKeyAttribute), true);
                if (sks.Length != 0)
                {
                    var skinfo = (SecondaryKeyAttribute)sks[0];
                    secondaryKeysInfo.Add((uint)fields.Count, skinfo);
                }
                fields.Add(TableFieldInfo.Build(Name, pi, _relationInfoResolver.FieldHandlerFactory));
            }
            var rvi = new RelationVersionInfo(primaryKeys, secondaryKeysInfo, fields.ToArray());
            if (LastPersistedVersion == 0)
            {
                _relationVersions.TryAdd(1, rvi);
                ClientTypeVersion = 1;
            }
            else
            {
                var last = _relationVersions.GetOrAdd(LastPersistedVersion, v => _relationInfoResolver.LoadRelationVersionInfo(_id, v, Name));
                if (RelationVersionInfo.Equal(last, rvi))
                {
                    _relationVersions[LastPersistedVersion] = rvi; // tvi was build from real types and not loaded so it is more exact
                    ClientTypeVersion = LastPersistedVersion;
                }
                else
                {
                    _relationVersions.TryAdd(LastPersistedVersion + 1, rvi);
                    ClientTypeVersion = LastPersistedVersion + 1;
                }
            }
        }

        void EnsureKnownLastPersistedVersion()
        {
            if (LastPersistedVersion != 0) return;
            LastPersistedVersion = _relationInfoResolver.GetLastPersistedVersion(_id);
        }

        internal Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> GetLoader(uint version)
        {
            return _loaders.GetOrAdd(version, CreateLoader);
        }

        Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object> CreateLoader(uint version)
        {
            EnsureClientTypeVersion();
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, DBObjectMetadata, AbstractBufferedReader, object>>(
                $"RelLoader_{Name}_{version}");
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            ilGenerator
                .Ldarg(3)
                .Castclass(ClientType)
                .Stloc(0);
            var relationVersionInfo = _relationVersions.GetOrAdd(version, version1 => _relationInfoResolver.LoadRelationVersionInfo(_id, version1, Name));
            var clientRelationVersionInfo = ClientRelationVersionInfo;
            var anyNeedsCtx = relationVersionInfo.NeedsCtx() || clientRelationVersionInfo.NeedsCtx();
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
            for (int fi = 0; fi < relationVersionInfo.FieldCount; fi++)
            {
                var srcFieldInfo = relationVersionInfo[fi];
                Action<IILGen> readerOrCtx;
                if (srcFieldInfo.Handler.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(1);
                else
                    readerOrCtx = il => il.Ldarg(2);
                var destFieldInfo = clientRelationVersionInfo[srcFieldInfo.Name];
                if (destFieldInfo != null)
                {

                    var fieldInfo = props.First(p => GetPersistantName(p) == destFieldInfo.Name).GetSetMethod(true);
                    var fieldType = fieldInfo.GetParameters()[0].ParameterType;
                    var specializedSrcHandler = srcFieldInfo.Handler.SpecializeLoadForType(fieldType, destFieldInfo.Handler);
                    var willLoad = specializedSrcHandler.HandledType();
                    var converterGenerator = _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, fieldType);
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
                for (int fi = 0; fi < clientRelationVersionInfo.FieldCount; fi++)
                {
                    var srcFieldInfo = clientRelationVersionInfo[fi];
                    var iFieldHandlerWithInit = srcFieldInfo.Handler as IFieldHandlerWithInit;
                    if (iFieldHandlerWithInit == null) continue;
                    if (relationVersionInfo[srcFieldInfo.Name] != null) continue;
                    Action<IILGen> readerOrCtx;
                    if (srcFieldInfo.Handler.NeedsCtx())
                        readerOrCtx = il => il.Ldloc(1);
                    else
                        readerOrCtx = il => il.Ldnull();
                    var specializedSrcHandler = srcFieldInfo.Handler;
                    var willLoad = specializedSrcHandler.HandledType();
                    var setterMethod = props.First(p => GetPersistantName(p) == srcFieldInfo.Name).GetSetMethod(true);
                    var converterGenerator = _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(willLoad, setterMethod.GetParameters()[0].ParameterType);
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

        static string GetPersistantName(PropertyInfo p)
        {
            var a = p.GetCustomAttribute<PersistedNameAttribute>();
            return a != null ? a.Name : p.Name;
        }

    }
}
