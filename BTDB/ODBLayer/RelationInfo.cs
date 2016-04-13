using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class RelationInfo
    {
        readonly uint _id;
        readonly string _name;
        readonly IRelationInfoResolver _relationInfoResolver;
        readonly Type _interfaceType;
        readonly Type _clientType;
        uint _clientTypeVersion;
        readonly ConcurrentDictionary<uint, RelationVersionInfo> _relationVersions = new ConcurrentDictionary<uint, RelationVersionInfo>();
        Func<IInternalObjectDBTransaction, object> _creator;
        Action<IInternalObjectDBTransaction, object> _initializer;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> _primaryKeysSaver;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> _valueSaver;

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>
            _primaryKeysloaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>
            _valueLoaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>();


        public RelationInfo(uint id, string name, IRelationInfoResolver relationInfoResolver, Type interfaceType, Type clientType, uint lastPersistedVersion)
        {
            _id = id;
            _name = name;
            _relationInfoResolver = relationInfoResolver;
            _interfaceType = interfaceType;
            _clientType = clientType;
            LastPersistedVersion = lastPersistedVersion;
        }

        internal uint Id => _id;

        internal string Name => _name;

        internal Type ClientType => _clientType;

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

        internal Func<IInternalObjectDBTransaction, object> Creator
        {
            get
            {
                if (_creator == null) CreateCreator();
                return _creator;
            }
        }

        void CreateCreator()
        {
            var method = ILBuilder.Instance.NewMethod<Func<IInternalObjectDBTransaction, object>>(
                $"RelationCreator_{Name}");
            var ilGenerator = method.Generator;
            ilGenerator
                .Newobj(_clientType.GetConstructor(Type.EmptyTypes))
                .Ret();
            var creator = method.Create();
            Interlocked.CompareExchange(ref _creator, creator, null);
        }

        internal Action<IInternalObjectDBTransaction, object> Initializer
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
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, object>>(
                $"RelationInitializer_{Name}");
            var ilGenerator = method.Generator;
            if (relationVersionInfo.NeedsInit())
            {
                ilGenerator.DeclareLocal(ClientType);
                ilGenerator
                    .Ldarg(1)
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
                var allFields = relationVersionInfo.GetAllFields();
                foreach (var srcFieldInfo in allFields)
                {
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

        internal Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> ValueSaver
        {
            get
            {
                if (_valueSaver == null)
                {
                    var saver = CreateSaver(ClientRelationVersionInfo.GetValueFields(), $"RelationValueSaver_{Name}");
                    Interlocked.CompareExchange(ref _valueSaver, saver, null);
                }
                return _valueSaver;
            }
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> PrimaryKeysSaver
        {
            get
            {
                if (_primaryKeysSaver == null)
                {
                    var saver = CreateSaver(ClientRelationVersionInfo.GetPrimaryKeyFields(), $"RelationKeySaver_{Name}");
                    Interlocked.CompareExchange(ref _primaryKeysSaver, saver, null);
                }
                return _primaryKeysSaver;
            }
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> CreateSaver(IReadOnlyCollection<TableFieldInfo> fields, string saverName)
        {
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object>>(saverName);
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            ilGenerator
                .Ldarg(2)
                .Castclass(ClientType)
                .Stloc(0);
            var anyNeedsCtx = ClientRelationVersionInfo.NeedsCtx();
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IWriterCtx));
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(1)
                    .Newobj(() => new DBWriterCtx(null, null))
                    .Stloc(1);
            }
            var props = ClientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            foreach (var field in fields)
            {
                var getter = props.First(p => GetPersistantName(p) == field.Name).GetGetMethod(true);
                Action<IILGen> writerOrCtx;
                var handler = field.Handler.SpecializeSaveForType(getter.ReturnType);
                if (handler.NeedsCtx())
                    writerOrCtx = il => il.Ldloc(1);
                else
                    writerOrCtx = il => il.Ldarg(1);
                handler.Save(ilGenerator, writerOrCtx, il =>
                {
                    il.Ldloc(0).Callvirt(getter);
                    _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(getter.ReturnType,
                                                                                    handler.HandledType())(il);
                });
            }
            ilGenerator
                .Ret();
            return method.Create();
        }

        internal void EnsureClientTypeVersion()
        {
            if (ClientTypeVersion != 0) return;
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

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetPrimaryKeysLoader(uint version)
        {
            return _primaryKeysloaders.GetOrAdd(version, ver => CreateLoader(ver, ClientRelationVersionInfo.GetPrimaryKeyFields(), $"RelationKeyLoader_{Name}_{ver}"));
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetValueLoader(uint version)
        {
            return _valueLoaders.GetOrAdd(version, ver => CreateLoader(ver, ClientRelationVersionInfo.GetValueFields(), $"RelationValueLoader_{Name}_{ver}"));
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> CreateLoader(uint version, IReadOnlyCollection<TableFieldInfo> fields, string loaderName)
        {
            EnsureClientTypeVersion();
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>(loaderName);
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            ilGenerator
                .Ldarg(2)
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
                    .Ldarg(1)
                    .Newobj(() => new DBReaderCtx(null, null))
                    .Stloc(1);
            }
            var props = _clientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            foreach (var srcFieldInfo in fields)
            {
                Action<IILGen> readerOrCtx;
                if (srcFieldInfo.Handler.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(1);
                else
                    readerOrCtx = il => il.Ldarg(1);
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
                foreach (var srcFieldInfo in clientRelationVersionInfo.GetValueFields())
                {
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

        public object CreateInstance(IInternalObjectDBTransaction tr, ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            var obj = Creator(tr);
            Initializer(tr, obj);
            var keyReader = new ByteBufferReader(keyBytes);
            keyReader.ReadVUInt32(); //index Relation
            var valueReader = new ByteBufferReader(valueBytes);
            var version = valueReader.ReadVUInt32();
            GetPrimaryKeysLoader(version)(tr, keyReader, obj);
            GetValueLoader(version)(tr, valueReader, obj);
            return obj;
        }
    }

    class RelationEnumerator<T> : IEnumerator<T>
    {
        readonly IInternalObjectDBTransaction _tr;
        readonly RelationInfo _relationInfo;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;

        uint _pos;
        bool _seekNeeded;

        readonly ByteBuffer _keyBytes;

        public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo)
        {
            _relationInfo = relationInfo;
            _tr = tr;

            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = BuildKeyBytes(_relationInfo.Id);
            _pos = 0;
            _seekNeeded = true;
        }

        public bool MoveNext()
        {
            if (!_seekNeeded)
                _pos++;
            return Seek();
        }

        bool Seek()
        {
            if (!_keyValueTr.SetKeyIndex(_pos))
                return false;
            _seekNeeded = false;
            return true;
        }

        public T Current
        {
            get
            {
                _keyValueTrProtector.Start();
                if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                {
                    _keyValueTr.SetKeyPrefix(_keyBytes);
                    Seek();
                }
                else if (_seekNeeded)
                {
                    Seek();
                    _seekNeeded = false;
                }
                _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKey();
                var valueBytes = _keyValueTr.GetValue();
                return (T)_relationInfo.CreateInstance(_tr, keyBytes, valueBytes);
            }
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        ByteBuffer BuildKeyBytes(uint id)
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteVUInt32(id);
            return keyWriter.Data.ToAsyncSafe();
        }

        public void Dispose()
        {
        }
    }

    public class RelationDBManipulator<T>
    {
        readonly RelationInfo _relationInfo;

        public RelationDBManipulator(object relationInfo) //todo better
        {
            _relationInfo = (RelationInfo)relationInfo;
        }

        ByteBuffer ValueBytes(IInternalObjectDBTransaction tr, T obj)
        {
            var valueWriter = new ByteBufferWriter();
            valueWriter.WriteVUInt32(_relationInfo.ClientTypeVersion);
            _relationInfo.ValueSaver(tr, valueWriter, obj);
            var valueBytes = valueWriter.Data; // Data from ByteBufferWriter are always fresh and not reused = AsyncSafe
            return valueBytes;
        }

        ByteBuffer KeyBytes(IInternalObjectDBTransaction tr, T obj)
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteVUInt32(_relationInfo.Id);
            _relationInfo.PrimaryKeysSaver(tr, keyWriter, obj);
            var keyBytes = keyWriter.Data;
            return keyBytes;
        }

        static void StartWorkingWithPK(IInternalObjectDBTransaction tr)
        {
            tr.TransactionProtector.Start();
            tr.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsPKPrefix);
        }

        public void Insert(IInternalObjectDBTransaction tr, T obj)
        {
            var keyBytes = KeyBytes(tr, obj);
            var valueBytes = ValueBytes(tr, obj);

            StartWorkingWithPK(tr);

            if (tr.KeyValueDBTransaction.Find(keyBytes)==FindResult.Exact)
                throw new BTDBException("Trying to insert duplicate key.");  //todo write key in message
            tr.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        public bool Upsert(IInternalObjectDBTransaction tr, T obj)
        {
            var keyBytes = KeyBytes(tr, obj);
            var valueBytes = ValueBytes(tr, obj);

            StartWorkingWithPK(tr);

            return tr.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        public void Update(IInternalObjectDBTransaction tr, T obj)
        {
            var keyBytes = KeyBytes(tr, obj);
            var valueBytes = ValueBytes(tr, obj);

            StartWorkingWithPK(tr);

            if (tr.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
                throw new BTDBException("Not found record to update."); //todo write key in message
            tr.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        public IEnumerator<T> GetEnumerator(IInternalObjectDBTransaction tr)
        {
            tr.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsPKPrefix);
            return new RelationEnumerator<T>(tr, _relationInfo);
        }

    }
}
