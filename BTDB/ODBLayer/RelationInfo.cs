using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class RelationInfo
    {
        readonly uint _id;
        readonly string _name;
        readonly IRelationInfoResolver _relationInfoResolver;
        readonly Type _interfaceType;
        readonly Type _clientType;
        readonly IDictionary<uint, RelationVersionInfo> _relationVersions = new Dictionary<uint, RelationVersionInfo>();
        Func<IInternalObjectDBTransaction, object> _creator;
        Action<IInternalObjectDBTransaction, object> _initializer;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> _primaryKeysSaver;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> _valueSaver;

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>
            _primaryKeysLoaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>
            _valueLoaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>>>
            _valueIDictFinders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>>>();

        //SK
        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>  //secondary key idx => sk key saver
            _secondaryKeysSavers = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>  //secondary key idx => sk value saver
            _secondaryKeysValueSavers = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>();

        readonly ConcurrentDictionary<uint, Action<byte[], byte[], AbstractBufferedWriter>>  //secondary key idx => 
            _secondaryKeyValuetoPKLoader = new ConcurrentDictionary<uint, Action<byte[], byte[], AbstractBufferedWriter>>();


        public RelationInfo(uint id, string name, IRelationInfoResolver relationInfoResolver, Type interfaceType, Type clientType, IKeyValueDBTransaction tr)
        {
            _id = id;
            _name = name;
            _relationInfoResolver = relationInfoResolver;
            _interfaceType = interfaceType;
            _clientType = clientType;
            LoadVersionInfos(tr);
            ClientRelationVersionInfo = CreateVersionInfoByReflection();
            ApartFields = FindApartFields(interfaceType, ClientRelationVersionInfo);
            if (LastPersistedVersion > 0 && _relationVersions[LastPersistedVersion].Equals(ClientRelationVersionInfo))
            {
                _relationVersions[LastPersistedVersion] = ClientRelationVersionInfo;
                ClientTypeVersion = LastPersistedVersion;
            }
            else
            {
                // TODO check and do upgrade
                ClientTypeVersion = LastPersistedVersion + 1;
                _relationVersions.Add(ClientTypeVersion, ClientRelationVersionInfo);
                var writerk = new ByteBufferWriter();
                writerk.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
                writerk.WriteVUInt32(_id);
                writerk.WriteVUInt32(ClientTypeVersion);
                var writerv = new ByteBufferWriter();
                ClientRelationVersionInfo.Save(writerv);
                tr.SetKeyPrefix(ByteBuffer.NewEmpty());
                tr.CreateOrUpdateKeyValue(writerk.Data, writerv.Data);
                NeedsFreeContent = ClientRelationVersionInfo.NeedsFreeContent();
            }
        }

        void LoadVersionInfos(IKeyValueDBTransaction tr)
        {
            LastPersistedVersion = 0;
            var writer = new ByteBufferWriter();
            writer.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
            writer.WriteVUInt32(_id);
            tr.SetKeyPrefix(writer.Data);
            if (!tr.FindFirstKey()) return;
            var keyReader = new KeyValueDBKeyReader(tr);
            var valueReader = new KeyValueDBValueReader(tr);
            do
            {
                keyReader.Restart();
                valueReader.Restart();
                LastPersistedVersion = keyReader.ReadVUInt32();
                var relationVersionInfo = RelationVersionInfo.Load(valueReader,
                    _relationInfoResolver.FieldHandlerFactory, _name);
                _relationVersions[LastPersistedVersion] = relationVersionInfo;
                if (relationVersionInfo.NeedsFreeContent())
                    NeedsFreeContent = true;
            } while (tr.FindNextKey());
        }

        internal uint Id => _id;

        internal string Name => _name;

        internal Type ClientType => _clientType;

        internal RelationVersionInfo ClientRelationVersionInfo { get; }

        internal uint LastPersistedVersion { get; set; }

        internal uint ClientTypeVersion { get; }

        internal Func<IInternalObjectDBTransaction, object> Creator
        {
            get
            {
                if (_creator == null) CreateCreator();
                return _creator;
            }
        }

        internal bool NeedsFreeContent { get; set; }

        internal IDictionary<string, MethodInfo> ApartFields { get; }

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

        internal Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> PrimaryKeysSaver
        {
            get
            {
                if (_primaryKeysSaver == null)
                {
                    var saver = CreateSaverWithApartFields(ClientRelationVersionInfo.GetPrimaryKeyFields(), $"RelationKeySaver_{Name}");
                    Interlocked.CompareExchange(ref _primaryKeysSaver, saver, null);
                }
                return _primaryKeysSaver;
            }
        }

        void CreateSaverIl(IILGen ilGen, IReadOnlyCollection<TableFieldInfo> fields,
                           Action<IILGen> pushInstance, Action<IILGen> pushRelationIface,
                           Action<IILGen> pushWriter, Action<IILGen> pushTransaction)
        {
            var anyNeedsCtx = fields.Any(tfi => tfi.Handler.NeedsCtx());
            IILLocal writerCtxLocal = null;
            if (anyNeedsCtx)
            {
                writerCtxLocal = ilGen.DeclareLocal(typeof(IWriterCtx));
                ilGen
                    .Do(pushTransaction)
                    .Do(pushWriter)
                    .LdcI4(1)
                    .Newobj(() => new DBWriterCtx(null, null, true))
                    .Stloc(writerCtxLocal);
            }
            var props = ClientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            foreach (var field in fields)
            {
                var getter = props.First(p => GetPersistantName(p) == field.Name).GetGetMethod(true);
                Action<IILGen> writerOrCtx;
                var handler = field.Handler.SpecializeSaveForType(getter.ReturnType);
                if (handler.NeedsCtx())
                    writerOrCtx = il => il.Ldloc(writerCtxLocal);
                else
                    writerOrCtx = pushWriter;
                MethodInfo apartFieldGetter = null;
                if (pushRelationIface != null)
                    ApartFields.TryGetValue(field.Name, out apartFieldGetter);
                handler.Save(ilGen, writerOrCtx, il =>
                {
                    if (apartFieldGetter != null)
                    {
                        il.Do(pushRelationIface);
                        getter = apartFieldGetter;
                    }
                    else
                    {
                        il.Do(pushInstance);
                    }
                    il.Callvirt(getter);
                    _relationInfoResolver.TypeConvertorGenerator.GenerateConversion(getter.ReturnType,
                                                                                    handler.HandledType())(il);
                });
            }
        }

        void StoreNthArgumentOfTypeIntoLoc(IILGen il, ushort argIdx, Type type, ushort locIdx)
        {
            il
               .Ldarg(argIdx)
               .Castclass(type)
               .Stloc(locIdx);
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> CreateSaverWithApartFields(
            IReadOnlyCollection<TableFieldInfo> fields, string saverName)
        {
            var method = ILBuilder.Instance.NewMethod<
                Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>(saverName);
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            StoreNthArgumentOfTypeIntoLoc(ilGenerator, 2, ClientType, 0);
            var hasApartFields = ApartFields.Any();
            if (hasApartFields)
            {
                ilGenerator.DeclareLocal(_interfaceType);
                StoreNthArgumentOfTypeIntoLoc(ilGenerator, 3, _interfaceType, 1);
            }
            CreateSaverIl(ilGenerator, fields,
                          il => il.Ldloc(0), hasApartFields ? il => il.Ldloc(1) : (Action<IILGen>)null,
                          il => il.Ldarg(1), il => il.Ldarg(0));
            ilGenerator
                .Ret();
            return method.Create();
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> CreateSaver(
            IReadOnlyCollection<TableFieldInfo> fields, string saverName)
        {
            var method =
                ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object>>(
                    saverName);
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            StoreNthArgumentOfTypeIntoLoc(ilGenerator, 2, ClientType, 0);
            CreateSaverIl(ilGenerator, fields,
                          il => il.Ldloc(0), null, il => il.Ldarg(1), il => il.Ldarg(0));
            ilGenerator
                .Ret();
            return method.Create();
        }

        RelationVersionInfo CreateVersionInfoByReflection()
        {
            var props = _clientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            var primaryKeys = new Dictionary<uint, TableFieldInfo>(1); //PK order->fieldInfo
            var secondaryKeys = new Dictionary<uint, IList<SecondaryKeyAttribute>>(); //value field index -> list of attributes

            var fields = new List<TableFieldInfo>(props.Length);
            foreach (var pi in props)
            {
                if (pi.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) continue;
                if (pi.GetIndexParameters().Length != 0) continue;
                var pks = pi.GetCustomAttributes(typeof(PrimaryKeyAttribute), true);
                if (pks.Length != 0)
                {
                    var pkinfo = (PrimaryKeyAttribute)pks[0];
                    var fieldInfo = TableFieldInfo.Build(Name, pi, _relationInfoResolver.FieldHandlerFactory);
                    if (fieldInfo.Handler.NeedsCtx())
                        throw new BTDBException($"Unsupported key field {fieldInfo.Name} type.");
                    primaryKeys.Add(pkinfo.Order, fieldInfo);
                    continue;
                }
                var sks = pi.GetCustomAttributes(typeof(SecondaryKeyAttribute), true);
                for (var i = 0; i < sks.Length; i++)
                {
                    var attribute = (SecondaryKeyAttribute)sks[i];
                    IList<SecondaryKeyAttribute> skAttrList;
                    if (!secondaryKeys.TryGetValue((uint)fields.Count, out skAttrList))
                    {
                        skAttrList = new List<SecondaryKeyAttribute>();
                        secondaryKeys[(uint)fields.Count] = skAttrList;
                    }
                    skAttrList.Add(attribute);
                }
                fields.Add(TableFieldInfo.Build(Name, pi, _relationInfoResolver.FieldHandlerFactory));
            }
            var firstSecondaryKeyIndex = 0u; //todo loading/upgrading/...
            return new RelationVersionInfo(primaryKeys, secondaryKeys, fields.ToArray(), firstSecondaryKeyIndex);
        }

        static IDictionary<string, MethodInfo> FindApartFields(Type interfaceType, RelationVersionInfo versionInfo)
        {
            var result = new Dictionary<string, MethodInfo>();
            var pks = versionInfo.GetPrimaryKeyFields().ToDictionary(tfi => tfi.Name, tfi => tfi);
            var methods = interfaceType.GetMethods();
            for (var i = 0; i < methods.Length; i++)
            {
                var method = methods[i];
                if (!method.Name.StartsWith("get_"))
                    continue;
                var name = method.Name.Substring(4);
                TableFieldInfo tfi;
                if (!pks.TryGetValue(name, out tfi))
                    throw new BTDBException($"Property {name} is not part of primary key.");
                if (method.ReturnType != tfi.Handler.HandledType())
                    throw new BTDBException($"Property {name} has different return type then member of primary key with the same name.");
                result.Add(name, method);
            }
            return result;
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetPrimaryKeysLoader(uint version)
        {
            return _primaryKeysLoaders.GetOrAdd(version, ver => CreateLoader(ver, _relationVersions[version].GetPrimaryKeyFields(), $"RelationKeyLoader_{Name}_{ver}"));
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetValueLoader(uint version)
        {
            return _valueLoaders.GetOrAdd(version, ver => CreateLoader(ver, _relationVersions[version].GetValueFields(), $"RelationValueLoader_{Name}_{ver}"));
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>> GetIDictFinder(uint version)
        {
            return _valueIDictFinders.GetOrAdd(version, ver => CreateIDictFinder(version));
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> GetSecondaryKeysKeySaver
            (uint secondaryKeyIndex, string name)
        {
            return _secondaryKeysSavers.GetOrAdd(secondaryKeyIndex,
                idx => CreateSaverWithApartFields(ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex),
                    $"Relation_{Name}_SK_{name}_KeySaver"));
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> GetSecondaryKeysValueSaver
            (uint secondaryKeyIndex, string name)
        {
            return _secondaryKeysValueSavers.GetOrAdd(secondaryKeyIndex,
                idx => CreateSaverWithApartFields(ClientRelationVersionInfo.GetSecondaryKeyValueKeys(secondaryKeyIndex),
                    $"Relation_{Name}_SK_{name}_ValueSaver"));
        }

        //takes secondaryKey key & value bytes and restores primary key bytes
        public Action<byte[], byte[], AbstractBufferedWriter> GetSKKeyValuetoPKMerger
            (uint secondaryKeyIndex)
        {
            return _secondaryKeyValuetoPKLoader.GetOrAdd(secondaryKeyIndex,
                idx => CreatePrimaryKeyFromSKDataMerger(secondaryKeyIndex,
                        $"Relation_SK_to_PK_{ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Name}"));
        }

        Action<byte[], byte[], AbstractBufferedWriter> CreatePrimaryKeyFromSKDataMerger(uint secondaryKeyIndex, string mergerName)
        {
            var method = ILBuilder.Instance.NewMethod<Action<byte[], byte[], AbstractBufferedWriter>>(mergerName);
            var ilGenerator = method.Generator;
            
            Action<IILGen> pushWriter = il => il.Ldarg(2);
            var skFields = ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Fields;
            if (!skFields.Any(f => f.IsFromPrimaryKey))
            {   //copy whole SK value into writer
                ilGenerator
                    .Do(pushWriter)
                    .Ldarg(1) //ByteBuffer SK value
                    .Call(() => default(AbstractBufferedWriter).WriteByteArrayRaw(null));
            }
            else
            {
                var krLoc = ilGenerator.DeclareLocal(typeof (ByteArrayReader)); //SK key reader
                var vrLoc = ilGenerator.DeclareLocal(typeof (ByteArrayReader)); //SK value reader
                var positionLoc = ilGenerator.DeclareLocal(typeof (ulong)); //stored position
                var memoPositionLoc = ilGenerator.DeclareLocal(typeof (IMemorizedPosition));
                var resetmemoPositionLoc = ilGenerator.DeclareLocal(typeof (IMemorizedPosition)); //for complete SKKeyReader reset

                ilGenerator
                   .Ldarg(0)
                   .Newobj(() => new ByteArrayReader(null))
                   .Stloc(krLoc);
                Action<IILGen> pushReaderSKKey = il => il.Ldloc(krLoc);

                ilGenerator
                   .Ldarg(1)
                   .Newobj(() => new ByteArrayReader(null))
                   .Stloc(vrLoc);
                Action<IILGen> pushReaderSKValue = il => il.Ldloc(vrLoc);

                var pkFieldsFromskKey = new Dictionary<uint, uint>(); //pk idx -> index in secondarykey key
                var skIdx = 0u;
                foreach (var field in skFields)
                {
                    if (field.IsFromPrimaryKey)
                    {
                        if (skIdx == 0)
                            throw new BTDBException("Secondary index should not start with primary key.");
                        pkFieldsFromskKey.Add(field.Index, skIdx);
                    }
                    skIdx++;
                }
                var fieldIdxSKKey = 1; //secondary index is inside key prefix (not in KeyReader)
                ilGenerator
                    .Ldloc(krLoc).Call(() => default(ByteArrayReader).MemorizeCurrentPosition())
                    .Stloc(resetmemoPositionLoc);

                var sks = ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex).ToList();
                var pks = ClientRelationVersionInfo.GetPrimaryKeyFields();
                var pkIdx = 0u;

                foreach (var pk in pks)
                {
                    if (!pkFieldsFromskKey.TryGetValue(pkIdx, out skIdx))
                    {   //copy PK field from secondary key value
                        GenerateCopyFieldFromByteBufferToWriterIl(ilGenerator, pk.Handler, pushReaderSKValue,
                                                                  pushWriter, positionLoc, memoPositionLoc);
                    }
                    else
                    {
                        if (fieldIdxSKKey > skIdx)
                        {   //start reading from beginning
                            ilGenerator
                                .Ldloc(resetmemoPositionLoc)
                                .Call(() => default(IMemorizedPosition).Restore());
                            fieldIdxSKKey = 1;
                        }
                        for (; fieldIdxSKKey < skIdx; fieldIdxSKKey++)
                            sks[fieldIdxSKKey].Handler.Skip(ilGenerator, pushReaderSKKey);

                        GenerateCopyFieldFromByteBufferToWriterIl(ilGenerator, sks[(int)skIdx].Handler, pushReaderSKKey,
                                                                  pushWriter, positionLoc, memoPositionLoc);
                    }
                    pkIdx++;
                }
            }

            ilGenerator.Ret();
            return method.Create();
        }

        void GenerateCopyFieldFromByteBufferToWriterIl(IILGen ilGenerator, IFieldHandler handler, Action<IILGen> pushReader,
                     Action<IILGen> pushWriter, IILLocal positionLoc, IILLocal memoPositionLoc )
        {
            ilGenerator
                .Do(pushReader)
                .Call(() => default(ByteArrayReader).MemorizeCurrentPosition())
                .Stloc(memoPositionLoc)

                .Do(pushReader)
                .Callvirt(() => default(ByteArrayReader).GetCurrentPosition())
                .Stloc(positionLoc);

            handler.Skip(ilGenerator, pushReader);

            ilGenerator
                .Do(pushWriter) //[W]
                .Do(pushReader) //[W,VR]
                .Dup() //[W, VR, VR]
                .Callvirt(() => default(ByteArrayReader).GetCurrentPosition()) //[W, VR, posNew];
                .Ldloc(positionLoc) //[W, VR, posNew, posOld]
                .Sub() //[W, VR, readLen]
                .ConvI4() //[W, VR, readLen(i)]
                .Ldloc(memoPositionLoc) //[W, VR, readLen, Memorize]
                .Callvirt(() => default(IMemorizedPosition).Restore()) //[W, VR, readLen]
                .Call(() => default(ByteArrayReader).ReadByteArrayRaw(0)) //[W, byte[]]
                .Call(() => default(AbstractBufferedWriter).WriteByteArrayRaw(null)); //[]
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> CreateLoader(uint version, IReadOnlyCollection<TableFieldInfo> fields, string loaderName)
        {
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>(loaderName);
            var ilGenerator = method.Generator;
            ilGenerator.DeclareLocal(ClientType);
            ilGenerator
                .Ldarg(2)
                .Castclass(ClientType)
                .Stloc(0);
            var relationVersionInfo = _relationVersions[version];
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

        public object CreateInstance(IInternalObjectDBTransaction tr, ByteBuffer keyBytes, ByteBuffer valueBytes,
            bool keyContainsRelationIndex = true)
        {
            var obj = Creator(tr);
            Initializer(tr, obj);
            var keyReader = new ByteBufferReader(keyBytes);
            if (keyContainsRelationIndex)
                keyReader.SkipVUInt32(); //index Relation
            GetPrimaryKeysLoader(ClientTypeVersion)(tr, keyReader, obj);
            var valueReader = new ByteBufferReader(valueBytes);
            var version = valueReader.ReadVUInt32();
            GetValueLoader(version)(tr, valueReader, obj);
            return obj;
        }

        public void FreeContent(IInternalObjectDBTransaction tr, ByteBuffer valueBytes)
        {
            var valueReader = new ByteBufferReader(valueBytes);
            var version = valueReader.ReadVUInt32();

            var dictionaries = new List<ulong>();
            GetIDictFinder(version)(tr, valueReader, dictionaries);

            //delete dictionaries
            foreach (var dictId in dictionaries)
            {
                var o = ObjectDB.AllDictionariesPrefix.Length;
                var prefix = new byte[o + PackUnpack.LengthVUInt(dictId)];
                Array.Copy(ObjectDB.AllDictionariesPrefix, prefix, o);
                PackUnpack.PackVUInt(prefix, ref o, dictId);

                tr.KeyValueDBTransaction.SetKeyPrefixUnsafe(prefix);
                tr.KeyValueDBTransaction.EraseAll();
            }
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>> CreateIDictFinder(uint version)
        {
            var method = ILBuilder.Instance
                .NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>>>(
                    $"Relation{Name}_IDictFinder");
            var ilGenerator = method.Generator;

            var relationVersionInfo = _relationVersions[version];
            var anyNeedsCtx = relationVersionInfo.GetValueFields()
                    .Any(f => f.Handler.NeedsCtx() && !(f.Handler is ODBDictionaryFieldHandler));
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx)); //loc 0
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(1)
                    .Newobj(() => new DBReaderCtx(null, null))
                    .Stloc(0);
            }
            foreach (var srcFieldInfo in relationVersionInfo.GetValueFields())
            {
                if (srcFieldInfo.Handler is ODBDictionaryFieldHandler)
                {
                    //currently not supported freeing IDict inside IDict
                    ilGenerator
                        .Ldarg(2) //IList<ulong>
                        .Ldarg(1) //reader
                        .Callvirt(() => default(AbstractBufferedReader).ReadVUInt64()) //read dictionary id
                        .Callvirt(() => default(IList<ulong>).Add(0ul)); //store dict id into list
                }
                else
                {
                    Action<IILGen> readerOrCtx;
                    if (srcFieldInfo.Handler.NeedsCtx())
                        readerOrCtx = il => il.Ldloc(0);
                    else
                        readerOrCtx = il => il.Ldarg(1);
                    srcFieldInfo.Handler.Skip(ilGenerator, readerOrCtx);
                    //todo optimize, do not generate skips when all dicts loaded
                }
            }
            ilGenerator.Ret();
            return method.Create();
        }

        void SaveKeyFieldFromArgument(IILGen ilGenerator, TableFieldInfo field, ushort parameterId, IILLocal writerLoc)
        {
            field.Handler.Save(ilGenerator,
                il => il.Ldloc(writerLoc),
                il => il.Ldarg(parameterId));
        }

        void SaveKeyFieldFromField(IILGen ilGenerator, TableFieldInfo field, FieldBuilder backingField, IILLocal writerLoc)
        {
            field.Handler.Save(ilGenerator,
                il => il.Ldloc(writerLoc),
                il => il.Ldarg(0).Ldfld(backingField));
        }

        void WriteIdIl(IILGen ilGenerator, Action<IILGen> pushWriter, int id)
        {
            ilGenerator
                .Do(pushWriter)
                .LdcI4(id)
                .Call(() => default(AbstractBufferedWriter).WriteVUInt32(0));
        }

        void WriteShortPrefixIl(IILGen ilGenerator, Action<IILGen> pushWriter, byte[] prefix)
        {
            foreach (byte b in prefix)
                ilGenerator
                    .Do(pushWriter)
                    .LdcI4(b)
                    .Call(() => default(AbstractBufferedWriter).WriteUInt8(0));
        }

        public void SaveKeyBytesAndCallMethod(IILGen ilGenerator, Type relationDBManipulatorType, string methodName,
            ParameterInfo[] methodParameters, Type methodReturnType,
            IDictionary<string, FieldBuilder> keyFieldProperties)
        {
            //arg0 = this = manipulator
            if (methodName.StartsWith("RemoveById") || methodName.StartsWith("FindById"))
            {
                var writerLoc = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
                ilGenerator.Newobj(() => new ByteBufferWriter());
                ilGenerator.Stloc(writerLoc);
                //ByteBufferWriter.WriteVUInt32(RelationInfo.Id);
                WriteIdIl(ilGenerator, il => il.Ldloc(writerLoc), (int)Id);
                var primaryKeyFields = ClientRelationVersionInfo.GetPrimaryKeyFields();

                ushort idx = 0;
                foreach (var field in primaryKeyFields)
                {
                    FieldBuilder backingField;
                    if (keyFieldProperties.TryGetValue(field.Name, out backingField))
                    {
                        SaveKeyFieldFromField(ilGenerator, field, backingField, writerLoc);
                        continue;
                    }
                    if (idx == methodParameters.Length)
                        throw new BTDBException($"Number of parameters in {methodName} does not match primary key count {primaryKeyFields.Count}.");
                    var par = methodParameters[idx++];
                    if (string.Compare(field.Name, par.Name.ToLower(), StringComparison.OrdinalIgnoreCase) != 0)
                        throw new BTDBException($"Parameter and primary keys mismatch in {methodName}, {field.Name}!={par.Name}.");
                    SaveKeyFieldFromArgument(ilGenerator, field, idx, writerLoc);
                }
                if (idx != methodParameters.Length)
                    throw new BTDBException($"Number of parameters in {methodName} does not match primary key count {primaryKeyFields.Count}.");

                //call manipulator.RemoveById/FindById(tr, byteBuffer)
                ilGenerator
                    .Ldarg(0); //manipulator
                //call byteBuffer.data
                var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
                ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
                ilGenerator.LdcI4(ShouldThrowWhenKeyNotFound(methodName, methodReturnType) ? 1 : 0);
                ilGenerator.Callvirt(relationDBManipulatorType.GetMethod(WhichMethodToCall(methodName)));
                if (methodReturnType == typeof(void))
                    ilGenerator.Pop();
            }
            else if (methodName.StartsWith("FindBy"))
            {
                var writerLoc = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
                ilGenerator.Newobj(() => new ByteBufferWriter());
                ilGenerator.Stloc(writerLoc);

                bool allowDefault = false;
                var skName = methodName.Substring(6);
                if (skName.EndsWith("OrDefault"))
                {
                    skName = skName.Substring(0, skName.Length - 9);
                    allowDefault = true;
                }
                Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);

                WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsSKPrefix);
                var skIndex = ClientRelationVersionInfo.GetSecondaryKeyIndex(skName);
                //ByteBuffered.WriteVUInt32(RelationInfo.Id);
                WriteIdIl(ilGenerator, pushWriter, (int)Id);
                //ByteBuffered.WriteVUInt32(skIndex);
                WriteIdIl(ilGenerator, pushWriter, (int)skIndex);

                if (methodParameters.Length != 1)
                    throw new BTDBException($"Expected one parameter in {methodName}.");
                var firstField = ClientRelationVersionInfo.GetSecondaryKeyFields(skIndex).First();

                SaveKeyFieldFromArgument(ilGenerator, firstField, 1, writerLoc);
                //call public T FindBySecondaryKeyOrDefault(uint secondaryKeyIndex, ByteBuffer secKeyBytes, bool throwWhenNotFound)
                ilGenerator.Ldarg(0); //manipulator
                //call byteBuffer.data
                var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
                ilGenerator.LdcI4((int)skIndex);
                ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
                ilGenerator.LdcI4(allowDefault ? 0 : 1); //? should throw
                ilGenerator.Callvirt(relationDBManipulatorType.GetMethod("FindBySecondaryKeyOrDefault"));
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        string WhichMethodToCall(string methodName)
        {
            if (methodName.StartsWith("FindById"))
                return "FindByIdOrDefault";
            return methodName;
        }

        bool ShouldThrowWhenKeyNotFound(string methodName, Type methodReturnType)
        {
            if (methodName.StartsWith("RemoveBy"))
                return methodReturnType == typeof(void);
            if (methodName.StartsWith("FindByIdOrDefault"))
                return false;
            return true;
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
            _keyValueTr.SetKeyPrefix(_keyBytes);
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
                return (T)_relationInfo.CreateInstance(_tr, keyBytes, valueBytes, false);
            }
        }

        object IEnumerator.Current => Current;

        public void Reset()
        {
            throw new NotSupportedException();
        }

        ByteBuffer BuildKeyBytes(uint id)
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteByteArrayRaw(ObjectDB.AllRelationsPKPrefix);
            keyWriter.WriteVUInt32(id);
            return keyWriter.Data.ToAsyncSafe();
        }

        public void Dispose()
        {
        }
    }
}
