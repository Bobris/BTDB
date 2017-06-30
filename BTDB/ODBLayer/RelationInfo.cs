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
    public class RelationInfo
    {
        readonly uint _id;
        readonly string _name;
        readonly IRelationInfoResolver _relationInfoResolver;
        readonly Type _interfaceType;
        readonly Type _clientType;
        readonly ITypeConvertorGenerator _typeConvertorGenerator;

        readonly IDictionary<uint, RelationVersionInfo> _relationVersions = new Dictionary<uint, RelationVersionInfo>();
        Func<IInternalObjectDBTransaction, object> _creator;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> _primaryKeysSaver;
        Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> _primaryKeysLoader;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> _valueSaver;

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>
            _valueLoaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>>>
            _valueIDictFinders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>>>();

        //SK
        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>  //secondary key idx => sk key saver
            _secondaryKeysSavers = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>();

        readonly ConcurrentDictionary<ulong, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, byte[], byte[]>>
            _secondaryKeysConvertSavers = new ConcurrentDictionary<ulong, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, byte[], byte[]>>();

        readonly ConcurrentDictionary<ulong, Action<byte[], byte[], AbstractBufferedWriter>>
            _secondaryKeyValuetoPKLoader = new ConcurrentDictionary<ulong, Action<byte[], byte[], AbstractBufferedWriter>>();

        public struct SimpleLoaderType : IEquatable<SimpleLoaderType>
        {
            readonly IFieldHandler _fieldHandler;
            readonly Type _realType;

            public IFieldHandler FieldHandler => _fieldHandler;
            public Type RealType => _realType;

            public SimpleLoaderType(IFieldHandler fieldHandler, Type realType)
            {
                _fieldHandler = fieldHandler;
                _realType = realType;
            }

            public bool Equals(SimpleLoaderType other)
            {
                return _fieldHandler == other._fieldHandler && _realType == other._realType;
            }
        }

        readonly ConcurrentDictionary<SimpleLoaderType, object>    //object is of type Action<AbstractBufferedReader, IReaderCtx, (object or value type same as in conc. dic. key)>
            _simpleLoader = new ConcurrentDictionary<SimpleLoaderType, object>();

        internal List<ulong> FreeContentOldDict { get; } = new List<ulong>();
        internal List<ulong> FreeContentNewDict { get; } = new List<ulong>();
        internal List<ulong> FreeContentOldOid { get; } = new List<ulong>();
        internal List<ulong> FreeContentNewOid { get; } = new List<ulong>();
        internal byte[] Prefix { get; private set; }

        public RelationInfo(uint id, string name, IRelationInfoResolver relationInfoResolver, Type interfaceType,
                            Type clientType, IInternalObjectDBTransaction tr)
        {
            _id = id;
            _name = name;
            _relationInfoResolver = relationInfoResolver;
            _interfaceType = interfaceType;
            _clientType = clientType;
            CalculatePrefix();
            LoadUnresolvedVersionInfos(tr.KeyValueDBTransaction);
            ClientRelationVersionInfo = CreateVersionInfoByReflection();
            ResolveVersionInfos();
            ApartFields = FindApartFields(interfaceType, ClientRelationVersionInfo);
            if (LastPersistedVersion > 0 && RelationVersionInfo.Equal(_relationVersions[LastPersistedVersion], ClientRelationVersionInfo))
            {
                _relationVersions[LastPersistedVersion] = ClientRelationVersionInfo;
                ClientTypeVersion = LastPersistedVersion;
            }
            else
            {
                ClientTypeVersion = LastPersistedVersion + 1;
                _relationVersions.Add(ClientTypeVersion, ClientRelationVersionInfo);
                var writerk = new ByteBufferWriter();
                writerk.WriteByteArrayRaw(ObjectDB.RelationVersionsPrefix);
                writerk.WriteVUInt32(_id);
                writerk.WriteVUInt32(ClientTypeVersion);
                var writerv = new ByteBufferWriter();
                ClientRelationVersionInfo.Save(writerv);
                tr.KeyValueDBTransaction.SetKeyPrefix(ByteBuffer.NewEmpty());
                tr.KeyValueDBTransaction.CreateOrUpdateKeyValue(writerk.Data, writerv.Data);

                if (LastPersistedVersion > 0)
                {
                    CheckThatPrimaryKeyHasNotChanged(name, ClientRelationVersionInfo, _relationVersions[LastPersistedVersion]);
                    UpdateSecondaryKeys(tr, ClientRelationVersionInfo, _relationVersions[LastPersistedVersion]);
                }
            }
            _typeConvertorGenerator = tr.Owner.TypeConvertorGenerator;
        }

        static void CheckThatPrimaryKeyHasNotChanged(string name, RelationVersionInfo info, RelationVersionInfo previousInfo)
        {
            var pkFields = info.GetPrimaryKeyFields();
            var prevPkFields = previousInfo.GetPrimaryKeyFields();
            if (pkFields.Count != prevPkFields.Count)
                throw new BTDBException($"Change of primary key in relation '{name}' is not allowed. Field count {pkFields.Count} != {prevPkFields.Count}.");
            var en = pkFields.GetEnumerator();
            var pen = prevPkFields.GetEnumerator();
            while (en.MoveNext() && pen.MoveNext())
            {
                if (!ArePrimaryKeyFieldsCompatible(en.Current.Handler, pen.Current.Handler))
                    throw new BTDBException($"Change of primary key in relation '{name}' is not allowed. Field '{en.Current.Name}' is not compatible.");
            }
        }

        static bool ArePrimaryKeyFieldsCompatible(IFieldHandler newHandler, IFieldHandler previousHandler)
        {
            var newHandledType = newHandler.HandledType();
            var previousHandledType = previousHandler.HandledType();
            if (newHandledType == previousHandledType)
                return true;
            if (newHandledType.IsEnum && previousHandledType.IsEnum)
            {
                var prevEnumCfg = new EnumFieldHandler.EnumConfiguration((previousHandler as EnumFieldHandler).Configuration);
                var newEnumCfg = new EnumFieldHandler.EnumConfiguration((newHandler as EnumFieldHandler).Configuration);

                return prevEnumCfg.IsBinaryRepresentationSubsetOf(newEnumCfg);
            }
            return false;
        }

        void UpdateSecondaryKeys(IInternalObjectDBTransaction tr, RelationVersionInfo info, RelationVersionInfo previousInfo)
        {
            foreach (var prevIdx in previousInfo.SecondaryKeys.Keys)
            {
                if (!info.SecondaryKeys.ContainsKey(prevIdx))
                    DeleteSecondaryKey(tr.KeyValueDBTransaction, prevIdx);
            }
            var secKeysToAdd = new List<KeyValuePair<uint, SecondaryKeyInfo>>();
            foreach (var sk in info.SecondaryKeys)
            {
                if (!previousInfo.SecondaryKeys.ContainsKey(sk.Key))
                    secKeysToAdd.Add(sk);
            }
            if (secKeysToAdd.Count > 0)
                CalculateSecondaryKey(tr, secKeysToAdd);
        }

        void DeleteSecondaryKey(IKeyValueDBTransaction keyValueTr, uint prevIdx)
        {
            var writer = new ByteBufferWriter();
            writer.WriteBlock(ObjectDB.AllRelationsSKPrefix);
            writer.WriteVUInt32(Id);
            writer.WriteVUInt32(prevIdx);

            keyValueTr.SetKeyPrefix(writer.Data);

            keyValueTr.EraseAll();
        }

        void CalculateSecondaryKey(IInternalObjectDBTransaction tr, IList<KeyValuePair<uint, SecondaryKeyInfo>> indexes)
        {
            var keyWriter = new ByteBufferWriter();

            var enumeratorType = typeof(RelationEnumerator<>).MakeGenericType(_clientType);
            keyWriter.WriteByteArrayRaw(Prefix);
            var enumerator = (IEnumerator)Activator.CreateInstance(enumeratorType, tr, this, keyWriter.GetDataAndRewind().ToAsyncSafe(), new SimpleModificationCounter());

            var keySavers = new Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object>[indexes.Count];

            for (int i = 0; i < indexes.Count; i++)
            {
                keySavers[i] = CreateSaver(ClientRelationVersionInfo.GetSecondaryKeyFields(indexes[i].Key),
                    $"Relation_{Name}_Upgrade_SK_{indexes[i].Value.Name}_KeySaver");
            }

            while (enumerator.MoveNext())
            {
                var obj = enumerator.Current;

                tr.TransactionProtector.Start();
                tr.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsSKPrefix);

                for (int i = 0; i < indexes.Count; i++)
                {
                    keyWriter.WriteVUInt32(Id);
                    keyWriter.WriteVUInt32(indexes[i].Key);
                    keySavers[i](tr, keyWriter, obj);
                    var keyBytes = keyWriter.GetDataAndRewind();

                    if (!tr.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, ByteBuffer.NewEmpty()))
                        throw new BTDBException("Internal error, secondary key bytes must be always unique.");
                }
            }
        }

        void LoadUnresolvedVersionInfos(IKeyValueDBTransaction tr)
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
                var relationVersionInfo = RelationVersionInfo.LoadUnresolved(valueReader, _name);
                _relationVersions[LastPersistedVersion] = relationVersionInfo;
            } while (tr.FindNextKey());
        }

        void ResolveVersionInfos()
        {
            foreach (var version in _relationVersions)
            {
                if (version.Key == ClientTypeVersion)
                    continue;
                version.Value.ResolveFieldHandlers(_relationInfoResolver.FieldHandlerFactory);
            }
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

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetPrimaryKeysLoader()
        {
            if (_primaryKeysLoader == null)
            {
                var loader = CreateLoader(ClientTypeVersion, ClientRelationVersionInfo.GetPrimaryKeyFields(), $"RelationKeyLoader_{Name}");
                Interlocked.CompareExchange(ref _primaryKeysLoader, loader, null);
            }
            return _primaryKeysLoader;
        }

        void CreateSaverIl(IILGen ilGen, IEnumerable<TableFieldInfo> fields,
                           Action<IILGen> pushInstance, Action<IILGen> pushRelationIface,
                           Action<IILGen> pushWriter, Action<IILGen> pushTransaction)
        {
            var writerCtxLocal = CreateWriterCtx(ilGen, fields, pushWriter, pushTransaction);
            var props = ClientType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            foreach (var field in fields)
            {
                var getter = props.First(p => GetPersistentName(p) == field.Name).GetGetMethod(true);
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

        static IILLocal CreateWriterCtx(IILGen ilGenerator, IEnumerable<TableFieldInfo> fields, Action<IILGen> pushWriter, Action<IILGen> pushTransaction)
        {
            var anyNeedsCtx = fields.Any(tfi => tfi.Handler.NeedsCtx());
            IILLocal writerCtxLocal = null;
            if (anyNeedsCtx)
            {
                writerCtxLocal = ilGenerator.DeclareLocal(typeof(IWriterCtx));
                ilGenerator
                    .Do(pushTransaction)
                    .Do(pushWriter)
                    .Newobj(() => new DBWriterCtx(null, null))
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

        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, byte[], byte[]> CreateBytesToSKSaver(
            uint version, uint secondaryKeyIndex, string saverName)
        {
            var method = ILBuilder.Instance.NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedWriter, byte[], byte[]>>(saverName);
            var ilGenerator = method.Generator;

            Action<IILGen> pushWriter = il => il.Ldarg(1);

            var firstBuffer = new BufferInfo();  //pk's
            var secondBuffer = new BufferInfo(); //values
            var outOfOrderSkParts = new Dictionary<int, LocalAndHandler>(); //local and specialized saver

            var pks = ClientRelationVersionInfo.GetPrimaryKeyFields().ToList();
            var skFieldIds = ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Fields.ToList();
            var skFields = ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex).ToList();
            var valueFields = _relationVersions[version].GetValueFields().ToList();
            var writerCtxLocal = CreateWriterCtx(ilGenerator, skFields, pushWriter, il => il.Ldarg(0));
            for (var skFieldIdx = 0; skFieldIdx < skFieldIds.Count; skFieldIdx++)
            {
                LocalAndHandler saveLocalInfo;
                if (outOfOrderSkParts.TryGetValue(skFieldIdx, out saveLocalInfo))
                {
                    var writerOrCtx = WriterOrContextForHandler(saveLocalInfo.Handler, writerCtxLocal, pushWriter);
                    saveLocalInfo.Handler.Save(ilGenerator, writerOrCtx, il => il.Ldloc(saveLocalInfo.Local));
                    continue;
                }
                var skf = skFieldIds[skFieldIdx];
                if (skf.IsFromPrimaryKey)
                {
                    InitializeBuffer(2, ref firstBuffer, ilGenerator, pks);
                    //firstBuffer.ActualFieldIdx == number of processed PK's
                    for (var pkIdx = firstBuffer.ActualFieldIdx; pkIdx < skf.Index; pkIdx++)
                    {
                        //all PK parts are contained in SK
                        int skFieldIdxForPk, tmp;
                        FindPosition(pkIdx, skFieldIds, 0, out tmp, out skFieldIdxForPk);
                        StoreIntoLocal(ilGenerator, pks[pkIdx].Handler, firstBuffer, outOfOrderSkParts, skFieldIdxForPk, skFields[skFieldIdxForPk].Handler);
                    }
                    CopyToOutput(ilGenerator, pks[(int)skf.Index].Handler, writerCtxLocal, pushWriter, skFields[skFieldIdx].Handler, firstBuffer);
                    firstBuffer.ActualFieldIdx = (int)skf.Index + 1;
                }
                else
                {
                    InitializeBuffer(3, ref secondBuffer, ilGenerator, valueFields);

                    var valueFieldIdx = valueFields.FindIndex(tfi => tfi.Name == skFields[skFieldIdx].Name);
                    for (var valueIdx = secondBuffer.ActualFieldIdx; valueIdx < valueFieldIdx; valueIdx++)
                    {
                        var valueField = valueFields[valueIdx];
                        var storeForSkIndex = skFields.FindIndex(skFieldIdx, fi => fi.Name == valueField.Name);
                        if (storeForSkIndex == -1)
                            valueField.Handler.Skip(ilGenerator, valueField.Handler.NeedsCtx() ? secondBuffer.PushCtx : secondBuffer.PushReader);
                        else
                            StoreIntoLocal(ilGenerator, valueField.Handler, secondBuffer, outOfOrderSkParts, storeForSkIndex, skFields[storeForSkIndex].Handler);
                    }
                    CopyToOutput(ilGenerator, valueFields[valueFieldIdx].Handler, writerCtxLocal, pushWriter, skFields[skFieldIdx].Handler, secondBuffer);
                    secondBuffer.ActualFieldIdx = valueFieldIdx + 1;
                }
            }

            ilGenerator.Ret();
            return method.Create();
        }

        static void CopyToOutput(IILGen ilGenerator, IFieldHandler valueHandler, IILLocal writerCtxLocal, Action<IILGen> pushWriter,
                                 IFieldHandler skHandler, BufferInfo buffer)

        {
            var writerOrCtx = WriterOrContextForHandler(valueHandler, writerCtxLocal, pushWriter);
            skHandler.SpecializeSaveForType(valueHandler.HandledType()).
                Save(ilGenerator, writerOrCtx, il =>
                {
                    valueHandler.Load(ilGenerator, valueHandler.NeedsCtx() ? buffer.PushCtx : buffer.PushReader);
                });
        }

        static void StoreIntoLocal(IILGen ilGenerator, IFieldHandler valueHandler, BufferInfo bufferInfo,
            Dictionary<int, LocalAndHandler> outOfOrderSkParts, int skFieldIdx, IFieldHandler skFieldHandler)
        {
            var local = ilGenerator.DeclareLocal(valueHandler.HandledType());
            valueHandler.Load(ilGenerator, valueHandler.NeedsCtx() ? bufferInfo.PushCtx : bufferInfo.PushReader);
            ilGenerator.Stloc(local);
            outOfOrderSkParts[skFieldIdx] = new LocalAndHandler
            {
                Handler = skFieldHandler.SpecializeSaveForType(valueHandler.HandledType()),
                Local = local
            };
        }

        static Action<IILGen> WriterOrContextForHandler(IFieldHandler handler, IILLocal writerCtxLocal, Action<IILGen> pushWriter)
        {
            Action<IILGen> writerOrCtx;
            if (handler.NeedsCtx())
            {
                writerOrCtx = il => il.Ldloc(writerCtxLocal);
            }
            else
            {
                writerOrCtx = pushWriter;
            }
            return writerOrCtx;
        }

        static void InitializeBuffer(ushort bufferArgIdx, ref BufferInfo bufferInfo, IILGen ilGenerator, List<TableFieldInfo> fields)
        {
            if (!bufferInfo.ReaderCreated)
            {
                bufferInfo.ReaderCreated = true;
                var readerLoc = ilGenerator.DeclareLocal(typeof(ByteArrayReader));
                bufferInfo.PushReader = il => il.Ldloc(readerLoc);
                ilGenerator
                    .Ldarg(bufferArgIdx)
                    .Newobj(() => new ByteArrayReader(null))
                    .Stloc(readerLoc);

                ilGenerator
                    .Do(bufferInfo.PushReader).Call(() => default(ByteArrayReader).SkipVUInt32());

                if (fields.Any(tfi => tfi.Handler.NeedsCtx()))
                {
                    var readerCtxLocal = ilGenerator.DeclareLocal(typeof(IReaderCtx));
                    ilGenerator
                        .Ldarg(0) //tr
                        .Ldloc(readerLoc)
                        .Newobj(() => new DBReaderCtx(null, null))
                        .Stloc(readerCtxLocal);
                    bufferInfo.PushCtx = il => il.Ldloc(readerCtxLocal);
                }
            }
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> CreateSaverWithApartFields(
            IEnumerable<TableFieldInfo> fields, string saverName)
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
            var secondaryKeyFields = new List<TableFieldInfo>();
            var secondaryKeys = new List<Tuple<int, IList<SecondaryKeyAttribute>>>(); //positive: sec key field idx, negative: pk order, attrs

            var fields = new List<TableFieldInfo>(props.Length);
            foreach (var pi in props)
            {
                if (pi.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) continue;
                if (pi.GetIndexParameters().Length != 0) continue;
                var pks = pi.GetCustomAttributes(typeof(PrimaryKeyAttribute), true);
                PrimaryKeyAttribute actualPKAttribute = null;
                if (pks.Length != 0)
                {
                    actualPKAttribute = (PrimaryKeyAttribute)pks[0];
                    var fieldInfo = TableFieldInfo.Build(Name, pi, _relationInfoResolver.FieldHandlerFactory,
                        FieldHandlerOptions.Orderable);
                    if (fieldInfo.Handler.NeedsCtx())
                        throw new BTDBException($"Unsupported key field {fieldInfo.Name} type.");
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
                            secondaryKeyFields.Add(TableFieldInfo.Build(Name, pi, _relationInfoResolver.FieldHandlerFactory, FieldHandlerOptions.Orderable));
                    }
                    var key = (SecondaryKeyAttribute)sks[i];
                    if (key.Name == "Id")
                        throw new BTDBException("'Id' is invalid name for secondary key, it is reserved for primary key identification.");
                    currentList.Add(key);
                }
                if (actualPKAttribute == null)
                    fields.Add(TableFieldInfo.Build(Name, pi, _relationInfoResolver.FieldHandlerFactory, FieldHandlerOptions.None));
            }
            var prevVersion = LastPersistedVersion > 0 ? _relationVersions[LastPersistedVersion] : null;
            return new RelationVersionInfo(primaryKeys, secondaryKeys, secondaryKeyFields.ToArray(), fields.ToArray(), prevVersion);
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

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetValueLoader(uint version)
        {
            return _valueLoaders.GetOrAdd(version, (ver, relationInfo) => CreateLoader(ver,
                relationInfo._relationVersions[ver].GetValueFields(), $"RelationValueLoader_{relationInfo.Name}_{ver}"), this);
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>> GetIDictFinder(uint version)
        {
            return _valueIDictFinders.GetOrAdd(version, CreateIDictFinder);
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> GetSecondaryKeysKeySaver
            (uint secondaryKeyIndex)
        {
            return _secondaryKeysSavers.GetOrAdd(secondaryKeyIndex,
                (secKeyIndex, relationInfo) => CreateSaverWithApartFields(relationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(secKeyIndex),
                    $"Relation_{relationInfo.Name}_SK_{relationInfo.ClientRelationVersionInfo.SecondaryKeys[secKeyIndex].Name}_KeySaver"), this);
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedWriter, byte[], byte[]> GetPKValToSKMerger
            (uint version, uint secondaryKeyIndex)
        {
            var h = secondaryKeyIndex + version * 100000ul;
            return _secondaryKeysConvertSavers.GetOrAdd(h,
                (_, ver, secKeyIndex, relationInfo) => CreateBytesToSKSaver(ver, secKeyIndex, 
                    $"Relation_{relationInfo.Name}_PkVal_to_SK_{relationInfo.ClientRelationVersionInfo.SecondaryKeys[secKeyIndex].Name}_v{ver}"),
                version, secondaryKeyIndex, this);
        }

        //takes secondaryKey key & value bytes and restores primary key bytes
        public Action<byte[], byte[], AbstractBufferedWriter> GetSKKeyValuetoPKMerger
            (uint secondaryKeyIndex, uint paramFieldCountInFirstBuffer)
        {
            var h = 10000ul * secondaryKeyIndex + paramFieldCountInFirstBuffer;
            return _secondaryKeyValuetoPKLoader.GetOrAdd(h,
                (_, secKeyIndex, relationInfo, paramFieldCount) => relationInfo.CreatePrimaryKeyFromSKDataMerger(secKeyIndex, paramFieldCount,
                        $"Relation_SK_to_PK_{relationInfo.ClientRelationVersionInfo.SecondaryKeys[secKeyIndex].Name}_p{paramFieldCount}"),
                secondaryKeyIndex, this, (int)paramFieldCountInFirstBuffer);
        }

        struct MemorizedPositionWithLength
        {
            public int BufferIndex { get; set; } //0 first, 1 second
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

        Action<byte[], byte[], AbstractBufferedWriter> CreatePrimaryKeyFromSKDataMerger(uint secondaryKeyIndex,
                                                         int paramFieldCountInFirstBuffer, string mergerName)
        {
            var method = ILBuilder.Instance.NewMethod<Action<byte[], byte[], AbstractBufferedWriter>>(mergerName);
            var ilGenerator = method.Generator;

            Action<IILGen> pushWriter = il => il.Ldarg(2);
            var skFields = ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Fields;

            var positionLoc = ilGenerator.DeclareLocal(typeof(ulong)); //stored position
            var memoPositionLoc = ilGenerator.DeclareLocal(typeof(IMemorizedPosition));

            var firstBuffer = new BufferInfo();
            var secondBuffer = new BufferInfo { ActualFieldIdx = paramFieldCountInFirstBuffer };
            var outOfOrderPKParts = new Dictionary<int, MemorizedPositionWithLength>(); //index -> bufferIdx, IMemorizedPosition, length

            var pks = ClientRelationVersionInfo.GetPrimaryKeyFields().ToList();
            for (var pkIdx = 0; pkIdx < pks.Count; pkIdx++)
            {
                if (outOfOrderPKParts.ContainsKey(pkIdx))
                {
                    var memo = outOfOrderPKParts[pkIdx];
                    var pushReader = GetBufferPushAction(memo.BufferIndex, firstBuffer.PushReader, secondBuffer.PushReader);
                    CopyFromMemorizedPosition(ilGenerator, pushReader, pushWriter, memo, memoPositionLoc);
                    continue;
                }
                int bufferIdx, skFieldIdx;
                FindPosition(pkIdx, skFields, paramFieldCountInFirstBuffer, out bufferIdx, out skFieldIdx);
                if (bufferIdx == 0)
                {
                    MergerInitializeFirstBufferReader(ilGenerator, ref firstBuffer);
                    CopyFromBuffer(ilGenerator, bufferIdx, skFieldIdx, ref firstBuffer, outOfOrderPKParts, pks, skFields, positionLoc,
                        memoPositionLoc, pushWriter);
                }
                else
                {
                    MergerInitializeBufferReader(ilGenerator, ref secondBuffer, 1);
                    CopyFromBuffer(ilGenerator, bufferIdx, skFieldIdx, ref secondBuffer, outOfOrderPKParts, pks, skFields, positionLoc,
                        memoPositionLoc, pushWriter);
                }
            }

            ilGenerator.Ret();
            return method.Create();
        }

        void CopyFromBuffer(IILGen ilGenerator, int bufferIdx, int skFieldIdx, ref BufferInfo bi, Dictionary<int, MemorizedPositionWithLength> outOfOrderPKParts,
                            List<TableFieldInfo> pks, IList<FieldId> skFields, IILLocal positionLoc, IILLocal memoPositionLoc, Action<IILGen> pushWriter)
        {
            for (var idx = bi.ActualFieldIdx; idx < skFieldIdx; idx++)
            {
                var field = skFields[idx];
                if (field.IsFromPrimaryKey)
                {
                    outOfOrderPKParts[(int)field.Index] = SkipWithMemorizing(bufferIdx, ilGenerator, bi.PushReader,
                                                                   pks[(int)field.Index].Handler, positionLoc);
                }
                else
                {
                    var f = ClientRelationVersionInfo.GetSecondaryKeyField((int)field.Index);
                    f.Handler.Skip(ilGenerator, bi.PushReader);
                }
            }

            var skField = skFields[skFieldIdx];
            GenerateCopyFieldFromByteBufferToWriterIl(ilGenerator, pks[(int)skField.Index].Handler, bi.PushReader,
                                                              pushWriter, positionLoc, memoPositionLoc);

            bi.ActualFieldIdx = skFieldIdx + 1;
        }

        static void FindPosition(int pkIdx, IList<FieldId> skFields, int paramFieldCountInFirstBuffer, out int bufferIdx, out int skFieldIdx)
        {
            for (var i = 0; i < skFields.Count; i++)
            {
                var field = skFields[i];
                if (!field.IsFromPrimaryKey) continue;
                if (field.Index != pkIdx) continue;
                skFieldIdx = i;
                bufferIdx = i < paramFieldCountInFirstBuffer ? 0 : 1;
                return;
            }
            throw new BTDBException("Secondary key relation processing error.");
        }

        static void MergerInitializeBufferReader(IILGen ilGenerator, ref BufferInfo bi, ushort arg)
        {
            if (bi.ReaderCreated)
                return;
            bi.ReaderCreated = true;
            var readerLoc = ilGenerator.DeclareLocal(typeof(ByteArrayReader));
            bi.PushReader = il => il.Ldloc(readerLoc);
            ilGenerator
                .Ldarg(arg)
                .Newobj(() => new ByteArrayReader(null))
                .Stloc(readerLoc);
        }

        static void MergerInitializeFirstBufferReader(IILGen ilGenerator, ref BufferInfo bi)
        {
            if (bi.ReaderCreated)
                return;
            MergerInitializeBufferReader(ilGenerator, ref bi, 0);
            ilGenerator
                //skip all relations
                .Do(bi.PushReader)
                .LdcI4(ObjectDB.AllRelationsSKPrefix.Length)
                .Callvirt(() => default(ByteArrayReader).SkipBlock(0))
                //skip relation id
                .Do(bi.PushReader).Call(() => default(ByteArrayReader).SkipVUInt32())
                //skip secondary key index
                .Do(bi.PushReader).Call(() => default(ByteArrayReader).SkipVUInt32());
        }


        Action<IILGen> GetBufferPushAction(int bufferIndex, Action<IILGen> pushReaderFirst, Action<IILGen> pushReaderSecond)
        {
            return bufferIndex == 0 ? pushReaderFirst : pushReaderSecond;
        }

        MemorizedPositionWithLength SkipWithMemorizing(int activeBuffer, IILGen ilGenerator, Action<IILGen> pushReader, IFieldHandler handler, IILLocal tempPosition)
        {
            var memoPos = ilGenerator.DeclareLocal(typeof(IMemorizedPosition));
            var memoLen = ilGenerator.DeclareLocal(typeof(int));
            var position = new MemorizedPositionWithLength { BufferIndex = activeBuffer, Pos = memoPos, Length = memoLen };
            MemorizeCurrentPosition(ilGenerator, pushReader, memoPos);
            StoreCurrentPosition(ilGenerator, pushReader, tempPosition);
            handler.Skip(ilGenerator, pushReader);
            ilGenerator
                .Do(pushReader) //[VR]
                .Callvirt(() => default(ByteArrayReader).GetCurrentPosition()) //[posNew];
                .Ldloc(tempPosition) //[posNew, posOld]
                .Sub() //[readLen]
                .ConvI4() //[readLen(i)]
                .Stloc(memoLen); //[]
            return position;
        }

        void CopyFromMemorizedPosition(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushWriter, MemorizedPositionWithLength memo,
                                       IILLocal memoPositionLoc)
        {
            MemorizeCurrentPosition(ilGenerator, pushReader, memoPositionLoc);
            ilGenerator
                .Do(pushWriter) //[W]
                .Do(pushReader) //[W,VR]
                .Ldloc(memo.Length) //[W, VR, readLen]
                .Ldloc(memo.Pos) //[W, VR, readLen, Memorize]
                .Callvirt(() => default(IMemorizedPosition).Restore()) //[W, VR]
                .Call(() => default(ByteArrayReader).ReadByteArrayRaw(0)) //[W, byte[]]
                .Call(() => default(AbstractBufferedWriter).WriteByteArrayRaw(null)) //[]
                .Ldloc(memoPositionLoc) //[Memorize]
                .Callvirt(() => default(IMemorizedPosition).Restore()); //[]
        }

        void MemorizeCurrentPosition(IILGen ilGenerator, Action<IILGen> pushReader, IILLocal memoPositionLoc)
        {
            ilGenerator
                .Do(pushReader)
                .Call(() => default(ByteArrayReader).MemorizeCurrentPosition())
                .Stloc(memoPositionLoc);
        }

        void StoreCurrentPosition(IILGen ilGenerator, Action<IILGen> pushReader, IILLocal positionLoc)
        {
            ilGenerator
                .Do(pushReader)
                .Callvirt(() => default(ByteArrayReader).GetCurrentPosition())
                .Stloc(positionLoc);
        }

        void GenerateCopyFieldFromByteBufferToWriterIl(IILGen ilGenerator, IFieldHandler handler, Action<IILGen> pushReader,
                                                       Action<IILGen> pushWriter, IILLocal positionLoc, IILLocal memoPositionLoc)
        {
            MemorizeCurrentPosition(ilGenerator, pushReader, memoPositionLoc);
            StoreCurrentPosition(ilGenerator, pushReader, positionLoc);

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

        public object GetSimpleLoader(SimpleLoaderType handler)
        {
            return _simpleLoader.GetOrAdd(handler, CreateSimpleLoader);
        }

        object CreateSimpleLoader(SimpleLoaderType loaderType)
        {
            var delegateType = typeof(Func<,,>).MakeGenericType(typeof(AbstractBufferedReader), typeof(IReaderCtx), loaderType.RealType);
            var dm = ILBuilder.Instance.NewMethod(loaderType.FieldHandler.Name + "SimpleReader", delegateType);
            var ilGenerator = dm.Generator;
            Action<IILGen> pushReaderOrCtx = il => il.Ldarg((ushort)(loaderType.FieldHandler.NeedsCtx() ? 1 : 0));
            loaderType.FieldHandler.Load(ilGenerator, pushReaderOrCtx);
            ilGenerator
                .Do(_typeConvertorGenerator.GenerateConversion(loaderType.FieldHandler.HandledType(), loaderType.RealType))
                .Ret();
            return dm.Create();
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> CreateLoader(uint version, IEnumerable<TableFieldInfo> fields, string loaderName)
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
                    var fieldInfo = props.First(p => GetPersistentName(p) == destFieldInfo.Name).GetSetMethod(true);
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
                    var setterMethod = props.First(p => GetPersistentName(p) == srcFieldInfo.Name).GetSetMethod(true);
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

        static string GetPersistentName(PropertyInfo p)
        {
            var a = p.GetCustomAttribute<PersistedNameAttribute>();
            return a != null ? a.Name : p.Name;
        }

        public object CreateInstance(IInternalObjectDBTransaction tr, ByteBuffer keyBytes, ByteBuffer valueBytes,
                                     bool keyContainsRelationIndex = true)
        {
            var obj = Creator(tr);
            var keyReader = new ByteBufferReader(keyBytes);
            if (keyContainsRelationIndex)
                keyReader.SkipVUInt32(); //index Relation
            GetPrimaryKeysLoader()(tr, keyReader, obj);
            var valueReader = new ByteBufferReader(valueBytes);
            var version = valueReader.ReadVUInt32();
            GetValueLoader(version)(tr, valueReader, obj);
            return obj;
        }

        public void FreeContent(IInternalObjectDBTransaction tr, ByteBuffer valueBytes)
        {
            FreeContentOldDict.Clear();
            FreeContentOldOid.Clear();

            FindUsedObjectsToFree(tr, valueBytes, FreeContentOldDict, FreeContentOldOid);

            foreach (var dictId in FreeContentOldDict)
            {
                FreeIDictionary(tr, dictId);
            }

            foreach (var oid in FreeContentOldOid)
            {
                FreeObject(tr, oid);
            }
        }

        internal static void FreeIDictionary(IInternalObjectDBTransaction tr, ulong dictId)
        {
            var o = ObjectDB.AllDictionariesPrefix.Length;
            var prefix = new byte[o + PackUnpack.LengthVUInt(dictId)];
            Array.Copy(ObjectDB.AllDictionariesPrefix, prefix, o);
            PackUnpack.PackVUInt(prefix, ref o, dictId);
            tr.TransactionProtector.Start();
            tr.KeyValueDBTransaction.SetKeyPrefixUnsafe(prefix);
            tr.KeyValueDBTransaction.EraseAll();
        }

        internal static void FreeObject(IInternalObjectDBTransaction tr, ulong oid)
        {
            var o = ObjectDB.AllDictionariesPrefix.Length;
            var prefix = new byte[o + PackUnpack.LengthVUInt(oid)];
            Array.Copy(ObjectDB.AllObjectsPrefix, prefix, o);
            PackUnpack.PackVUInt(prefix, ref o, oid);
            tr.TransactionProtector.Start();
            tr.KeyValueDBTransaction.SetKeyPrefixUnsafe(prefix);
            tr.KeyValueDBTransaction.EraseAll();
        }

        public void FindUsedObjectsToFree(IInternalObjectDBTransaction tr, ByteBuffer valueBytes, IList<ulong> dictionaries, IList<ulong> oids)
        {
            var valueReader = new ByteArrayReader(valueBytes.ToByteArray());
            var version = valueReader.ReadVUInt32();

            GetIDictFinder(version)(tr, valueReader, dictionaries, oids);
        }

        Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>> CreateIDictFinder(uint version)
        {
            var method = ILBuilder.Instance
                .NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>>>(
                    $"Relation{Name}_IDictFinder");
            var ilGenerator = method.Generator;

            var relationVersionInfo = _relationVersions[version];
            var needGenerateFreeFor = 0;
            var fakeMethod = ILBuilder.Instance.NewMethod<Action>("Relation_fake");
            var fakeGenerator = fakeMethod.Generator;
            var valueFields = relationVersionInfo.GetValueFields().ToArray();
            for (int i = 0; i < valueFields.Length; i++)
            {
                var needsFreeContent = valueFields[i].Handler.FreeContent(fakeGenerator, _ => { });
                if (needsFreeContent)
                    needGenerateFreeFor = i + 1;
            }
            if (needGenerateFreeFor == 0)
            {
                ilGenerator.Ret();
                return method.Create();
            }
            var anyNeedsCtx = relationVersionInfo.GetValueFields().Any(f => f.Handler.NeedsCtx());
            if (anyNeedsCtx)
            {
                ilGenerator.DeclareLocal(typeof(IReaderCtx)); //loc 0
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(1)
                    .Ldarg(2)
                    .Ldarg(3)
                    .Newobj(() => new DBReaderWithFreeInfoCtx(null, null, null, null))
                    .Stloc(0);
            }
            for (int i = 0; i < needGenerateFreeFor; i++)
            {
                Action<IILGen> readerOrCtx;
                if (valueFields[i].Handler.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(0);
                else
                    readerOrCtx = il => il.Ldarg(1);
                valueFields[i].Handler.FreeContent(ilGenerator, readerOrCtx);
            }
            ilGenerator.Ret();
            return method.Create();
        }

        void CalculatePrefix()
        {
            var o = ObjectDB.AllRelationsPKPrefix.Length;
            var prefix = new byte[o + PackUnpack.LengthVUInt(Id)];
            Array.Copy(ObjectDB.AllRelationsPKPrefix, prefix, o);
            PackUnpack.PackVUInt(prefix, ref o, Id);
            Prefix = prefix;
        }
    }

    public class DBReaderWithFreeInfoCtx : DBReaderCtx
    {
        readonly IList<ulong> _freeDictionaries;
        readonly IList<ulong> _freeOids;

        public DBReaderWithFreeInfoCtx(IInternalObjectDBTransaction transaction, AbstractBufferedReader reader,
                                       IList<ulong> freeDictionaries, IList<ulong> freeOids)
            : base(transaction, reader)
        {
            _freeDictionaries = freeDictionaries;
            _freeOids = freeOids;
        }

        public IList<ulong> DictIds => _freeDictionaries;
        public IList<ulong> Oids => _freeOids;

        public override void RegisterDict(ulong dictId)
        {
            _freeDictionaries.Add(dictId);
        }

        public override void RegisterOid(ulong oid)
        {
            _freeOids.Add(oid);
        }

        public override void FreeContentInNativeObject()
        {
            var oid = _reader.ReadVInt64();
            if (oid == 0)
            {
            }
            else if (oid <= int.MinValue || oid > 0)
            {
                RegisterOid((ulong)oid);
                _transaction.TransactionProtector.Start();
                _transaction.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                if (!_transaction.KeyValueDBTransaction.FindExactKey(ObjectDBTransaction.BuildKeyFromOid((ulong)oid)))
                    return;
                var reader = new ByteArrayReader(_transaction.KeyValueDBTransaction.GetValueAsByteArray());
                var tableId = reader.ReadVUInt32();
                var tableInfo = ((ObjectDB)_transaction.Owner).TablesInfo.FindById(tableId);
                if (tableInfo == null)
                    return;
                var tableVersion = reader.ReadVUInt32();
                var freeContentTuple = tableInfo.GetFreeContent(tableVersion);
                if (freeContentTuple.Item1)
                {
                    freeContentTuple.Item2(_transaction, null, reader, _freeDictionaries, _freeOids);
                }
            }
            else
            {
                _transaction.FreeContentInNativeObject(this);
            }
        }
    }

    class SimpleModificationCounter : IRelationModificationCounter
    {
        public int ModificationCounter => 0;

        public void CheckModifiedDuringEnum(int prevModification)
        {
        }
    }
}
