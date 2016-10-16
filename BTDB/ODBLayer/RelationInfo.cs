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
        readonly ITypeConvertorGenerator _typeConvertorGenerator;

        readonly IDictionary<uint, RelationVersionInfo> _relationVersions = new Dictionary<uint, RelationVersionInfo>();
        Func<IInternalObjectDBTransaction, object> _creator;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object> _primaryKeysSaver;
        Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object> _valueSaver;

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>
            _primaryKeysLoaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>
            _valueLoaders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>>>
            _valueIDictFinders = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>>>();

        //SK
        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>  //secondary key idx => sk key saver
            _secondaryKeysSavers = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>();

        readonly ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>  //secondary key idx => sk value saver
            _secondaryKeysValueSavers = new ConcurrentDictionary<uint, Action<IInternalObjectDBTransaction, AbstractBufferedWriter, object, object>>();

        readonly ConcurrentDictionary<ulong, Action<byte[], byte[], AbstractBufferedWriter>>  //secondary key idx => 
            _secondaryKeyValuetoPKLoader = new ConcurrentDictionary<ulong, Action<byte[], byte[], AbstractBufferedWriter>>();

        int _modificationCounter;

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

        public RelationInfo(uint id, string name, IRelationInfoResolver relationInfoResolver, Type interfaceType,
                            Type clientType, IInternalObjectDBTransaction tr)
        {
            _id = id;
            _name = name;
            _relationInfoResolver = relationInfoResolver;
            _interfaceType = interfaceType;
            _clientType = clientType;
            LoadVersionInfos(tr.KeyValueDBTransaction);
            ClientRelationVersionInfo = CreateVersionInfoByReflection();
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
                    CheckThatPrimaryKeyHasNotChanged(ClientRelationVersionInfo, _relationVersions[LastPersistedVersion]);
                    UpdateSecondaryKeys(tr, ClientRelationVersionInfo, _relationVersions[LastPersistedVersion]);
                }
            }
            _typeConvertorGenerator = tr.Owner.TypeConvertorGenerator;
        }

        void CheckThatPrimaryKeyHasNotChanged(RelationVersionInfo info, RelationVersionInfo previousInfo)
        {
            var pkFields = info.GetPrimaryKeyFields();
            var prevPkFields = previousInfo.GetPrimaryKeyFields();
            if (pkFields.Count != prevPkFields.Count)
                throw new BTDBException("Change of primary key in relation is not allowed.");
            var en = pkFields.GetEnumerator();
            var pen = prevPkFields.GetEnumerator();
            while (en.MoveNext() && pen.MoveNext())
            {
                if (en.Current.Handler.HandledType() != pen.Current.Handler.HandledType())
                    throw new BTDBException("Change of primary key in relation is not allowed.");
            }
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
            keyWriter.WriteByteArrayRaw(ObjectDB.AllRelationsPKPrefix);
            keyWriter.WriteVUInt32(Id);
            var enumerator = (IEnumerator)Activator.CreateInstance(enumeratorType, tr, this, keyWriter.GetDataAndRewind().ToAsyncSafe());

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

        void CreateSaverIl(IILGen ilGen, IEnumerable<TableFieldInfo> fields,
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
                    .Newobj(() => new DBWriterCtx(null, null))
                    .Stloc(writerCtxLocal);
            }
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

        void StoreNthArgumentOfTypeIntoLoc(IILGen il, ushort argIdx, Type type, ushort locIdx)
        {
            il
               .Ldarg(argIdx)
               .Castclass(type)
               .Stloc(locIdx);
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

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetPrimaryKeysLoader(uint version)
        {
            return _primaryKeysLoaders.GetOrAdd(version, ver => CreateLoader(ver, _relationVersions[version].GetPrimaryKeyFields(), $"RelationKeyLoader_{Name}_{ver}"));
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, object> GetValueLoader(uint version)
        {
            return _valueLoaders.GetOrAdd(version, ver => CreateLoader(ver, _relationVersions[version].GetValueFields(), $"RelationValueLoader_{Name}_{ver}"));
        }

        internal Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>, IList<ulong>> GetIDictFinder(uint version)
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
            (uint secondaryKeyIndex, uint paramFieldCountInFirstBuffer)
        {
            var h = 10000ul * secondaryKeyIndex + paramFieldCountInFirstBuffer;
            return _secondaryKeyValuetoPKLoader.GetOrAdd(h,
                idx => CreatePrimaryKeyFromSKDataMerger(secondaryKeyIndex, paramFieldCountInFirstBuffer,
                        $"Relation_SK_to_PK_{ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Name}_p{paramFieldCountInFirstBuffer}"));
        }

        struct MemorizedPositionWithLength
        {
            public IILLocal Pos { get; set; } // IMemorizedPosition
            public IILLocal Length { get; set; } // int
        }

        Action<byte[], byte[], AbstractBufferedWriter> CreatePrimaryKeyFromSKDataMerger(uint secondaryKeyIndex,
                                                          uint paramFieldCountInFirstBuffer, string mergerName)
        {
            var method = ILBuilder.Instance.NewMethod<Action<byte[], byte[], AbstractBufferedWriter>>(mergerName);
            var ilGenerator = method.Generator;

            Action<IILGen> pushWriter = il => il.Ldarg(2);
            var skFields = ClientRelationVersionInfo.SecondaryKeys[secondaryKeyIndex].Fields;

            var readerLoc = ilGenerator.DeclareLocal(typeof(ByteArrayReader));
            var positionLoc = ilGenerator.DeclareLocal(typeof(ulong)); //stored position
            var memoPositionLoc = ilGenerator.DeclareLocal(typeof(IMemorizedPosition));

            Action<IILGen> pushReader = il => il.Ldloc(readerLoc);

            if (paramFieldCountInFirstBuffer > 0)
            {
                ilGenerator
                    .Ldarg(0)
                    .Newobj(() => new ByteArrayReader(null))
                    .Stloc(readerLoc);
                ilGenerator
                    //skip all relations
                    .Do(pushReader)
                    .LdcI4(ObjectDB.AllRelationsSKPrefix.Length)
                    .Callvirt(() => default(ByteArrayReader).SkipBlock(0))
                    //skip relation id
                    .Do(pushReader).Call(() => default(ByteArrayReader).SkipVUInt32())
                    //skip secondary key index
                    .Do(pushReader).Call(() => default(ByteArrayReader).SkipVUInt32());
            }

            var pks = ClientRelationVersionInfo.GetPrimaryKeyFields().ToList();
            int processedFieldCount = 0;
            int lastPKIndex = -1;
            var outOfOrderPKParts = new Dictionary<int, MemorizedPositionWithLength>(); //index -> IMemorizedPosition, length
            var seekPositions = new Dictionary<int, IILLocal>(); //index - > IMemorizedPosition
            for (int i = 0; i < skFields.Count; i++)
            {
                var field = skFields[i];
                if (processedFieldCount == paramFieldCountInFirstBuffer)
                {
                    ilGenerator
                        .Ldarg(1)
                        .Newobj(() => new ByteArrayReader(null))
                        .Stloc(readerLoc);
                }

                if (field.IsFromPrimaryKey)
                {
                    if (outOfOrderPKParts.ContainsKey((int)field.Index))
                    {
                        var memo = outOfOrderPKParts[(int)field.Index];
                        CopyFromMemorizedPosition(ilGenerator, pushReader, pushWriter, memo);
                        continue;
                    }
                    while (lastPKIndex + 1 < field.Index)
                    {   //memorize position & length in local variables
                        lastPKIndex++;
                        outOfOrderPKParts[lastPKIndex] = SkipWithMemorizing(ilGenerator, pushReader,
                                                                   pks[lastPKIndex].Handler, positionLoc);
                    }
                    if (seekPositions.ContainsKey((int)field.Index))
                    {
                        var position = seekPositions[(int)field.Index];
                        ilGenerator
                            .Ldloc(position) //[Memorize]
                            .Callvirt(() => default(IMemorizedPosition).Restore()); //[]
                    }
                    GenerateCopyFieldFromByteBufferToWriterIl(ilGenerator, pks[(int)field.Index].Handler, pushReader,
                                                              pushWriter, positionLoc, memoPositionLoc);
                    if (NeedMemorizeCurrentPosition(i, skFields))
                    {
                        var position = ilGenerator.DeclareLocal(typeof(IMemorizedPosition));
                        seekPositions[(int)field.Index + 1] = position;
                        MemorizeCurrentPosition(ilGenerator, pushReader, position);
                    }
                    lastPKIndex = (int)field.Index;
                }
                else
                {
                    var f = ClientRelationVersionInfo.GetSecondaryKeyField((int)field.Index);
                    f.Handler.Skip(ilGenerator, pushReader);
                }

                processedFieldCount++;
            }
            ilGenerator.Ret();
            return method.Create();
        }

        MemorizedPositionWithLength SkipWithMemorizing(IILGen ilGenerator, Action<IILGen> pushReader, IFieldHandler handler, IILLocal tempPosition)
        {
            var memoPos = ilGenerator.DeclareLocal(typeof(IMemorizedPosition));
            var memoLen = ilGenerator.DeclareLocal(typeof(int));
            var position = new MemorizedPositionWithLength { Pos = memoPos, Length = memoLen };
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

        void CopyFromMemorizedPosition(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushWriter, MemorizedPositionWithLength memo)
        {
            ilGenerator
                .Do(pushWriter) //[W]
                .Do(pushReader) //[W,VR]
                .Ldloc(memo.Length) //[W, VR, readLen]
                .Ldloc(memo.Pos) //[W, VR, readLen, Memorize]
                .Callvirt(() => default(IMemorizedPosition).Restore()) //[W, VR]
                .Call(() => default(ByteArrayReader).ReadByteArrayRaw(0)) //[W, byte[]]
                .Call(() => default(AbstractBufferedWriter).WriteByteArrayRaw(null)); //[]
        }

        bool NeedMemorizeCurrentPosition(int currentFieldIndex, IList<FieldId> skFields)
        {
            if (currentFieldIndex + 1 == skFields.Count)
                return false;
            var pkIndex = skFields[currentFieldIndex].Index;
            var nextPkIndex = skFields[currentFieldIndex + 1].Index;
            return pkIndex > nextPkIndex;
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
            GetPrimaryKeysLoader(ClientTypeVersion)(tr, keyReader, obj);
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

        void SaveKeyFieldFromArgument(IILGen ilGenerator, TableFieldInfo field, ushort parameterId, IILLocal writerLoc)
        {
            field.Handler.Save(ilGenerator,
                il => il.Ldloc(writerLoc),
                il => il.Ldarg(parameterId));
        }

        void SaveKeyFieldFromApartField(IILGen ilGenerator, TableFieldInfo field, FieldBuilder backingField, IILLocal writerLoc)
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

        internal void SaveKeyBytesAndCallMethod(IILGen ilGenerator, Type relationDBManipulatorType, string methodName,
            ParameterInfo[] methodParameters, Type methodReturnType,
            IDictionary<string, FieldBuilder> apartFields)
        {
            var writerLoc = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
            ilGenerator.Newobj(() => new ByteBufferWriter());
            ilGenerator.Stloc(writerLoc);
            Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);

            //arg0 = this = manipulator
            if (methodName.StartsWith("RemoveById") || methodName.StartsWith("FindById"))
            {
                //ByteBufferWriter.WriteVUInt32(RelationInfo.Id);
                WriteIdIl(ilGenerator, pushWriter, (int)Id);
                var primaryKeyFields = ClientRelationVersionInfo.GetPrimaryKeyFields();

                var idx = SaveMethodParameters(ilGenerator, methodName, methodParameters, methodParameters.Length,
                    apartFields, primaryKeyFields, writerLoc);
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
                bool allowDefault = false;
                var skName = methodName.Substring(6);
                if (skName.EndsWith("OrDefault"))
                {
                    skName = skName.Substring(0, skName.Length - 9);
                    allowDefault = true;
                }

                WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsSKPrefix);
                var skIndex = ClientRelationVersionInfo.GetSecondaryKeyIndex(skName);
                //ByteBuffered.WriteVUInt32(RelationInfo.Id);
                WriteIdIl(ilGenerator, pushWriter, (int)Id);
                //ByteBuffered.WriteVUInt32(skIndex);
                WriteIdIl(ilGenerator, pushWriter, (int)skIndex);

                var secondaryKeyFields = ClientRelationVersionInfo.GetSecondaryKeyFields(skIndex);
                SaveMethodParameters(ilGenerator, methodName, methodParameters, methodParameters.Length,
                    apartFields, secondaryKeyFields, writerLoc);

                //call public T FindBySecondaryKeyOrDefault(uint secondaryKeyIndex, ByteBuffer secKeyBytes, bool throwWhenNotFound)
                ilGenerator.Ldarg(0); //manipulator
                ilGenerator.LdcI4((int)skIndex);
                ilGenerator.LdcI4(methodParameters.Length);
                //call byteBuffer.data
                var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
                ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
                if (methodReturnType.IsGenericType && methodReturnType.GetGenericTypeDefinition() == typeof(IEnumerator<>) &&
                              methodReturnType.GetGenericArguments()[0] == ClientType)
                {
                    ilGenerator.Callvirt(relationDBManipulatorType.GetMethod("FindBySecondaryKey"));
                }
                else
                {
                    ilGenerator.LdcI4(allowDefault ? 0 : 1); //? should throw
                    ilGenerator.Callvirt(relationDBManipulatorType.GetMethod("FindBySecondaryKeyOrDefault"));
                }
            }
            else if (methodName == "ListById")
            {
                WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsPKPrefix);

                //ByteBufferWriter.WriteVUInt32(RelationInfo.Id);
                WriteIdIl(ilGenerator, pushWriter, (int)Id);
                var primaryKeyFields = ClientRelationVersionInfo.GetPrimaryKeyFields();

                var paramsCount = SaveMethodParameters(ilGenerator, methodName, methodParameters, methodParameters.Length,
                    apartFields, primaryKeyFields, writerLoc);

                //call manipulator.GetEnumerator(tr, byteBuffer)
                ilGenerator
                    .Ldarg(0); //manipulator
                //call byteBuffer.data
                var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
                ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
                ilGenerator.LdcI4(paramsCount + apartFields.Count);
                ilGenerator.Callvirt(relationDBManipulatorType.GetMethod(methodName));
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        ushort SaveMethodParameters(IILGen ilGenerator, string methodName,
                                    ParameterInfo[] methodParameters, int paramCount,
                                    IDictionary<string, FieldBuilder> apartFields,
                                    IEnumerable<TableFieldInfo> fields, IILLocal writerLoc)
        {
            ushort idx = 0;
            foreach (var field in fields)
            {
                FieldBuilder backingField;
                if (apartFields.TryGetValue(field.Name, out backingField))
                {
                    SaveKeyFieldFromApartField(ilGenerator, field, backingField, writerLoc);
                    continue;
                }
                if (idx == paramCount)
                {
                    break;
                }
                var par = methodParameters[idx++];
                if (string.Compare(field.Name, par.Name.ToLower(), StringComparison.OrdinalIgnoreCase) != 0)
                {
                    throw new BTDBException($"Parameter and key mismatch in {methodName}, {field.Name}!={par.Name}.");
                }
                SaveKeyFieldFromArgument(ilGenerator, field, idx, writerLoc);
            }
            return idx;
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

        internal void FillBufferWhenNotIgnoredKeyPropositionIl(ushort advEnumParamOrder, TableFieldInfo field, IILLocal emptyBufferLoc,
                                                               FieldInfo instField, IILGen ilGenerator)
        {
            //stack contains KeyProposition
            var ignoreLabel = ilGenerator.DefineLabel(instField + "_ignore");
            var doneLabel = ilGenerator.DefineLabel(instField + "_done");
            var writerLoc = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
            ilGenerator
                .Dup()
                .LdcI4((int)KeyProposition.Ignored)
                .BeqS(ignoreLabel)
                .Newobj(() => new ByteBufferWriter())
                .Stloc(writerLoc);
            field.Handler.Save(ilGenerator,
                il => il.Ldloc(writerLoc),
                il => il.Ldarg(advEnumParamOrder).Ldfld(instField));
            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
            ilGenerator
                .Br(doneLabel)
                .Mark(ignoreLabel)
                .Ldloc(emptyBufferLoc)
                .Mark(doneLabel);
        }

        public void SaveListPrefixBytes(uint secondaryKeyIndex, IILGen ilGenerator, string methodName, ParameterInfo[] methodParameters,
            IDictionary<string, FieldBuilder> apartFields)
        {
            var writerLoc = ilGenerator.DeclareLocal(typeof(ByteBufferWriter));
            ilGenerator
                .Newobj(() => new ByteBufferWriter())
                .Stloc(writerLoc);

            Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);

            WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsSKPrefix);
            //ByteBuffered.WriteVUInt32(RelationInfo.Id);
            WriteIdIl(ilGenerator, pushWriter, (int)Id);
            //ByteBuffered.WriteVUInt32(skIndex);
            WriteIdIl(ilGenerator, pushWriter, (int)secondaryKeyIndex);

            var secondaryKeyFields = ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
            var paramCount = methodParameters.Length - 1; //last param is key proposition
            SaveMethodParameters(ilGenerator, methodName, methodParameters, paramCount, apartFields,
                secondaryKeyFields, writerLoc);

            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
        }

        public void SavePKListPrefixBytes(IILGen ilGenerator, string methodName, ParameterInfo[] methodParameters,
                                         IDictionary<string, FieldBuilder> apartFields)
        {
            var writerLoc = ilGenerator.DeclareLocal(typeof(ByteBufferWriter));
            ilGenerator
                .Newobj(() => new ByteBufferWriter())
                .Stloc(writerLoc);

            Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);

            WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsPKPrefix);
            //ByteBuffered.WriteVUInt32(RelationInfo.Id);
            WriteIdIl(ilGenerator, pushWriter, (int)Id);

            var keyFields = ClientRelationVersionInfo.GetPrimaryKeyFields();
            var paramCount = methodParameters.Length - 1; //last param is key proposition
            SaveMethodParameters(ilGenerator, methodName, methodParameters, paramCount, apartFields,
                keyFields, writerLoc);

            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
        }

        public int ModificationCounter => _modificationCounter;

        public void MarkModification()
        {
            _modificationCounter++;
        }

        public void CheckModifiedDuringEnum(int prevModification)
        {
            if (prevModification != _modificationCounter)
                throw new InvalidOperationException("Relation modified during iteration.");
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
}
