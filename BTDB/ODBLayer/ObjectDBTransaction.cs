using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class ObjectDBTransaction : IInternalObjectDBTransaction
    {
        readonly ObjectDB _owner;
        IKeyValueDBTransaction _keyValueTr;
        readonly bool _readOnly;
        readonly long _transactionNumber;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector = new KeyValueDBTransactionProtector();

        Dictionary<ulong, object> _objSmallCache;
        Dictionary<object, DBObjectMetadata> _objSmallMetadata;
        Dictionary<ulong, WeakReference> _objBigCache;
        ConditionalWeakTable<object, DBObjectMetadata> _objBigMetadata;
        int _lastGCIndex;

        Dictionary<ulong, object> _dirtyObjSet;
        HashSet<TableInfo> _updatedTables;
        long _lastDictId;

        public ObjectDBTransaction(ObjectDB owner, IKeyValueDBTransaction keyValueTr, bool readOnly)
        {
            _owner = owner;
            _keyValueTr = keyValueTr;
            _readOnly = readOnly;
            _lastDictId = (long)_owner.LastDictId;
            _transactionNumber = keyValueTr.GetTransactionNumber();
        }

        public void Dispose()
        {
            if (_keyValueTr == null) return;
            _keyValueTr.Dispose();
            _keyValueTr = null;
        }

        public IObjectDB Owner => _owner;

        public IKeyValueDBTransaction KeyValueDBTransaction => _keyValueTr;

        public KeyValueDBTransactionProtector TransactionProtector => _keyValueTrProtector;

        public ulong AllocateDictionaryId()
        {
            return (ulong)(Interlocked.Increment(ref _lastDictId) - 1);
        }

        public object ReadInlineObject(IReaderCtx readerCtx)
        {
            var reader = readerCtx.Reader();
            var tableId = reader.ReadVUInt32();
            var tableVersion = reader.ReadVUInt32();
            var tableInfo = _owner.TablesInfo.FindById(tableId);
            if (tableInfo == null) throw new BTDBException($"Unknown TypeId {tableId} of inline object");
            EnsureClientTypeNotNull(tableInfo);
            var obj = tableInfo.Creator(this, null);
            readerCtx.RegisterObject(obj);
            tableInfo.GetLoader(tableVersion)(this, null, reader, obj);
            readerCtx.ReadObjectDone();
            return obj;
        }

        public void WriteInlineObject(object @object, IWriterCtx writerCtx)
        {
            var ti = GetTableInfoFromType(@object.GetType());
            EnsureClientTypeNotNull(ti);
            IfNeededPersistTableInfo(ti);
            var writer = writerCtx.Writer();
            writer.WriteVUInt32(ti.Id);
            writer.WriteVUInt32(ti.ClientTypeVersion);
            ti.Saver(this, null, writer, @object);
        }

        void IfNeededPersistTableInfo(TableInfo tableInfo)
        {
            if (_readOnly) return;
            if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion || tableInfo.NeedStoreSingletonOid)
            {
                if (_updatedTables == null) _updatedTables = new HashSet<TableInfo>();
                if (_updatedTables.Add(tableInfo))
                {
                    PersistTableInfo(tableInfo);
                }
            }
        }

        public IEnumerable<T> Enumerate<T>() where T : class
        {
            return Enumerate(typeof(T)).Cast<T>();
        }

        public IEnumerable<object> Enumerate(Type type)
        {
            if (type == typeof(object)) type = null;
            else if (type != null) AutoRegisterType(type);
            ulong oid = 0;
            ulong finalOid = _owner.GetLastAllocatedOid();
            long prevProtectionCounter = 0;
            while (true)
            {
                _keyValueTrProtector.Start();
                if (oid == 0)
                {
                    prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                    _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                    if (!_keyValueTr.FindFirstKey()) break;
                }
                else
                {
                    if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                    {
                        _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                        oid++;
                        var key = BuildKeyFromOid(oid);
                        var result = _keyValueTr.Find(ByteBuffer.NewSync(key));
                        if (result == FindResult.Previous)
                        {
                            if (!_keyValueTr.FindNextKey())
                            {
                                result = FindResult.NotFound;
                            }
                        }
                        if (result == FindResult.NotFound)
                        {
                            oid--;
                            break;
                        }
                    }
                    else
                    {
                        if (!_keyValueTr.FindNextKey()) break;
                    }
                    prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                }
                oid = ReadOidFromCurrentKeyInTransaction();
                var o = GetObjFromObjCacheByOid(oid);
                if (o != null)
                {
                    if (type == null || type.IsInstanceOfType(o))
                    {
                        yield return o;
                    }
                    continue;
                }
                TableInfo tableInfo;
                var reader = ReadObjStart(oid, out tableInfo);
                if (type != null && !type.IsAssignableFrom(tableInfo.ClientType)) continue;
                var obj = ReadObjFinish(oid, tableInfo, reader);
                yield return obj;
            }
            if (_dirtyObjSet == null) yield break;
            var dirtyObjsToEnum = _dirtyObjSet.Where(p => p.Key > oid && p.Key <= finalOid).ToList();
            dirtyObjsToEnum.Sort((p1, p2) =>
                                     {
                                         if (p1.Key < p2.Key) return -1;
                                         if (p1.Key > p2.Key) return 1;
                                         return 0;
                                     });
            foreach (var dObjPair in dirtyObjsToEnum)
            {
                var obj = dObjPair.Value;
                if (type != null && !type.IsInstanceOfType(obj)) continue;
                yield return obj;
            }
        }

        object GetObjFromObjCacheByOid(ulong oid)
        {
            if (_objSmallCache != null)
            {
                object result;
                return !_objSmallCache.TryGetValue(oid, out result) ? null : result;
            }
            if (_objBigCache != null)
            {
                WeakReference weakObj;
                if (_objBigCache.TryGetValue(oid, out weakObj))
                {
                    return weakObj.Target;
                }
            }
            return null;
        }

        object ReadObjFinish(ulong oid, TableInfo tableInfo, ByteArrayReader reader)
        {
            var tableVersion = reader.ReadVUInt32();
            var metadata = new DBObjectMetadata(oid, DBObjectState.Read);
            var obj = tableInfo.Creator(this, metadata);
            AddToObjCache(oid, obj, metadata);
            tableInfo.GetLoader(tableVersion)(this, metadata, reader, obj);
            return obj;
        }

        void AddToObjCache(ulong oid, object obj, DBObjectMetadata metadata)
        {
            if (_objBigCache != null)
            {
                CompactObjCacheIfNeeded();
                _objBigCache[oid] = new WeakReference(obj);
                _objBigMetadata.Add(obj, metadata);
                return;
            }
            if (_objSmallCache == null)
            {
                _objSmallCache = new Dictionary<ulong, object>();
                _objSmallMetadata = new Dictionary<object, DBObjectMetadata>(ReferenceEqualityComparer<object>.Instance);
            }
            else if (_objSmallCache.Count > 30)
            {
                _objBigCache = new Dictionary<ulong, WeakReference>();
                _objBigMetadata = new ConditionalWeakTable<object, DBObjectMetadata>();
                foreach (var pair in _objSmallCache)
                {
                    _objBigCache.Add(pair.Key, new WeakReference(pair.Value));
                }
                _objSmallCache = null;
                foreach (var pair in _objSmallMetadata)
                {
                    _objBigMetadata.Add(pair.Key, pair.Value);
                }
                _objSmallMetadata = null;
                _objBigCache.Add(oid, new WeakReference(obj));
                _objBigMetadata.Add(obj, metadata);
                return;
            }
            _objSmallCache.Add(oid, obj);
            _objSmallMetadata.Add(obj, metadata);
        }

        void CompactObjCacheIfNeeded()
        {
            if (_objBigCache == null) return;
            var gcIndex = GC.CollectionCount(0);
            if (_lastGCIndex == gcIndex) return;
            _lastGCIndex = gcIndex;
            CompactObjCache();
        }

        void CompactObjCache()
        {
            var toRemove = new List<ulong>();
            foreach (var pair in _objBigCache)
            {
                if (!pair.Value.IsAlive)
                {
                    toRemove.Add(pair.Key);
                }
            }
            foreach (var k in toRemove)
            {
                _objBigCache.Remove(k);
            }
        }

        ByteArrayReader ReadObjStart(ulong oid, out TableInfo tableInfo)
        {
            var reader = new ByteArrayReader(_keyValueTr.GetValueAsByteArray());
            var tableId = reader.ReadVUInt32();
            tableInfo = _owner.TablesInfo.FindById(tableId);
            if (tableInfo == null) throw new BTDBException($"Unknown TypeId {tableId} of Oid {oid}");
            EnsureClientTypeNotNull(tableInfo);
            return reader;
        }

        public object Get(ulong oid)
        {
            var o = GetObjFromObjCacheByOid(oid);
            if (o != null)
            {
                return o;
            }
            return GetDirectlyFromStorage(oid);
        }

        object GetDirectlyFromStorage(ulong oid)
        {
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            if (!_keyValueTr.FindExactKey(BuildKeyFromOid(oid)))
            {
                return null;
            }
            TableInfo tableInfo;
            var reader = ReadObjStart(oid, out tableInfo);
            return ReadObjFinish(oid, tableInfo, reader);
        }

        public ulong GetOid(object obj)
        {
            if (obj == null) return 0;
            DBObjectMetadata meta;
            if (_objSmallMetadata != null)
            {
                return !_objSmallMetadata.TryGetValue(obj, out meta) ? 0 : meta.Id;
            }
            if (_objBigMetadata != null)
            {
                return !_objBigMetadata.TryGetValue(obj, out meta) ? 0 : meta.Id;
            }
            return 0;
        }

        public KeyValuePair<uint, uint> GetStorageSize(ulong oid)
        {
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            if (!_keyValueTr.FindExactKey(BuildKeyFromOid(oid)))
            {
                return new KeyValuePair<uint, uint>(0, 0);
            }
            var res = _keyValueTr.GetStorageSizeOfCurrentKey();
            return res;
        }

        public IEnumerable<Type> EnumerateSingletonTypes()
        {
            foreach (var tableInfo in _owner.TablesInfo.EnumerateTableInfos().ToArray())
            {
                var oid = tableInfo.LazySingletonOid;
                if (oid == 0) continue;
                try
                {
                    EnsureClientTypeNotNull(tableInfo);
                }
                catch (BTDBException)
                {
                    // Ignore imposibility to create type
                }
                var type = tableInfo.ClientType;
                if (type != null)
                    yield return type;
            }
        }

        public object Singleton(Type type)
        {
            var tableInfo = AutoRegisterType(type);
            tableInfo.EnsureClientTypeVersion();
            var oid = (ulong)tableInfo.SingletonOid;
            var obj = GetObjFromObjCacheByOid(oid);
            if (obj == null)
            {
                var content = tableInfo.SingletonContent(_transactionNumber);
                if (content == null)
                {
                    _keyValueTrProtector.Start();
                    _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                    if (_keyValueTr.FindExactKey(BuildKeyFromOid(oid)))
                    {
                        content = _keyValueTr.GetValueAsByteArray();
                        tableInfo.CacheSingletonContent(_transactionNumber, content);
                    }
                }
                if (content != null)
                {
                    var reader = new ByteArrayReader(content);
                    reader.SkipVUInt32();
                    obj = ReadObjFinish(oid, tableInfo, reader);
                }
            }
            if (obj != null)
            {
                if (!type.IsInstanceOfType(obj))
                {
                    throw new BTDBException($"Internal error oid {oid} does not belong to {tableInfo.Name}");
                }
                return obj;
            }

            _updatedTables?.Remove(tableInfo);
            var metadata = new DBObjectMetadata(oid, DBObjectState.Dirty);
            obj = tableInfo.Creator(this, metadata);
            tableInfo.Initializer(this, metadata, obj);
            AddToObjCache(oid, obj, metadata);
            AddToDirtySet(oid, obj);
            return obj;
        }

        void AddToDirtySet(ulong oid, object obj)
        {
            if (_dirtyObjSet == null) _dirtyObjSet = new Dictionary<ulong, object>();
            _dirtyObjSet.Add(oid, obj);
        }

        public T Singleton<T>() where T : class
        {
            return (T)Singleton(typeof(T));
        }

        public object New(Type type)
        {
            var tableInfo = AutoRegisterType(type);
            tableInfo.EnsureClientTypeVersion();
            var oid = 0ul;
            if (!tableInfo.StoredInline) oid = _owner.AllocateNewOid();
            var metadata = new DBObjectMetadata(oid, DBObjectState.Dirty);
            var obj = tableInfo.Creator(this, metadata);
            tableInfo.Initializer(this, metadata, obj);
            if (oid != 0)
            {
                AddToObjCache(oid, obj, metadata);
                AddToDirtySet(oid, obj);
            }
            return obj;
        }

        public T New<T>() where T : class
        {
            return (T)New(typeof(T));
        }

        public ulong Store(object @object)
        {
            var ti = AutoRegisterType(@object.GetType());
            CheckStoredInline(ti);
            ti.EnsureClientTypeVersion();
            DBObjectMetadata metadata;
            if (_objSmallMetadata != null)
            {
                if (_objSmallMetadata.TryGetValue(@object, out metadata))
                {
                    if (metadata.Id == 0)
                    {
                        metadata.Id = _owner.AllocateNewOid();
                        _objSmallCache.Add(metadata.Id, @object);
                    }
                    if (metadata.State != DBObjectState.Dirty)
                    {
                        metadata.State = DBObjectState.Dirty;
                        AddToDirtySet(metadata.Id, @object);
                    }
                    return metadata.Id;
                }
            }
            else if (_objBigMetadata != null)
            {
                if (_objBigMetadata.TryGetValue(@object, out metadata))
                {
                    if (metadata.Id == 0)
                    {
                        metadata.Id = _owner.AllocateNewOid();
                        CompactObjCacheIfNeeded();
                        _objBigCache.Add(metadata.Id, new WeakReference(@object));
                    }
                    if (metadata.State != DBObjectState.Dirty)
                    {
                        metadata.State = DBObjectState.Dirty;
                        AddToDirtySet(metadata.Id, @object);
                    }
                    return metadata.Id;
                }
            }
            return RegisterNewObject(@object);
        }

        static void CheckStoredInline(TableInfo ti)
        {
            if (ti.StoredInline)
            {
                throw new BTDBException($"Object {ti.Name} should be stored inline and not directly");
            }
        }

        public ulong StoreAndFlush(object @object)
        {
            var ti = AutoRegisterType(@object.GetType());
            CheckStoredInline(ti);
            ti.EnsureClientTypeVersion();
            DBObjectMetadata metadata;
            if (_objSmallMetadata != null)
            {
                if (_objSmallMetadata.TryGetValue(@object, out metadata))
                {
                    if (metadata.Id == 0)
                    {
                        metadata.Id = _owner.AllocateNewOid();
                        _objSmallCache.Add(metadata.Id, @object);
                    }
                    StoreObject(@object);
                    metadata.State = DBObjectState.Read;
                    return metadata.Id;
                }
            }
            else if (_objBigMetadata != null)
            {
                if (_objBigMetadata.TryGetValue(@object, out metadata))
                {
                    if (metadata.Id == 0)
                    {
                        metadata.Id = _owner.AllocateNewOid();
                        CompactObjCacheIfNeeded();
                        _objBigCache.Add(metadata.Id, new WeakReference(@object));
                    }
                    StoreObject(@object);
                    metadata.State = DBObjectState.Read;
                    return metadata.Id;
                }
            }
            var id = _owner.AllocateNewOid();
            AddToObjCache(id, @object, new DBObjectMetadata(id, DBObjectState.Read));
            StoreObject(@object);
            return id;
        }

        public ulong StoreIfNotInlined(object @object, bool autoRegister)
        {
            TableInfo ti;
            if (autoRegister)
            {
                ti = AutoRegisterType(@object.GetType());
            }
            else
            {
                ti = GetTableInfoFromType(@object.GetType());
                if (ti == null) return ulong.MaxValue;
            }
            ti.EnsureClientTypeVersion();
            DBObjectMetadata metadata;
            if (_objSmallMetadata != null)
            {
                if (_objSmallMetadata.TryGetValue(@object, out metadata))
                {
                    if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return 0;
                    return metadata.Id;
                }
            }
            else if (_objBigMetadata != null)
            {
                if (_objBigMetadata.TryGetValue(@object, out metadata))
                {
                    if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return 0;
                    return metadata.Id;
                }
            }
            return ti.StoredInline ? ulong.MaxValue : RegisterNewObject(@object);
        }

        ulong RegisterNewObject(object obj)
        {
            var id = _owner.AllocateNewOid();
            AddToObjCache(id, obj, new DBObjectMetadata(id, DBObjectState.Dirty));
            AddToDirtySet(id, obj);
            return id;
        }

        void EnsureClientTypeNotNull(TableInfo tableInfo)
        {
            if (tableInfo.ClientType == null)
            {
                var typeByName = _owner.Type2NameRegistry.FindTypeByName(tableInfo.Name);
                if (typeByName != null)
                {
                    tableInfo.ClientType = typeByName;
                }
                else
                {
                    throw new BTDBException($"Type {tableInfo.Name} is not registered");
                }
            }
        }

        ulong ReadOidFromCurrentKeyInTransaction()
        {
            var key = _keyValueTr.GetKey();
            var bufOfs = key.Offset;
            var oid = PackUnpack.UnpackVUInt(key.Buffer, ref bufOfs);
            return oid;
        }

        static byte[] BuildKeyFromOid(ulong oid)
        {
            var key = new byte[PackUnpack.LengthVUInt(oid)];
            int ofs = 0;
            PackUnpack.PackVUInt(key, ref ofs, oid);
            return key;
        }

        TableInfo AutoRegisterType(Type type)
        {
            var ti = _owner.TablesInfo.FindByType(type);
            if (ti == null)
            {
                if (type.InheritsOrImplements(typeof(IEnumerable<>)))
                {
                    throw new InvalidOperationException("Cannot store " + type.ToSimpleName() + " type to DB directly.");
                }
                var name = _owner.Type2NameRegistry.FindNameByType(type) ?? _owner.RegisterType(type);
                ti = _owner.TablesInfo.LinkType2Name(type, name);
            }
            return ti;
        }

        TableInfo GetTableInfoFromType(Type type)
        {
            var ti = _owner.TablesInfo.FindByType(type);
            if (ti == null)
            {
                var name = _owner.Type2NameRegistry.FindNameByType(type);
                if (name == null) return null;
                ti = _owner.TablesInfo.LinkType2Name(type, name);
            }
            return ti;
        }

        public void Delete(object @object)
        {
            if (@object == null) throw new ArgumentNullException(nameof(@object));
            var indirect = @object as IIndirect;
            if (indirect != null)
            {
                if (indirect.Oid != 0)
                {
                    Delete(indirect.Oid);
                    return;
                }
                Delete(indirect.ValueAsObject);
                return;
            }
            var tableInfo = AutoRegisterType(@object.GetType());
            DBObjectMetadata metadata;
            if (_objSmallMetadata != null)
            {
                if (!_objSmallMetadata.TryGetValue(@object, out metadata))
                {
                    _objSmallMetadata.Add(@object, new DBObjectMetadata(0, DBObjectState.Deleted));
                    return;
                }
            }
            else if (_objBigMetadata != null)
            {
                if (!_objBigMetadata.TryGetValue(@object, out metadata))
                {
                    _objBigMetadata.Add(@object, new DBObjectMetadata(0, DBObjectState.Deleted));
                    return;
                }
            }
            else return;
            if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return;
            metadata.State = DBObjectState.Deleted;
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            if (_keyValueTr.FindExactKey(BuildKeyFromOid(metadata.Id)))
                _keyValueTr.EraseCurrent();
            tableInfo.CacheSingletonContent(_transactionNumber + 1, null);
            if (_objSmallCache != null)
            {
                _objSmallCache.Remove(metadata.Id);
            }
            else
            {
                _objBigCache?.Remove(metadata.Id);
            }
            _dirtyObjSet?.Remove(metadata.Id);
        }

        public void Delete(ulong oid)
        {
            object obj = null;
            if (_objSmallCache != null)
            {
                if (_objSmallCache.TryGetValue(oid, out obj))
                {
                    _objSmallCache.Remove(oid);
                }
            }
            else if (_objBigCache != null)
            {
                WeakReference weakobj;
                if (_objBigCache.TryGetValue(oid, out weakobj))
                {
                    obj = weakobj.Target;
                    _objBigCache.Remove(oid);
                }
            }
            _dirtyObjSet?.Remove(oid);
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            if (_keyValueTr.FindExactKey(BuildKeyFromOid(oid)))
                _keyValueTr.EraseCurrent();
            if (obj == null) return;
            DBObjectMetadata metadata = null;
            if (_objSmallMetadata != null)
            {
                if (!_objSmallMetadata.TryGetValue(obj, out metadata))
                {
                    return;
                }
            }
            else if (_objBigMetadata != null)
            {
                if (!_objBigMetadata.TryGetValue(obj, out metadata))
                {
                    return;
                }
            }
            if (metadata == null) return;
            metadata.State = DBObjectState.Deleted;
        }

        public void DeleteAll<T>() where T : class
        {
            DeleteAll(typeof(T));
        }

        public void DeleteAll(Type type)
        {
            foreach (var o in Enumerate(type))
            {
                Delete(o);
            }
        }

        public ulong GetCommitUlong() => _keyValueTr.GetCommitUlong();
        public void SetCommitUlong(ulong value) => _keyValueTr.SetCommitUlong(value);

        public void Commit()
        {
            try
            {
                while (_dirtyObjSet != null)
                {
                    var curObjsToStore = _dirtyObjSet;
                    _dirtyObjSet = null;
                    foreach (var o in curObjsToStore)
                    {
                        StoreObject(o.Value);
                    }
                }
                _owner.CommitLastDictId((ulong)_lastDictId, _keyValueTr);
                _keyValueTr.Commit();
                if (_updatedTables != null)
                    foreach (var updatedTable in _updatedTables)
                    {
                        updatedTable.LastPersistedVersion = updatedTable.ClientTypeVersion;
                        updatedTable.ResetNeedStoreSingletonOid();
                    }
            }
            finally
            {
                Dispose();
            }
        }

        void StoreObject(object o)
        {
            var type = o.GetType();
            if (!type.IsClass) throw new BTDBException("You can store only classes, not " + type.ToSimpleName());
            var tableInfo = _owner.TablesInfo.FindByType(type);
            IfNeededPersistTableInfo(tableInfo);
            DBObjectMetadata metadata = null;
            if (_objSmallMetadata != null)
            {
                _objSmallMetadata.TryGetValue(o, out metadata);
            }
            else if (_objBigMetadata != null)
            {
                _objBigMetadata.TryGetValue(o, out metadata);
            }
            if (metadata == null) throw new BTDBException("Metadata for object not found");
            if (metadata.State == DBObjectState.Deleted) return;
            var writer = new ByteBufferWriter();
            writer.WriteVUInt32(tableInfo.Id);
            writer.WriteVUInt32(tableInfo.ClientTypeVersion);
            tableInfo.Saver(this, metadata, writer, o);
            if (tableInfo.IsSingletonOid(metadata.Id))
            {
                tableInfo.CacheSingletonContent(_transactionNumber + 1, null);
            }
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            _keyValueTr.CreateOrUpdateKeyValue(BuildKeyFromOid(metadata.Id), writer.Data.ToByteArray());
        }

        void PersistTableInfo(TableInfo tableInfo)
        {
            _keyValueTrProtector.Start();
            if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion)
            {
                if (tableInfo.LastPersistedVersion <= 0)
                {
                    _keyValueTr.SetKeyPrefix(ObjectDB.TableNamesPrefix);
                    if (_keyValueTr.CreateKey(BuildKeyFromOid(tableInfo.Id)))
                    {
                        using (var writer = new KeyValueDBValueWriter(_keyValueTr))
                        {
                            writer.WriteString(tableInfo.Name);
                        }
                    }
                }
                _keyValueTr.SetKeyPrefix(ObjectDB.TableVersionsPrefix);
                if (_keyValueTr.CreateKey(TableInfo.BuildKeyForTableVersions(tableInfo.Id, tableInfo.ClientTypeVersion)))
                {
                    var tableVersionInfo = tableInfo.ClientTableVersionInfo;
                    using (var writer = new KeyValueDBValueWriter(_keyValueTr))
                    {
                        tableVersionInfo.Save(writer);
                    }
                }
            }
            if (tableInfo.NeedStoreSingletonOid)
            {
                _keyValueTr.SetKeyPrefix(ObjectDB.TableSingletonsPrefix);
                _keyValueTr.CreateOrUpdateKeyValue(BuildKeyFromOid(tableInfo.Id), BuildKeyFromOid((ulong)tableInfo.SingletonOid));
            }
        }

        public IRelationCreator<T> InitRelation<T>(string relationName)
        {
            var interfaceType = typeof(T);
            var relationInfo = _owner.RelationsInfo.CreateByName(KeyValueDBTransaction, relationName, interfaceType);
            relationInfo.EnsureClientTypeVersion();
            var relationDBManipulatorType = typeof(RelationDBManipulator<>).MakeGenericType(relationInfo.ClientType);

            var classImpl = ILBuilder.Instance.NewType("Relation" + relationName, typeof(object), new[] { interfaceType });
            var transactionField = classImpl.DefineField("transaction", typeof(IInternalObjectDBTransaction), System.Reflection.FieldAttributes.InitOnly | System.Reflection.FieldAttributes.Public);
            var manipulatorField = classImpl.DefineField("manipulator", relationDBManipulatorType, System.Reflection.FieldAttributes.InitOnly | System.Reflection.FieldAttributes.Public);
            var constructorMethod = classImpl.DefineConstructor(new[] { typeof(IObjectDBTransaction), relationDBManipulatorType });
            var il = constructorMethod.Generator;
            // super.ctor();
            il.Ldarg(0).Call(() => new object());
            // this.transaction = (IInternalObjectDBTransaction)arg0; 
            il.Ldarg(0).Ldarg(1).Castclass(typeof(IInternalObjectDBTransaction)).Stfld(transactionField)
            //this.manipulator = arg1; 
            .Ldarg(0).Ldarg(2).Stfld(manipulatorField)
            //return;
            .Ret();
            var methods = interfaceType.GetMethods();
            foreach (var method in methods)
            {
                var reqMethod = classImpl.DefineMethod(method.Name, method.ReturnType,
                    method.GetParameters().Select(pi => pi.ParameterType).ToArray(),
                    System.Reflection.MethodAttributes.Virtual | System.Reflection.MethodAttributes.Public);
                reqMethod.Generator
                    .Ldarg(0)
                    .Ldfld(manipulatorField)
                    .Ldarg(0)
                    .Ldfld(transactionField);
                int paramCount = method.GetParameters().Length;
                for (ushort i = 1; i <= paramCount; i++)
                    reqMethod.Generator.Ldarg(i);
                reqMethod.Generator.Callvirt(relationDBManipulatorType.GetMethod(method.Name))
                    .Ret();
                classImpl.DefineMethodOverride(reqMethod, method);
            }
            var classImplType = classImpl.CreateType();

            return BuildRelationCreatorInstance<T>(classImplType, relationName, relationInfo);
        }

        IRelationCreator<T> BuildRelationCreatorInstance<T>(Type classImplType, string relationName, RelationInfo relationInfo)
        {
            var interfaceType = typeof(IRelationCreator<T>);
            var relationDBManipulatorType = typeof(RelationDBManipulator<>).MakeGenericType(relationInfo.ClientType);

            var classImpl = ILBuilder.Instance.NewType("RelationBuilder" + relationName, typeof(object), new[] { interfaceType });
            var manipulatorField = classImpl.DefineField("manipulator", relationDBManipulatorType,
                System.Reflection.FieldAttributes.InitOnly | System.Reflection.FieldAttributes.Public);
            var constructorMethod = classImpl.DefineConstructor(new[] { relationDBManipulatorType });
            var il = constructorMethod.Generator;
            // super.ctor();
            il.Ldarg(0).Call(() => new object());
            //this.manipulator = arg0; 
            il.Ldarg(0).Ldarg(1).Stfld(manipulatorField)
            //return;
            .Ret();
            //method Create
            var methodBuilder = classImpl.DefineMethod("Create", typeof(T), new[] { typeof(IObjectDBTransaction) },
                System.Reflection.MethodAttributes.Virtual | System.Reflection.MethodAttributes.Public);
            var ilGenerator = methodBuilder.Generator;
            ilGenerator
                .Ldarg(1)
                .Ldarg(0)
                .Ldfld(manipulatorField)
                //new Relation$Name(IObjectDBTransaction, manipulator)
                .Newobj(classImplType.GetConstructors()[0])
                .Castclass(typeof(T))
                .Ret();
            classImpl.DefineMethodOverride(methodBuilder, interfaceType.GetMethod("Create"));

            var relationCreatorType = classImpl.CreateType();
            var manipulator = Activator.CreateInstance(relationDBManipulatorType, relationInfo);
            return (IRelationCreator<T>)relationCreatorType.GetConstructors()[0].Invoke(new[] { manipulator });
        }
    }
}
