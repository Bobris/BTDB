using System;
using System.Collections;
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
    internal class ObjectDBTransaction : IObjectDBTransaction, IInternalObjectDBTransaction
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

        public IObjectDB Owner
        {
            get { return _owner; }
        }

        public IKeyValueDBTransaction KeyValueDBTransaction
        {
            get { return _keyValueTr; }
        }

        public KeyValueDBTransactionProtector TransactionProtector
        {
            get { return _keyValueTrProtector; }
        }

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
            if (tableInfo == null) throw new BTDBException(string.Format("Unknown TypeId {0} of inline object", tableId));
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
            var taken = false;
            ulong oid = 0;
            ulong finalOid = _owner.GetLastAllocatedOid();
            long prevProtectionCounter = 0;
            try
            {
                while (true)
                {
                    if (!taken) _keyValueTrProtector.Start(ref taken);
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
                            byte[] key = BuildKeyFromOid(oid);
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
                    }
                    oid = ReadOidFromCurrentKeyInTransaction();
                    var o = GetObjFromObjCacheByOid(oid);
                    if (o != null)
                    {
                        if (type == null || type.IsInstanceOfType(o))
                        {
                            _keyValueTrProtector.Stop(ref taken);
                            yield return o;
                        }
                        continue;
                    }
                    TableInfo tableInfo;
                    var reader = ReadObjStart(oid, out tableInfo);
                    if (type != null && !type.IsAssignableFrom(tableInfo.ClientType)) continue;
                    _keyValueTrProtector.Stop(ref taken);
                    object obj = ReadObjFinish(oid, tableInfo, reader);
                    yield return obj;
                }
            }
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
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
                object obj = dObjPair.Value;
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
            if (tableInfo == null) throw new BTDBException(string.Format("Unknown TypeId {0} of Oid {1}", tableId, oid));
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
            bool taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                if (!_keyValueTr.FindExactKey(BuildKeyFromOid(oid)))
                {
                    return null;
                }
                TableInfo tableInfo;
                var reader = ReadObjStart(oid, out tableInfo);
                _keyValueTrProtector.Stop(ref taken);
                return ReadObjFinish(oid, tableInfo, reader);
            }
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
            }
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
                    bool taken = false;
                    try
                    {
                        _keyValueTrProtector.Start(ref taken);
                        _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                        if (_keyValueTr.FindExactKey(BuildKeyFromOid(oid)))
                        {
                            content = _keyValueTr.GetValueAsByteArray();
                            tableInfo.CacheSingletonContent(_transactionNumber, content);
                        }
                    }
                    finally
                    {
                        if (taken) _keyValueTrProtector.Stop();
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
                    throw new BTDBException(string.Format("Internal error oid {0} does not belong to {1}", oid, tableInfo.Name));
                }
                return obj;
            }

            if (_updatedTables != null) _updatedTables.Remove(tableInfo);
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
                throw new BTDBException(string.Format("Object {0} should be stored inline and not directly", ti.Name));
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
                    throw new BTDBException(string.Format("Type {0} is not registered", tableInfo.Name));
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
            if (@object == null) throw new ArgumentNullException("object");
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
            var taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                if (_keyValueTr.FindExactKey(BuildKeyFromOid(metadata.Id)))
                    _keyValueTr.EraseCurrent();
            }
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
            }
            tableInfo.CacheSingletonContent(_transactionNumber + 1, null);
            if (_objSmallCache != null)
            {
                _objSmallCache.Remove(metadata.Id);
            }
            else if (_objBigCache != null)
            {
                _objBigCache.Remove(metadata.Id);
            }
            if (_dirtyObjSet != null) _dirtyObjSet.Remove(metadata.Id);
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
            if (_dirtyObjSet != null) _dirtyObjSet.Remove(oid);
            var taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                if (_keyValueTr.FindExactKey(BuildKeyFromOid(oid)))
                    _keyValueTr.EraseCurrent();
            }
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
            }
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
                if (_updatedTables != null) foreach (var updatedTable in _updatedTables)
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
            var tableInfo = _owner.TablesInfo.FindByType(o.GetType());
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
            var shouldStop = false;
            try
            {
                _keyValueTrProtector.Start(ref shouldStop);
                _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                _keyValueTr.CreateOrUpdateKeyValue(BuildKeyFromOid(metadata.Id), writer.Data.ToByteArray());
            }
            finally
            {
                if (shouldStop) _keyValueTrProtector.Stop();
            }
        }

        void PersistTableInfo(TableInfo tableInfo)
        {
            var taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
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
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
            }
        }
    }
}
