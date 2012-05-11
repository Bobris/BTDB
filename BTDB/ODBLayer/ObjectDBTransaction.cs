using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    internal class ObjectDBTransaction : IObjectDBTransaction, IInternalObjectDBTransaction
    {
        readonly ObjectDB _owner;
        IKeyValueDBTransaction _keyValueTr;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector = new KeyValueDBTransactionProtector();
        readonly ConcurrentDictionary<ulong, WeakReference> _objCache = new ConcurrentDictionary<ulong, WeakReference>();
        readonly ConditionalWeakTable<object, DBObjectMetadata> _objMetadata = new ConditionalWeakTable<object, DBObjectMetadata>();
        int _lastGCIndex;
        readonly ConcurrentDictionary<ulong, object> _dirtyObjSet = new ConcurrentDictionary<ulong, object>();
        readonly ConcurrentDictionary<TableInfo, bool> _updatedTables = new ConcurrentDictionary<TableInfo, bool>();
        long _lastDictId;

        public ObjectDBTransaction(ObjectDB owner, IKeyValueDBTransaction keyValueTr)
        {
            _owner = owner;
            _keyValueTr = keyValueTr;
            _lastDictId = (long)_owner.LastDictId;
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
            if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion || tableInfo.NeedStoreSingletonOid)
            {
                _updatedTables.GetOrAdd(tableInfo, PersistTableInfo);
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
                    WeakReference weakObj;
                    if (_objCache.TryGetValue(oid, out weakObj))
                    {
                        var o = weakObj.Target;
                        if (o != null)
                        {
                            if (type == null || type.IsAssignableFrom(o.GetType()))
                            {
                                _keyValueTrProtector.Stop(ref taken);
                                yield return o;
                                continue;
                            }
                            continue;
                        }
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
                if (type != null && !type.IsAssignableFrom(obj.GetType())) continue;
                yield return obj;
            }
        }

        object ReadObjFinish(ulong oid, TableInfo tableInfo, ByteArrayReader reader)
        {
            var tableVersion = reader.ReadVUInt32();
            var metadata = new DBObjectMetadata(oid, DBObjectState.Read);
            var obj = tableInfo.Creator(this, metadata);
            CompactObjCacheIfNeeded();
            _objCache.TryAdd(oid, new WeakReference(obj));
            _objMetadata.Add(obj, metadata);
            tableInfo.GetLoader(tableVersion)(this, metadata, reader, obj);
            return obj;
        }

        void CompactObjCacheIfNeeded()
        {
            var gcIndex = GC.CollectionCount(0);
            if (_lastGCIndex == gcIndex) return;
            _lastGCIndex = gcIndex;
            CompactObjCache();
        }

        void CompactObjCache()
        {
            foreach (var pair in _objCache)
            {
                if (!pair.Value.IsAlive)
                {
                    _objCache.TryRemove(pair.Key);
                }
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
            WeakReference weakObj;
            if (_objCache.TryGetValue(oid, out weakObj))
            {
                var o = weakObj.Target;
                if (o != null)
                {
                    return o;
                }
            }
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
            if (!_objMetadata.TryGetValue(obj, out meta)) return 0;
            return meta.Id;
        }

        public object Singleton(Type type)
        {
            var tableInfo = AutoRegisterType(type);
            lock (tableInfo.SingletonLock)
            {
                tableInfo.EnsureClientTypeVersion();
                var oid = tableInfo.SingletonOid;
                var obj = Get(oid);
                if (obj != null)
                {
                    if (!type.IsAssignableFrom(obj.GetType()))
                    {
                        throw new BTDBException(string.Format("Internal error oid {0} does not belong to {1}", oid, tableInfo.Name));
                    }
                    return obj;
                }
                _updatedTables.TryRemove(tableInfo);
                var metadata = new DBObjectMetadata(oid, DBObjectState.Dirty);
                obj = tableInfo.Creator(this, metadata);
                tableInfo.Initializer(this, metadata, obj);
                CompactObjCacheIfNeeded();
                _objCache.TryAdd(oid, new WeakReference(obj));
                _dirtyObjSet.TryAdd(oid, obj);
                _objMetadata.Add(obj, metadata);
                return obj;
            }
        }

        public T Singleton<T>() where T : class
        {
            return (T)Singleton(typeof(T));
        }

        public ulong Store(object @object)
        {
            var ti = AutoRegisterType(@object.GetType());
            ti.EnsureClientTypeVersion();
            DBObjectMetadata metadata;
            if (_objMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0)
                {
                    metadata.Id = _owner.AllocateNewOid();
                    CompactObjCacheIfNeeded();
                    _objCache.TryAdd(metadata.Id, new WeakReference(@object));
                }
                metadata.State = DBObjectState.Dirty;
                _dirtyObjSet.TryAdd(metadata.Id, @object);
                return metadata.Id;
            }
            return RegisterNewObject(@object);
        }

        public ulong StoreAndFlush(object @object)
        {
            var ti = AutoRegisterType(@object.GetType());
            ti.EnsureClientTypeVersion();
            DBObjectMetadata metadata;
            if (_objMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0)
                {
                    metadata.Id = _owner.AllocateNewOid();
                    CompactObjCacheIfNeeded();
                    _objCache.TryAdd(metadata.Id, new WeakReference(@object));
                }
                StoreObject(@object);
                metadata.State = DBObjectState.Read;
                return metadata.Id;
            }
            var id = _owner.AllocateNewOid();
            CompactObjCacheIfNeeded();
            _objMetadata.Add(@object, new DBObjectMetadata(id, DBObjectState.Read));
            _objCache.TryAdd(id, new WeakReference(@object));
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
            if (_objMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return 0;
                return metadata.Id;
            }
            return ti.StoredInline ? ulong.MaxValue : RegisterNewObject(@object);
        }

        ulong RegisterNewObject(object obj)
        {
            CompactObjCacheIfNeeded();
            var id = _owner.AllocateNewOid();
            _objMetadata.Add(obj, new DBObjectMetadata(id, DBObjectState.Dirty));
            _objCache.TryAdd(id, new WeakReference(obj));
            _dirtyObjSet.TryAdd(id, obj);
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
            AutoRegisterType(@object.GetType());
            DBObjectMetadata metadata;
            if (!_objMetadata.TryGetValue(@object, out metadata))
            {
                _objMetadata.Add(@object, new DBObjectMetadata(0, DBObjectState.Deleted));
                return;
            }
            if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return;
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
            _objCache.TryRemove(metadata.Id);
            _dirtyObjSet.TryRemove(metadata.Id);
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
                foreach (var o in _dirtyObjSet)
                {
                    StoreObject(o.Value);
                }
                _owner.CommitLastDictId((ulong)_lastDictId, _keyValueTr);
                _keyValueTr.Commit();
                foreach (var updatedTable in _updatedTables)
                {
                    updatedTable.Key.LastPersistedVersion = updatedTable.Key.ClientTypeVersion;
                    updatedTable.Key.ResetNeedStoreSingletonOid();
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
            DBObjectMetadata metadata;
            _objMetadata.TryGetValue(o, out metadata);
            var writer = new ByteBufferWriter();
            writer.WriteVUInt32(tableInfo.Id);
            writer.WriteVUInt32(tableInfo.ClientTypeVersion);
            tableInfo.Saver(this, metadata, writer, o);
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

        bool PersistTableInfo(TableInfo tableInfo)
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
                    _keyValueTr.CreateOrUpdateKeyValue(BuildKeyFromOid(tableInfo.Id), BuildKeyFromOid(tableInfo.SingletonOid));
                }
                return true;
            }
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
            }
        }
    }
}
