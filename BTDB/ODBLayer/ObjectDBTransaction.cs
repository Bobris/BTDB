using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    internal class ObjectDBTransaction : IObjectDBTransaction
    {
        readonly ObjectDB _owner;
        IKeyValueDBTransaction _keyValueTr;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector = new KeyValueDBTransactionProtector();
        readonly ConcurrentDictionary<ulong, WeakReference> _objCache = new ConcurrentDictionary<ulong, WeakReference>();
        readonly ConditionalWeakTable<object, DBObjectMetadata> _objMetadata = new ConditionalWeakTable<object, DBObjectMetadata>();

        readonly ConcurrentDictionary<ulong, object> _dirtyObjSet = new ConcurrentDictionary<ulong, object>();
        readonly ConcurrentDictionary<TableInfo, bool> _updatedTables = new ConcurrentDictionary<TableInfo, bool>();

        public ObjectDBTransaction(ObjectDB owner, IKeyValueDBTransaction keyValueTr)
        {
            _owner = owner;
            _keyValueTr = keyValueTr;
        }

        public void Dispose()
        {
            if (_keyValueTr == null) return;
            _keyValueTr.Dispose();
            _keyValueTr = null;
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
                            if (_keyValueTr.FindKey(key, 0, key.Length, FindKeyStrategy.OnlyNext) == FindKeyResult.NotFound)
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
                    KeyValueDBValueReader reader = ReadObjStart(oid, out tableInfo);
                    if (type != null && !type.IsAssignableFrom(tableInfo.ClientType)) continue;
                    object obj = ReadObjFinish(oid, tableInfo, reader);
                    _keyValueTrProtector.Stop(ref taken);
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

        object ReadObjFinish(ulong oid, TableInfo tableInfo, KeyValueDBValueReader reader)
        {
            var tableVersion = reader.ReadVUInt32();
            var metadata = new DBObjectMetadata(oid, DBObjectState.Read);
            var obj = tableInfo.Creator(this, metadata);
            _objCache.TryAdd(oid, new WeakReference(obj));
            _objMetadata.Add(obj, metadata);
            tableInfo.GetLoader(tableVersion)(this, metadata, reader, obj);
            return obj;
        }

        KeyValueDBValueReader ReadObjStart(ulong oid, out TableInfo tableInfo)
        {
            var reader = new KeyValueDBValueReader(_keyValueTr);
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
            tableInfo.EnsureClientTypeVersion();
            lock (tableInfo.SingletonLock)
            {
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
                StoreSingletonOid(tableInfo.Id, oid);
                var metadata = new DBObjectMetadata(oid, DBObjectState.Dirty);
                obj = tableInfo.Creator(this, metadata);
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
                    _objCache.TryAdd(metadata.Id, new WeakReference(@object));
                }
                metadata.State = DBObjectState.Dirty;
                _dirtyObjSet.TryAdd(metadata.Id, @object);
                return metadata.Id;
            }
            return RegisterNewObject(@object);
        }

        public ulong StoreIfUnknown(object @object)
        {
            var ti = AutoRegisterType(@object.GetType());
            ti.EnsureClientTypeVersion();
            DBObjectMetadata metadata;
            if (_objMetadata.TryGetValue(@object, out metadata))
            {
                if (metadata.Id == 0 || metadata.State == DBObjectState.Deleted) return 0;
                return metadata.Id;
            }
            return RegisterNewObject(@object);
        }

        ulong RegisterNewObject(object obj)
        {
            var id = _owner.AllocateNewOid();
            _objMetadata.Add(obj, new DBObjectMetadata(id, DBObjectState.Dirty));
            _objCache.TryAdd(id, new WeakReference(obj));
            _dirtyObjSet.TryAdd(id, obj);
            return id;
        }

        void StoreSingletonOid(uint tableId, ulong oid)
        {
            bool taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(ObjectDB.TableSingletonsPrefix);
                _keyValueTr.CreateOrUpdateKeyValue(BuildKeyFromOid(tableId), BuildKeyFromOid(oid));
            }
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
            }
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
            int len;
            byte[] buf;
            int bufOfs;
            _keyValueTr.PeekKey(0, out len, out buf, out bufOfs);
            ulong oid = PackUnpack.UnpackVUInt(buf, ref bufOfs);
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
                _keyValueTr.Commit();
            }
            finally
            {
                Dispose();
            }
            foreach (var updatedTable in _updatedTables)
            {
                updatedTable.Key.LastPersistedVersion = updatedTable.Key.ClientTypeVersion;
            }
        }

        void StoreObject(object o)
        {
            var tableInfo = _owner.TablesInfo.FindByType(o.GetType());
            if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion)
            {
                _updatedTables.GetOrAdd(tableInfo, PersistTableInfo);
            }
            DBObjectMetadata metadata;
            _objMetadata.TryGetValue(o, out metadata);
            var shouldStop = false;
            try
            {
                _keyValueTrProtector.Start(ref shouldStop);
                _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                _keyValueTr.CreateKey(BuildKeyFromOid(metadata.Id));
                using (var writer = new KeyValueDBValueWriter(_keyValueTr))
                {
                    writer.WriteVUInt32(tableInfo.Id);
                    writer.WriteVUInt32(tableInfo.ClientTypeVersion);
                    tableInfo.Saver(this, metadata, writer, o);
                }
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
                byte[] key;
                int ofs;
                if (tableInfo.LastPersistedVersion <= 0)
                {
                    _keyValueTr.SetKeyPrefix(ObjectDB.TableNamesPrefix);
                    key = new byte[PackUnpack.LengthVUInt(tableInfo.Id)];
                    ofs = 0;
                    PackUnpack.PackVUInt(key, ref ofs, tableInfo.Id);
                    if (_keyValueTr.CreateKey(key))
                    {
                        using (var writer = new KeyValueDBValueWriter(_keyValueTr))
                        {
                            writer.WriteString(tableInfo.Name);
                        }
                    }
                }
                _keyValueTr.SetKeyPrefix(ObjectDB.TableVersionsPrefix);
                key = new byte[PackUnpack.LengthVUInt(tableInfo.Id) + PackUnpack.LengthVUInt(tableInfo.ClientTypeVersion)];
                ofs = 0;
                PackUnpack.PackVUInt(key, ref ofs, tableInfo.Id);
                PackUnpack.PackVUInt(key, ref ofs, tableInfo.ClientTypeVersion);
                if (_keyValueTr.CreateKey(key))
                {
                    var tableVersionInfo = tableInfo.ClientTableVersionInfo;
                    using (var writer = new KeyValueDBValueWriter(_keyValueTr))
                    {
                        tableVersionInfo.Save(writer);
                    }
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
