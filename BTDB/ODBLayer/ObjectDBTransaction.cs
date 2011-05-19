using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.ODBLayer
{
    internal class ObjectDBTransaction : IObjectDBTransaction, IObjectDBTransactionInternal
    {
        readonly ObjectDB _owner;
        readonly IKeyValueDBTransaction _keyValueTr;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector = new KeyValueDBTransactionProtector();
        readonly ConcurrentDictionary<ulong, WeakReference> _objCache = new ConcurrentDictionary<ulong, WeakReference>();
        readonly ConcurrentDictionary<ulong, object> _dirtyObjSet = new ConcurrentDictionary<ulong, object>();
        readonly ConcurrentDictionary<TableInfo, bool> _updatedTables = new ConcurrentDictionary<TableInfo, bool>();
        bool _valid;

        public ulong CreateNewObjectId()
        {
            var id = _owner.AllocateNewOid();
            return id;
        }

        public void RegisterNewObject(ulong id, object obj)
        {
            _objCache.TryAdd(id, new WeakReference(obj));
            _dirtyObjSet.TryAdd(id, obj);
        }

        public AbstractBufferedWriter PrepareToWriteObject(ulong id)
        {
            var shouldStop = false;
            try
            {
                _keyValueTrProtector.Start(ref shouldStop);
                _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                var key = new byte[PackUnpack.LengthVUInt(id)];
                var ofs = 0;
                PackUnpack.PackVUInt(key, ref ofs, id);
                _keyValueTr.CreateKey(key);
                var writer = new KeyValueDBValueProtectedWriter(_keyValueTr, _keyValueTrProtector);
                shouldStop = false;
                return writer;
            }
            finally
            {
                if (shouldStop) _keyValueTrProtector.Stop();
            }
        }

        public void ObjectModified(ulong id, object obj)
        {
            _dirtyObjSet.TryAdd(id, obj);
        }

        public ObjectDBTransaction(ObjectDB owner, IKeyValueDBTransaction keyValueTr)
        {
            _owner = owner;
            _keyValueTr = keyValueTr;
            _valid = true;
        }

        public void Dispose()
        {
            _valid = false;
            _keyValueTr.Dispose();
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
            var obj = tableInfo.GetLoader(tableVersion)(this, oid, reader);
            _objCache.TryAdd(oid, new WeakReference(obj));
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
            var midLevelObject = obj as IDBObject;
            if (midLevelObject == null) throw new BTDBException("Only MidLevelObjects are allowed");
            return midLevelObject.Oid;
        }

        public void CheckPropertyOperationValidity(object obj)
        {
            if (!_valid) throw new BTDBException(string.Format("Cannot access object {0} outside of transaction",((IDBObject)obj).TableName));
            if (((IDBObject)obj).Deleted) throw new BTDBException(string.Format("Cannot access deleted object {0}",((IDBObject)obj).TableName));
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
                return tableInfo.Inserter(this, oid);
            }
        }

        public T Singleton<T>() where T : class
        {
            return (T)Singleton(typeof(T));
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

        public object Insert(Type type)
        {
            var ti = AutoRegisterType(type);
            ti.EnsureClientTypeVersion();
            return ti.Inserter(this, _owner.AllocateNewOid());
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

        public T Insert<T>() where T : class
        {
            return (T)Insert(typeof(T));
        }

        public void InternalDelete(object obj)
        {
            var o = (IDBObject)obj;
            var oid = o.Oid;
            var taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
                var key = new byte[PackUnpack.LengthVUInt(oid)];
                int ofs = 0;
                PackUnpack.PackVUInt(key, ref ofs, oid);
                if (_keyValueTr.FindExactKey(key))
                    _keyValueTr.EraseCurrent();
            }
            finally
            {
                if (taken) _keyValueTrProtector.Stop();
            }
            _objCache.TryRemove(oid);
            _dirtyObjSet.TryRemove(oid);
        }

        public void Delete(object @object)
        {
            if (@object == null) throw new ArgumentNullException("object");
            var o = @object as IDBObject;
            if (o == null) throw new BTDBException("Object to delete is not ObjectDB object");
            o.Delete();
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
            }
            finally
            {
                _valid = false;
            }
            _keyValueTr.Commit();
            foreach (var updatedTable in _updatedTables)
            {
                updatedTable.Key.LastPersistedVersion = updatedTable.Key.ClientTypeVersion;
            }
        }

        void StoreObject(object o)
        {
            var midLevelObject = o as IDBObject;
            var tableInfo = _owner.TablesInfo.FindById(midLevelObject.TableId);
            if (tableInfo.LastPersistedVersion != tableInfo.ClientTypeVersion)
            {
                _updatedTables.GetOrAdd(tableInfo, PersistTableInfo);
            }
            tableInfo.Saver(o);
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
