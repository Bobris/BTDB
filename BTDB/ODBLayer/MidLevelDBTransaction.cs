using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.ODBLayer
{
    internal class MidLevelDBTransaction : IMidLevelDBTransaction, IMidLevelDBTransactionInternal
    {
        readonly MidLevelDB _owner;
        readonly ILowLevelDBTransaction _lowLevelTr;
        readonly LowLevelDBTransactionProtector _lowLevelTrProtector = new LowLevelDBTransactionProtector();
        readonly ConcurrentDictionary<ulong, WeakReference> _objCache = new ConcurrentDictionary<ulong, WeakReference>();
        readonly ConcurrentDictionary<ulong, object> _dirtyObjSet = new ConcurrentDictionary<ulong, object>();
        readonly ConcurrentDictionary<TableInfo, bool> _updatedTables = new ConcurrentDictionary<TableInfo, bool>();

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
                _lowLevelTrProtector.Start(ref shouldStop);
                _lowLevelTr.SetKeyPrefix(MidLevelDB.AllObjectsPrefix);
                var key = new byte[PackUnpack.LengthVUInt(id)];
                var ofs = 0;
                PackUnpack.PackVUInt(key, ref ofs, id);
                _lowLevelTr.CreateKey(key);
                var writer = new LowLevelDBValueProtectedWriter(_lowLevelTr, _lowLevelTrProtector);
                shouldStop = false;
                return writer;
            }
            finally
            {
                if (shouldStop) _lowLevelTrProtector.Stop();
            }
        }

        public void ObjectModified(ulong id, object obj)
        {
            _dirtyObjSet.TryAdd(id, obj);
        }

        public MidLevelDBTransaction(MidLevelDB owner, ILowLevelDBTransaction lowLevelTr)
        {
            _owner = owner;
            _lowLevelTr = lowLevelTr;
        }

        public void Dispose()
        {
            _lowLevelTr.Dispose();
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
                    if (!taken) _lowLevelTrProtector.Start(ref taken);
                    if (oid == 0)
                    {
                        prevProtectionCounter = _lowLevelTrProtector.ProtectionCounter;
                        _lowLevelTr.SetKeyPrefix(MidLevelDB.AllObjectsPrefix);
                        if (!_lowLevelTr.FindFirstKey()) break;
                    }
                    else
                    {
                        if (_lowLevelTrProtector.WasInterupted(prevProtectionCounter))
                        {
                            _lowLevelTr.SetKeyPrefix(MidLevelDB.AllObjectsPrefix);
                            oid++;
                            byte[] key = BuildKeyFromOid(oid);
                            if (_lowLevelTr.FindKey(key, 0, key.Length, FindKeyStrategy.OnlyNext) == FindKeyResult.NotFound)
                            {
                                oid--;
                                break;
                            }
                        }
                        else
                        {
                            if (!_lowLevelTr.FindNextKey()) break;
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
                                _lowLevelTrProtector.Stop(ref taken);
                                yield return o;
                                continue;
                            }
                            continue;
                        }
                    }
                    var reader = new LowLevelDBValueReader(_lowLevelTr);
                    var tableId = reader.ReadVUInt32();
                    var tableInfo = _owner.TablesInfo.FindById(tableId);
                    if (tableInfo == null) throw new BTDBException(string.Format("Unknown TypeId {0} of Oid {1}", tableId, oid));
                    EnsureClientTypeNotNull(tableInfo);
                    if (type != null && !type.IsAssignableFrom(tableInfo.ClientType)) continue;
                    var tableVersion = reader.ReadVUInt32();
                    var obj = tableInfo.GetLoader(tableVersion)(this, oid, reader);
                    _objCache.TryAdd(oid, new WeakReference(obj));
                    _lowLevelTrProtector.Stop(ref taken);
                    yield return obj;
                }
            }
            finally
            {
                if (taken) _lowLevelTrProtector.Stop();
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
            _lowLevelTr.PeekKey(0, out len, out buf, out bufOfs);
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
            return ti.Inserter(this);
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

        public void Delete(object @object)
        {
            if (@object == null) throw new ArgumentNullException("object");
            var o = @object as IMidLevelObject;
            if (o == null) throw new BTDBException("Object to delete is not MidLevelDB object");
            var oid = o.Oid;
            var taken = false;
            try
            {
                _lowLevelTrProtector.Start(ref taken);
                _lowLevelTr.SetKeyPrefix(MidLevelDB.AllObjectsPrefix);
                var key = new byte[PackUnpack.LengthVUInt(oid)];
                int ofs = 0;
                PackUnpack.PackVUInt(key, ref ofs, oid);
                if (_lowLevelTr.FindExactKey(key))
                    _lowLevelTr.EraseCurrent();
            }
            finally
            {
                if (taken) _lowLevelTrProtector.Stop();
            }
            _objCache.TryRemove(oid);
            _dirtyObjSet.TryRemove(oid);
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
            foreach (var o in _dirtyObjSet)
            {
                StoreObject(o.Value);
            }
            _lowLevelTr.Commit();
            foreach (var updatedTable in _updatedTables)
            {
                updatedTable.Key.LastPersistedVersion = updatedTable.Key.ClientTypeVersion;
            }
        }

        void StoreObject(object o)
        {
            var midLevelObject = o as IMidLevelObject;
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
                _lowLevelTrProtector.Start(ref taken);
                byte[] key;
                int ofs;
                if (tableInfo.LastPersistedVersion <= 0)
                {
                    _lowLevelTr.SetKeyPrefix(MidLevelDB.TableNamesPrefix);
                    key = new byte[PackUnpack.LengthVUInt(tableInfo.Id)];
                    ofs = 0;
                    PackUnpack.PackVUInt(key, ref ofs, tableInfo.Id);
                    if (_lowLevelTr.CreateKey(key))
                    {
                        using (var writer = new LowLevelDBValueWriter(_lowLevelTr))
                        {
                            writer.WriteString(tableInfo.Name);
                        }
                    }
                }
                _lowLevelTr.SetKeyPrefix(MidLevelDB.TableVersionsPrefix);
                key = new byte[PackUnpack.LengthVUInt(tableInfo.Id) + PackUnpack.LengthVUInt(tableInfo.ClientTypeVersion)];
                ofs = 0;
                PackUnpack.PackVUInt(key, ref ofs, tableInfo.Id);
                PackUnpack.PackVUInt(key, ref ofs, tableInfo.ClientTypeVersion);
                if (_lowLevelTr.CreateKey(key))
                {
                    var tableVersionInfo = tableInfo.ClientTableVersionInfo;
                    using (var writer = new LowLevelDBValueWriter(_lowLevelTr))
                    {
                        tableVersionInfo.Save(writer);
                    }
                }
                return true;
            }
            finally
            {
                if (taken) _lowLevelTrProtector.Stop();
            }
        }
    }
}
