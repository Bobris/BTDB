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
        readonly ConcurrentDictionary<ulong, WeakReference> _objCache = new ConcurrentDictionary<ulong, WeakReference>();
        readonly ConcurrentDictionary<ulong, object> _dirtyObjSet = new ConcurrentDictionary<ulong, object>();
        readonly ConcurrentDictionary<TableInfo, bool> _updatedTables = new ConcurrentDictionary<TableInfo, bool>();
        long _lastObjId;

        public ulong CreateNewObjectId()
        {
            var id = (ulong)System.Threading.Interlocked.Increment(ref _lastObjId);
            return id;
        }

        public void RegisterNewObject(ulong id, object obj)
        {
            _objCache.TryAdd(id, new WeakReference(obj));
            _dirtyObjSet.TryAdd(id, obj);
        }

        public AbstractBufferedWriter PrepareToWriteObject(ulong id)
        {
            var key = new byte[1 + PackUnpack.LengthVUInt(id)];
            key[0] = 1;
            var ofs = 1;
            PackUnpack.PackVUInt(key, ref ofs, id);
            _lowLevelTr.SetKeyPrefix(null);
            _lowLevelTr.CreateKey(key);
            return new LowLevelDBValueWriter(_lowLevelTr);
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

        public IQueryable<T> Query<T>() where T : class
        {
            return new Query<T>(new QueryProvider(this));
        }

        public IEnumerable<T> Enumerate<T>() where T : class
        {
            return Enumerate(typeof(T)).Cast<T>();
        }

        public IEnumerable<object> Enumerate(Type type)
        {
            var ti = _owner.TablesInfo.FindByType(type);
            if (ti == null)
            {
                var name = _owner.Type2NameRegistry.FindNameByType(type) ?? _owner.RegisterType(type);
                _owner.TablesInfo.LinkType2Name(type, name);
            }
            // TODO
            foreach (var o in EnumerateAll())
            {
                if (type.IsAssignableFrom(o.GetType())) yield return o;
            }
        }

        IEnumerable<object> EnumerateAll()
        {
            // TODO
            foreach (var o in _dirtyObjSet)
            {
                yield return o.Value;
            }
            _lowLevelTr.SetKeyPrefix(MidLevelDB.AllObjectsPrefix);
            if (!_lowLevelTr.FindFirstKey()) yield break;
            do
            {
                int len;
                byte[] buf;
                int bufOfs;
                _lowLevelTr.PeekKey(0, out len, out buf, out bufOfs);
                var oid = PackUnpack.UnpackVUInt(buf, ref bufOfs);
                var reader = new LowLevelDBValueReader(_lowLevelTr);
                var tableId = reader.ReadVUInt32();
                var tableInfo = _owner.TablesInfo.FindById(tableId);
                var tableVersion = reader.ReadVUInt32();
                var obj = tableInfo.GetLoader(tableVersion)(this, oid, reader);
                _objCache.TryAdd(oid, new WeakReference(obj));
                yield return obj;
            } while (_lowLevelTr.FindNextKey());
        }

        public object Insert(Type type)
        {
            var ti = _owner.TablesInfo.FindByType(type);
            if (ti == null)
            {
                var name = _owner.Type2NameRegistry.FindNameByType(type) ?? _owner.RegisterType(type);
                ti = _owner.TablesInfo.LinkType2Name(type, name);
            }
            ti.EnsureClientTypeVersion();
            return ti.Inserter(this);
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
            _lowLevelTr.SetKeyPrefix(MidLevelDB.AllObjectsPrefix);
            var key = new byte[PackUnpack.LengthVUInt(oid)];
            int ofs = 0;
            PackUnpack.PackVUInt(key, ref ofs, oid);
            if (_lowLevelTr.FindExactKey(key))
                _lowLevelTr.EraseCurrent();
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
            _lowLevelTr.SetKeyPrefix(null);
            return true;
        }
    }
}
