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
        long _lastObjId;

        public ulong CreateNewObjectId()
        {
            var id = (ulong)System.Threading.Interlocked.Increment(ref _lastObjId);
            return id;
        }

        public void RegisterDirtyObject(ulong id, object obj)
        {
            _dirtyObjSet.TryAdd(id, obj);
        }

        public AbstractBufferedWriter PrepareToWriteObject(ulong id)
        {
            var key = new byte[1+PackUnpack.LengthVUInt(id)];
            key[0] = 1;
            int ofs = 1;
            PackUnpack.PackVUInt(key,ref ofs,id);
            _lowLevelTr.CreateKey(key);
            return new LowLevelDBValueWriter(_lowLevelTr);
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
                yield break;
            }
            // TODO
            foreach (var o in EnumerateAll())
            {
                if (type.IsAssignableFrom(o.GetType())) yield return o;
            }
        }

        public IEnumerable<object> EnumerateAll()
        {
            // TODO
            foreach (var o in _dirtyObjSet)
            {
                yield return o.Value;
            }
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
            throw new NotImplementedException();
        }

        public void DeleteAll<T>() where T : class
        {
            DeleteAll(typeof(T));
        }

        public void DeleteAll(Type type)
        {
            throw new NotImplementedException();
        }

        public void Commit()
        {
            foreach (var o in _dirtyObjSet)
            {
                StoreObject(o.Value);
            }
            _lowLevelTr.Commit();
        }

        void StoreObject(object o)
        {
            var midLevelObject = o as IMidLevelObject;
            var tableInfo = _owner.TablesInfo.FindById(midLevelObject.TableId);
            tableInfo.Saver(o);
        }
    }
}