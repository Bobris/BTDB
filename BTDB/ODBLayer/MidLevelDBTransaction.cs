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

        public ulong ObjectInserted(object obj)
        {
            var id = (ulong)System.Threading.Interlocked.Increment(ref _lastObjId);
            _dirtyObjSet.TryAdd(id, obj);
            return id;
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
            yield break;
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
                storeObject(o.Value);
            }
            _lowLevelTr.Commit();
        }

        void storeObject(object o)
        {
            throw new NotImplementedException();
        }
    }
}