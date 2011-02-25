using System;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.ODBLayer
{
    class MidLevelDBTransaction : IMidLevelDBTransaction
    {
        MidLevelDB _owner;
        readonly ILowLevelDBTransaction _lowLevelTr;
        System.Collections.Concurrent.ConcurrentDictionary<ulong, WeakReference> _objCache;

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
            return Enumerate(typeof (T)).Cast<T>();
        }

        public IEnumerable<object> Enumerate(Type type)
        {
            // TODO
            yield break;
        }

        public object Insert(Type type)
        {
            throw new NotImplementedException();
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
            _lowLevelTr.Commit();
        }
    }
}