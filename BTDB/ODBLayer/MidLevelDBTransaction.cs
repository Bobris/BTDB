using System;
using System.Linq;

namespace BTDB.ODBLayer
{
    class MidLevelDBTransaction : IMidLevelDBTransaction
    {
        MidLevelDB _owner;

        public MidLevelDBTransaction(MidLevelDB owner)
        {
            _owner = owner;
        }

        public void Dispose()
        {
        }

        public IQueryable<T> Query<T>() where T : class
        {
            return new Query<T>(new QueryProvider(this));
        }

        public object Insert(Type type)
        {
            throw new NotImplementedException();
        }

        public T Insert<T>() where T : class
        {
            throw new NotImplementedException();
        }

        public void Delete(object @object)
        {
            throw new NotImplementedException();
        }

        public void DeleteAll<T>() where T : class
        {
            throw new NotImplementedException();
        }

        public void DeleteAll(Type type)
        {
            throw new NotImplementedException();
        }

        public void Commit()
        {
            throw new NotImplementedException();
        }
    }
}