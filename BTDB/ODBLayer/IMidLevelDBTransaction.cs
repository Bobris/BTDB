using System;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.ODBLayer
{
    public interface IMidLevelDBTransaction : IDisposable
    {
        IQueryable<T> Query<T>() where T : class;
        IEnumerable<T> Enumerate<T>() where T : class;
        IEnumerable<object> Enumerate(Type type);

        object Insert(Type type);
        T Insert<T>() where T : class;

        void Delete(object @object);

        void DeleteAll<T>() where T : class;
        void DeleteAll(Type type);

        void Commit();
    }
}