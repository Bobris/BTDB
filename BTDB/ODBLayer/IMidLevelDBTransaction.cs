using System;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.ODBLayer
{
    public interface IMidLevelDBTransaction : IDisposable
    {
        IEnumerable<T> Enumerate<T>() where T : class;
        IEnumerable<object> Enumerate(Type type);

        object Get(ulong oid);

        object Singleton(Type type);
        T Singleton<T>() where T : class;

        object Insert(Type type);
        T Insert<T>() where T : class;

        void Delete(object @object);

        void DeleteAll<T>() where T : class;
        void DeleteAll(Type type);

        void Commit();
    }
}