using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public interface IObjectDBTransaction : IDisposable
    {
        IEnumerable<T> Enumerate<T>() where T : class;
        IEnumerable<object> Enumerate(Type type);

        object Get(ulong oid);
        ulong GetOid(object @object);
        
        object Singleton(Type type);
        T Singleton<T>() where T : class;

        ulong Store(object @object);
        ulong StoreAndFlush(object @object);
        void Delete(object @object);

        void DeleteAll<T>() where T : class;
        void DeleteAll(Type type);

        void Commit();
    }
}