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
        // return key and value byte size by object id
        KeyValuePair<uint, uint> GetStorageSize(ulong oid); 

        IEnumerable<Type> EnumerateSingletonTypes();
        object Singleton(Type type);
        T Singleton<T>() where T : class;

        object New(Type type);
        T New<T>() where T : class;

        ulong Store(object @object);
        ulong StoreAndFlush(object @object);
        void Delete(object @object);
        void Delete(ulong oid);

        void DeleteAll<T>() where T : class;
        void DeleteAll(Type type);

        void Commit();

        IRelationCreator<T> InitRelation<T>(string relationName);
    }
}