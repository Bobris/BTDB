using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer;

public interface IObjectDBTransaction : IDisposable
{
    IObjectDB Owner { get; }
    IKeyValueDBTransaction KeyValueDBTransaction { get; }

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

    /// <summary>
    /// It remove all data, metadata are not deleted. It means you still cannot do incompatible Relation primary key change.
    /// </summary>
    void DeleteAllData();

    ulong GetCommitUlong();
    void SetCommitUlong(ulong value);

    /// <summary>
    /// This creates safe checkpoint for next open in transaction log
    /// </summary>
    void NextCommitTemporaryCloseTransactionLog();

    void Commit();

    /// <summary>
    /// This is just storage for boolean, add could store here that it does not want to commit transaction, it is up to infrastructure code around if it will listen this advice.
    /// </summary>
    bool RollbackAdvised { get; set; }

    Func<IObjectDBTransaction, T> InitRelation<T>(string relationName) where T : class, IRelation;

    // Because you can register same type with different names it can return it more than once
    IEnumerable<Type> EnumerateRelationTypes();

    IRelation GetRelation(Type type);

    T GetRelation<T>() where T : class, IRelation
    {
        return (T)GetRelation(typeof(T));
    }
}
