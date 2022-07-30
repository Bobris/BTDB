using System;
using System.Collections.Generic;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public interface ICovariantRelation<out T> : IReadOnlyCollection<T>, IRelation
{
}

public interface IRelation<T> : ICovariantRelation<T>
{
    bool Upsert(T item);
    (long Inserted, long Updated) UpsertRange(IEnumerable<T> items);
    void SerializeInsert(ref SpanWriter writer, T item);
}

public interface IRelation
{
    /// Quickly remove all items including any dependent IDictionaries.
    /// <exception cref="NotSupportedException">is thrown only as default implementation for custom relations</exception>
    void RemoveAll() => throw new NotSupportedException();
    IEnumerable<T> As<T>() => throw new NotSupportedException();
    Type BtdbInternalGetRelationInterfaceType();
    IRelation? BtdbInternalNextInChain { get; set; }
}

public interface IRelationOnCreate<T> where T: IRelation
{
    void OnCreate(IObjectDBTransaction transaction, T creating);
}
