using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface ICovariantRelation<out T> : IReadOnlyCollection<T>, IRelation where T : class
{
}

public interface IRelation<T> : ICovariantRelation<T> where T : class
{
    bool Upsert(T item);
    (long Inserted, long Updated) UpsertRange(IEnumerable<T> items);
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

[Generate]
public interface IRelationOnCreate<T> where T : IRelation
{
    void OnCreate(IObjectDBTransaction transaction, T creating);
}
