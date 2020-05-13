using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public interface ICovariantRelation<out T> : IReadOnlyCollection<T>, IRelation
    {
    }

    public interface IRelation<T> : ICovariantRelation<T>
    {
        bool Upsert(T item);
    }

    public interface IRelation
    {
        Type BtdbInternalGetRelationInterfaceType();
        IRelation? BtdbInternalNextInChain { get; set; }
    }
}
