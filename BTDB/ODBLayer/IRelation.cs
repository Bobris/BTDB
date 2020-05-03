using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public interface IRelation<T> : IReadOnlyCollection<T>, IRelation where T : class
    {
        bool Upsert(T item);
    }

    public interface IRelation
    {
    }
}
