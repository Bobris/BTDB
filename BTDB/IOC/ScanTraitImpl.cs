using System;
using BTDB.Collections;

namespace BTDB.IOC;

class ScanTraitImpl : IScanTrait, IScanTraitImpl
{
    StructList<Predicate<Type>> _filters;

    public void Where(Predicate<Type> filter)
    {
        _filters.Add(filter);
    }

    public bool MatchFilter(Type type)
    {
        foreach (var predicate in _filters)
        {
            if (!predicate(type)) return false;
        }
        return true;
    }
}
