using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal class ScanTraitImpl : IScanTrait, IScanTraitImpl
    {
        readonly List<Predicate<Type>> _filters = new List<Predicate<Type>>();

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
}