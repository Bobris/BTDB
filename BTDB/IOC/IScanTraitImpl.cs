using System;

namespace BTDB.IOC
{
    internal interface IScanTraitImpl
    {
        bool MatchFilter(Type type);
    }
}