using System;

namespace BTDB.IOC;

interface IScanTraitImpl
{
    bool MatchFilter(Type type);
}
