using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal interface IAsTraitImpl
    {
        IEnumerable<Type> GetAsTypesFor(Type implementationType);
    }
}