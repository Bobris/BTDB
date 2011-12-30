using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal interface IAsTraitImpl
    {
        IEnumerable<KeyValuePair<object, Type>> GetAsTypesFor(Type implementationType);
    }
}