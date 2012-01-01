using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal interface IAsTraitImpl
    {
        IEnumerable<KeyAndType> GetAsTypesFor(Type implementationType);
        bool PreserveExistingDefaults { get; }
    }
}