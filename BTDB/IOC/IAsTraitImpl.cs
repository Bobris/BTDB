using System;
using System.Collections.Generic;

namespace BTDB.IOC;

interface IAsTraitImpl
{
    IEnumerable<KeyAndType> GetAsTypesFor(Type implementationType);
    bool PreserveExistingDefaults { get; }
    bool UniqueRegistration { get; }
}
