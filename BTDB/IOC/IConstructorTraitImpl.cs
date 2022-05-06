using System;
using System.Collections.Generic;
using System.Reflection;

namespace BTDB.IOC;

interface IConstructorTraitImpl
{
    IEnumerable<ConstructorInfo> ReturnPossibleConstructors(Type forType);
    ConstructorInfo ChooseConstructor(Type forType, IEnumerable<ConstructorInfo> candidates);
}
