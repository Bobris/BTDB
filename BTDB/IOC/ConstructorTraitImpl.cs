using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace BTDB.IOC
{
    internal class ConstructorTraitImpl : IConstructorTrait, IConstructorTraitImpl
    {
        Type[] _parameterTypes;

        public IEnumerable<ConstructorInfo> ReturnPossibleConstructors(Type forType)
        {
            return forType.GetConstructors();
        }

        public ConstructorInfo ChooseConstructor(Type forType, IEnumerable<ConstructorInfo> candidates)
        {
            if (_parameterTypes != null)
            {
                return candidates.FirstOrDefault(ci => ci.GetParameters().Select(pi => pi.ParameterType).SequenceEqual(_parameterTypes));
            }
            return candidates.OrderByDescending(ci => ci.GetParameters().Length).FirstOrDefault();
        }

        public void UsingConstructor(params Type[] parameterTypes)
        {
            if (_parameterTypes != null) throw new InvalidOperationException("UsingConstructor specification could be used only once");
            _parameterTypes = parameterTypes;
        }
    }
}