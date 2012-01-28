using System;

namespace BTDB.IL.Caching
{
    public class CachingILBuilder : IILBuilder
    {
        readonly IILBuilder _wrapping;

        public CachingILBuilder(IILBuilder wrapping)
        {
            _wrapping = wrapping;
        }

        public IILDynamicMethod NewMethod(string name, Type @delegate)
        {
            return _wrapping.NewMethod(name, @delegate);
        }

        public IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : class
        {
            return _wrapping.NewMethod<TDelegate>(name);
        }

        public IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType)
        {
            return _wrapping.NewMethod(name, @delegate, thisType);
        }

        public IILDynamicType NewType(string name, Type baseType, Type[] interfaces)
        {
            return _wrapping.NewType(name, baseType, interfaces);
        }
    }
}