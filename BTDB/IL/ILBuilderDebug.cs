using System;

namespace BTDB.IL
{
    public class ILBuilderDebug : IILBuilder
    {
        public IILDynamicMethod NewMethod(string name, Type @delegate)
        {
            if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            return new ILDynamicMethodDebugImpl(name, @delegate, null);
        }

        public IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : class
        {
            var t = typeof(TDelegate);
            if (!t.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            return new ILDynamicMethodDebugImpl<TDelegate>(name);
        }

        public IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType)
        {
            if (thisType == null) throw new ArgumentNullException("thisType");
            if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            return new ILDynamicMethodDebugImpl(name, @delegate, thisType);
        }

        public IILDynamicType NewType(string name, Type baseType, Type[] interfaces)
        {
            return new ILDynamicTypeDebugImpl(name, baseType, interfaces);
        }
    }
}