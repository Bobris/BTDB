using System;

namespace BTDB.IL
{
    public class ILBuilderImpl : IILBuilder
    {
        public bool Debuggable { get; set; }

        public IILDynamicMethod NewMethod(string name, Type @delegate)
        {
            if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            if (Debuggable) return new ILDynamicMethodDebugImpl(name, @delegate, null);
            return new ILDynamicMethodImpl(name, @delegate, null);
        }

        public IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : class
        {
            var t = typeof(TDelegate);
            if (!t.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            if (Debuggable) return new ILDynamicMethodDebugImpl<TDelegate>(name);
            return new ILDynamicMethodImpl<TDelegate>(name);
        }

        public IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType)
        {
            if (thisType == null) throw new ArgumentNullException("thisType");
            if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            if (Debuggable) return new ILDynamicMethodDebugImpl(name, @delegate, thisType);
            return new ILDynamicMethodImpl(name, @delegate, thisType);
        }

        public IILDynamicType NewType(string name, Type baseType, Type[] interfaces)
        {
            if (Debuggable) return new ILDynamicTypeDebugImpl(name, baseType, interfaces);
            return new ILDynamicTypeImpl(name, baseType, interfaces);
        }
    }
}