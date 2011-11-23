using System;

namespace BTDB.IL
{
    public class ILBuilderImpl : IILBuilder
    {
        public bool Debuggable { get; set; }

        public IILDynamicMethod NewMethod(string name, Type @delegate)
        {
            if (!@delegate.IsSubclassOf(typeof(Delegate))) throw new ArgumentException("Generic paramater T must be Delegate");
            if (Debuggable) return new ILDynamicMethodDebugImpl(name, @delegate);
            return new ILDynamicMethodImpl(name, @delegate);
        }

        public IILDynamicMethod<T> NewMethod<T>(string name) where T : class
        {
            var t = typeof(T);
            if (!t.IsSubclassOf(typeof(Delegate))) throw new ArgumentException("Generic paramater T must be Delegate");
            if (Debuggable) return new ILDynamicMethodDebugImpl<T>(name);
            return new ILDynamicMethodImpl<T>(name);
        }

        public IILDynamicType NewType(string name, Type baseType, Type[] interfaces)
        {
            if (Debuggable) return new ILDynamicTypeDebugImpl(name, baseType, interfaces);
            return new ILDynamicTypeImpl(name, baseType, interfaces);
        }
    }
}