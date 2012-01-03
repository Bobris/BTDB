using System;

namespace BTDB.IL
{
    public interface IILBuilder
    {
        bool Debuggable { get; set; }

        IILDynamicMethod NewMethod(string name, Type @delegate);
        IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : class;
        IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType);

        IILDynamicType NewType(string name, Type baseType, Type[] interfaces);
    }
}