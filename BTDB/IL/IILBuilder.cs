using System;

namespace BTDB.IL
{
    public interface IILBuilder
    {
        bool Debuggable { get; set; }

        IILDynamicMethod NewMethod(string name, Type @delegate);
        IILDynamicMethod<T> NewMethod<T>(string name) where T : class;
        IILDynamicType NewType(string name, Type baseType, Type[] interfaces);
    }
}