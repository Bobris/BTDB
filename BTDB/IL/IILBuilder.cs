using System;
using System.Collections.Generic;

namespace BTDB.IL;

public interface IILBuilder
{
    IILDynamicMethod NewMethod(string name, Type @delegate);
    IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : Delegate;
    IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType);

    IILDynamicType NewType(string name, Type baseType, Type[] interfaces);
    Type NewEnum(string name, Type baseType, IEnumerable<KeyValuePair<string, object>> literals);
}
