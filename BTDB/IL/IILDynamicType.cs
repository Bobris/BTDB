using System;
using System.Reflection;

namespace BTDB.IL;

public interface IILDynamicType
{
    IILMethod DefineMethod(string name, Type returns, Type[] parameters, MethodAttributes methodAttributes = MethodAttributes.Public);
    IILField DefineField(string name, Type type, FieldAttributes fieldAttributes);
    IILEvent DefineEvent(string name, EventAttributes eventAttributes, Type type);
    IILMethod DefineConstructor(Type[] parameters);
    IILMethod DefineConstructor(Type[] parameters, string[] parametersNames);
    void DefineMethodOverride(IILMethod methodBuilder, MethodInfo baseMethod);
    Type CreateType();
}
