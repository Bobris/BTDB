using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

public class ILBuilderRelease : IILBuilder
{
    public IILDynamicMethod NewMethod(string name, Type @delegate)
    {
        if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
        return new ILDynamicMethodImpl(name, @delegate, null);
    }

    public IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : Delegate
    {
        return new ILDynamicMethodImpl<TDelegate>(name);
    }

    public IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType)
    {
        if (thisType == null) throw new ArgumentNullException(nameof(thisType));
        if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
        return new ILDynamicMethodImpl(name, @delegate, thisType);
    }

    public IILDynamicType NewType(string name, Type baseType, Type[] interfaces)
    {
        return new ILDynamicTypeImpl(name, baseType, interfaces);
    }

    static readonly ConcurrentDictionary<string, int> UniqueNames = new ConcurrentDictionary<string, int>();

    internal static string UniqueName(string name)
    {
        name = name.Replace('<', '_');
        name = name.Replace('>', '_');
        name = name.Replace(',', '_');
        name = name.Replace(" ", "");
        var uniqueName = name;
        var uniqueIdx = UniqueNames.AddOrUpdate(name, 0, (s, v) => v + 1);
        if (uniqueIdx != 0)
            uniqueName = $"{name}__{uniqueIdx}";
        return uniqueName;
    }

    public Type NewEnum(string name, Type baseType, IEnumerable<KeyValuePair<string, object>> literals)
    {
        name = UniqueName(name);
        var ab = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.RunAndCollect);
        var mb = ab.DefineDynamicModule(name);
        var enumBuilder = mb.DefineEnum(name, TypeAttributes.Public, baseType);
        foreach (var pair in literals)
        {
            enumBuilder.DefineLiteral(pair.Key, pair.Value);
        }
        return enumBuilder.CreateTypeInfo();
    }
}
