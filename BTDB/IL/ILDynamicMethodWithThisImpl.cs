using System;
using System.Linq;
using System.Reflection.Emit;

namespace BTDB.IL;

public class ILDynamicMethodWithThisImpl : IILDynamicMethodWithThis
{
    readonly Type _delegateType;
    int _expectedLength;
    IILGen? _gen;
    readonly DynamicMethod _dynamicMethod;

    public ILDynamicMethodWithThisImpl(string name, Type delegateType, Type thisType)
    {
        _delegateType = delegateType;
        _expectedLength = 64;
        var mi = delegateType.GetMethod("Invoke");
        _dynamicMethod = new DynamicMethod(name, mi.ReturnType, new[] { thisType }.Concat(
                                           mi.GetParameters().Select(pi => pi.ParameterType)).ToArray(), true);
    }

    public void ExpectedLength(int length)
    {
        _expectedLength = length;
    }

    public bool InitLocals
    {
        get => _dynamicMethod.InitLocals;
        set => _dynamicMethod.InitLocals = value;
    }

    public IILGen Generator => _gen ??= new ILGenImpl(_dynamicMethod.GetILGenerator(_expectedLength), new IilGenForbiddenInstructionsGodPowers());

    public void FinalizeCreation()
    {
    }

    public object Create(object @this)
    {
        return _dynamicMethod.CreateDelegate(_delegateType, @this);
    }

}
