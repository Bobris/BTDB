using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

class ILDynamicMethodImpl : IILDynamicMethod, IILDynamicMethodWithThis
{
    readonly Type _delegateType;
    int _expectedLength;
    IILGen? _gen;
    readonly DynamicMethod _dynamicMethod;

    public ILDynamicMethodImpl(string name, Type delegateType, Type? thisType)
    {
        _delegateType = delegateType;
        _expectedLength = 64;
        var mi = delegateType.GetMethod("Invoke");
        Type[] parameterTypes;
        if (thisType == null)
        {
            parameterTypes = mi!.GetParameters().Select(pi => pi.ParameterType).ToArray();
        }
        else
        {
            parameterTypes = new[] { thisType }.Concat(mi!.GetParameters().Select(pi => pi.ParameterType)).ToArray();
        }
        _dynamicMethod = new DynamicMethod(name, mi.ReturnType, parameterTypes, true);
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

    public object Create()
    {
        return _dynamicMethod.CreateDelegate(_delegateType);
    }

    public void FinalizeCreation()
    {
    }

    public object Create(object @this)
    {
        return _dynamicMethod.CreateDelegate(_delegateType, @this);
    }

    public MethodInfo TrueMethodInfo => _delegateType.GetMethod("Invoke")!;
}

class ILDynamicMethodImpl<TDelegate> : ILDynamicMethodImpl, IILDynamicMethod<TDelegate> where TDelegate : Delegate
{
    public ILDynamicMethodImpl(string name) : base(name, typeof(TDelegate), null)
    {
    }

    public new TDelegate Create()
    {
        return (TDelegate)base.Create();
    }
}
