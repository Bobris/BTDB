using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

class ILMethodImpl : IILMethodPrivate
{
    int _expectedLength;
    IILGen? _gen;
    readonly MethodBuilder _method;
    readonly IILGenForbiddenInstructions _forbiddenInstructions;

    public ILMethodImpl(MethodBuilder method, IILGenForbiddenInstructions forbiddenInstructions)
    {
        _method = method;
        _forbiddenInstructions = forbiddenInstructions;
        _expectedLength = 64;
    }

    public void ExpectedLength(int length)
    {
        _expectedLength = length;
    }

    public bool InitLocals
    {
        get => _method.InitLocals;
        set => _method.InitLocals = value;
    }

    public IILGen Generator => _gen ??= new ILGenImpl(_method.GetILGenerator(_expectedLength), _forbiddenInstructions);

    public MethodInfo TrueMethodInfo => _method;

    public MethodBuilder MethodBuilder => _method;

    public Type ReturnType => TrueMethodInfo.ReturnType;

    public Type[] Parameters => TrueMethodInfo.GetParameters().Select(p => p.ParameterType).ToArray();
}
