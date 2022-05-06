using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

class ILConstructorImpl : IILMethod
{
    readonly ConstructorBuilder _constructor;
    int _expectedLength;
    IILGen? _gen;

    readonly IILGenForbiddenInstructions _forbiddenInstructions;

    public ILConstructorImpl(ConstructorBuilder constructor, IILGenForbiddenInstructions forbiddenInstructions,
        string[] parameterNames)
    {
        _constructor = constructor;
        _forbiddenInstructions = forbiddenInstructions;
        _expectedLength = 64;

        DefineParameterNames(parameterNames);
    }

    void DefineParameterNames(string[] parameterNames)
    {
        for (var i = 0; i < parameterNames.Length; i++)
        {
            _constructor.DefineParameter(i + 1, ParameterAttributes.None, parameterNames[i]);
        }
    }

    public void ExpectedLength(int length)
    {
        _expectedLength = length;
    }

    public bool InitLocals
    {
        get => _constructor.InitLocals;
        set => _constructor.InitLocals = value;
    }

    public IILGen Generator => _gen ??= new ILGenImpl(_constructor.GetILGenerator(_expectedLength), _forbiddenInstructions);
}
