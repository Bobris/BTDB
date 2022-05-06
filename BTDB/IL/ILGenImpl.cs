using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

class ILGenImpl : IILGen
{
    readonly ILGenerator _ilGenerator;
    readonly IILGenForbiddenInstructions _forbiddenInstructions;

    public ILGenImpl(ILGenerator ilGenerator, IILGenForbiddenInstructions forbiddenInstructions)
    {
        _ilGenerator = ilGenerator;
        _forbiddenInstructions = forbiddenInstructions;
    }

    public IILLabel DefineLabel(string name)
    {
        return new ILLabelImpl(_ilGenerator.DefineLabel());
    }

    class ILLabelImpl : IILLabel
    {
        readonly Label _label;

        public ILLabelImpl(Label label)
        {
            _label = label;
        }

        public Label Label => _label;
    }

    public IILLocal DeclareLocal(Type type, string name, bool pinned = false)
    {
        return new ILLocalImpl(_ilGenerator.DeclareLocal(type, pinned));
    }

    class ILLocalImpl : IILLocal
    {
        readonly LocalBuilder _localBuilder;

        public ILLocalImpl(LocalBuilder localBuilder)
        {
            _localBuilder = localBuilder;
        }

        public int Index => LocalBuilder.LocalIndex;

        public bool Pinned => LocalBuilder.IsPinned;

        public Type LocalType => LocalBuilder.LocalType;

        public LocalBuilder LocalBuilder => _localBuilder;
    }

    public IILGen Comment(string text)
    {
        return this;
    }

    public void Emit(OpCode opCode)
    {
        _ilGenerator.Emit(opCode);
    }

    public void Emit(OpCode opCode, sbyte param)
    {
        _ilGenerator.Emit(opCode, param);
    }

    public void Emit(OpCode opCode, byte param)
    {
        _ilGenerator.Emit(opCode, param);
    }

    public void Emit(OpCode opCode, ushort param)
    {
        _ilGenerator.Emit(opCode, param);
    }

    public void Emit(OpCode opCode, int param)
    {
        _ilGenerator.Emit(opCode, param);
    }

    public void Emit(OpCode opCode, FieldInfo param)
    {
        _ilGenerator.Emit(opCode, param);
    }

    public void Emit(OpCode opCode, ConstructorInfo param)
    {
        _ilGenerator.Emit(opCode, param);
    }

    public void Emit(OpCode opCode, MethodInfo param)
    {
        _forbiddenInstructions.Emit(_ilGenerator, opCode, param);
    }

    public void Emit(OpCode opCode, Type type)
    {
        _ilGenerator.Emit(opCode, type);
    }

    public void Emit(OpCode opCode, IILLocal ilLocal)
    {
        _ilGenerator.Emit(opCode, ((ILLocalImpl)ilLocal).LocalBuilder);
    }

    public void Emit(OpCode opCode, IILLabel ilLabel)
    {
        _ilGenerator.Emit(opCode, ((ILLabelImpl)ilLabel).Label);
    }

    public IILGen Mark(IILLabel label)
    {
        _ilGenerator.MarkLabel(((ILLabelImpl)label).Label);
        return this;
    }

    public IILGen Ldftn(IILMethod method)
    {
        _ilGenerator.Emit(OpCodes.Ldftn, ((IILMethodPrivate)method).TrueMethodInfo);
        return this;
    }

    public IILGen Ldstr(string str)
    {
        _ilGenerator.Emit(OpCodes.Ldstr, str);
        return this;
    }

    public IILGen Try()
    {
        _ilGenerator.BeginExceptionBlock();
        return this;
    }

    public IILGen Catch(Type exceptionType)
    {
        _ilGenerator.BeginCatchBlock(exceptionType);
        return this;
    }

    public IILGen Finally()
    {
        _ilGenerator.BeginFinallyBlock();
        return this;
    }

    public IILGen EndTry()
    {
        _ilGenerator.EndExceptionBlock();
        return this;
    }

    public void Emit(OpCode opCode, IILField ilField)
    {
        _ilGenerator.Emit(opCode, ((IILFieldPrivate)ilField).TrueField);
    }

    public void Emit(OpCode opCode, long value)
    {
        _ilGenerator.Emit(opCode, value);
    }

    public void Emit(OpCode opCode, float value)
    {
        _ilGenerator.Emit(opCode, value);
    }

    public void Emit(OpCode opCode, double value)
    {
        _ilGenerator.Emit(opCode, value);
    }
}
