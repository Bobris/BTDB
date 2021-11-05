using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

public interface IILGen
{
    IILLabel DefineLabel(string? name = null);
    IILLocal DeclareLocal(Type type, string? name = null, bool pinned = false);
    IILGen Comment(string text);
    IILGen Mark(IILLabel label);
    IILGen Ldftn(IILMethod method);
    IILGen Ldstr(string str);
    IILGen Try();
    IILGen Catch(Type exceptionType);
    IILGen Finally();
    IILGen EndTry();

    // These should not be used directly
    void Emit(OpCode opCode);
    void Emit(OpCode opCode, sbyte param);
    void Emit(OpCode opCode, byte param);
    void Emit(OpCode opCode, ushort param);
    void Emit(OpCode opCode, int param);
    void Emit(OpCode opCode, FieldInfo param);
    void Emit(OpCode opCode, ConstructorInfo param);
    void Emit(OpCode opCode, MethodInfo param);
    void Emit(OpCode opCode, Type type);
    void Emit(OpCode opCode, IILLocal ilLocal);
    void Emit(OpCode opCode, IILLabel ilLabel);
    void Emit(OpCode opCode, IILField ilField);
    void Emit(OpCode opCode, long value);
    void Emit(OpCode opCode, float value);
    void Emit(OpCode opCode, double value);
}
