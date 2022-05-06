using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

interface IILGenForbiddenInstructions
{
    void Emit(ILGenerator ilGen, OpCode opCode, MethodInfo methodInfo);
    void Emit(ILGenerator ilGen, OpCode opCode, ConstructorInfo constructorInfo);
    void FinishType(Type finalType);
}
