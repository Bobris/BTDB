using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL;

class IilGenForbiddenInstructionsGodPowers : IILGenForbiddenInstructions
{
    public void Emit(ILGenerator ilGen, OpCode opCode, MethodInfo methodInfo)
    {
        ilGen.Emit(opCode, methodInfo);
    }

    public void Emit(ILGenerator ilGen, OpCode opCode, ConstructorInfo constructorInfo)
    {
        ilGen.Emit(opCode, constructorInfo);
    }

    public void FinishType(Type finalType)
    {
    }
}
