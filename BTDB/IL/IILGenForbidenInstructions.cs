using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal interface IILGenForbidenInstructions
    {
        void Emit(ILGenerator ilGen, OpCode opCode, MethodInfo methodInfo);
        void FinishType(Type finalType);
    }
}