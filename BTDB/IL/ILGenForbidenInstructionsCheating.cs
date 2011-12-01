using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILGenForbidenInstructionsCheating : IILGenForbidenInstructions
    {
        readonly TypeBuilder _typeBuilder;

        public ILGenForbidenInstructionsCheating(TypeBuilder typeBuilder)
        {
            _typeBuilder = typeBuilder;
        }

        public void Emit(ILGenerator ilGen, OpCode opCode, MethodInfo methodInfo)
        {
            ilGen.Emit(opCode, methodInfo);
        }

        public void FinishType(Type finalType)
        {
        }
    }
}