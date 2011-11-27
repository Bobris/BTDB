using System;
using System.Reflection;

namespace BTDB.IL
{
    public interface IILGen
    {
        IILLabel DefineLabel(string name = null);
        IILLocal DeclareLocal(Type type, string name = null, bool pinned = false);
        IILGen Do(Action<IILGen> action);
        IILGen Comment(string text);
        IILGen LdcI4(int value);
        IILGen Ldarg(ushort parameterIndex);
        IILGen Ldfld(FieldInfo fieldInfo);
        IILGen Ldflda(FieldInfo fieldInfo);
        IILGen Stfld(FieldInfo fieldInfo);
        IILGen Stloc(ushort localVariableIndex);
        IILGen Stloc(IILLocal localBuilder);
        IILGen Ldloc(ushort localVariableIndex);
        IILGen Ldloc(IILLocal localBuilder);
        IILGen Ldloca(IILLocal localBuilder);
        IILGen Mark(IILLabel label);
        IILGen Brfalse(IILLabel targetLabel);
        IILGen BrfalseS(IILLabel targetLabel);
        IILGen Brtrue(IILLabel targetLabel);
        IILGen BrtrueS(IILLabel targetLabel);
        IILGen Br(IILLabel targetLabel);
        IILGen BrS(IILLabel targetLabel);
        IILGen BneUnS(IILLabel targetLabel);
        IILGen BeqS(IILLabel targetLabel);
        IILGen BgeUnS(IILLabel targetLabel);
        IILGen BgeUn(IILLabel targetLabel);
        IILGen Newobj(ConstructorInfo constructorInfo);
        IILGen Callvirt(MethodInfo methodInfo);
        IILGen Call(MethodInfo methodInfo);
        IILGen Call(ConstructorInfo constructorInfo);
        IILGen Ldftn(MethodInfo methodInfo);
        IILGen Ldftn(IILMethod method);
        IILGen Ldstr(string str);
        IILGen Ldnull();
        IILGen Throw();
        IILGen Ret();
        IILGen Pop();
        IILGen Castclass(Type toType);
        IILGen Isinst(Type asType);
        IILGen ConvU1();
        IILGen ConvU2();
        IILGen ConvU4();
        IILGen ConvU8();
        IILGen ConvI1();
        IILGen ConvI2();
        IILGen ConvI4();
        IILGen ConvI8();
        IILGen ConvR4();
        IILGen ConvR8();
        IILGen Tail();
        IILGen LdelemRef();
        IILGen Try();
        IILGen Catch(Type exceptionType);
        IILGen Finally();
        IILGen EndTry();
        IILGen Add();
        IILGen Sub();
        IILGen Mul();
        IILGen Div();
        IILGen Dup();
        IILGen Ldtoken(Type type);
    }
}