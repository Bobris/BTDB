using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILGenImpl : IILGen
    {
        readonly ILGenerator _ilGenerator;
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILGenImpl(ILGenerator ilGenerator, IILGenForbidenInstructions forbidenInstructions)
        {
            _ilGenerator = ilGenerator;
            _forbidenInstructions = forbidenInstructions;
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

            public Label Label
            {
                get { return _label; }
            }
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

            public int Index
            {
                get { return LocalBuilder.LocalIndex; }
            }

            public bool Pinned
            {
                get { return LocalBuilder.IsPinned; }
            }

            public Type LocalType
            {
                get { return LocalBuilder.LocalType; }
            }

            public LocalBuilder LocalBuilder
            {
                get { return _localBuilder; }
            }
        }

        public IILGen Do(Action<IILGen> action)
        {
            action(this);
            return this;
        }

        public IILGen Comment(string text)
        {
            return this;
        }

        public IILGen LdcI4(int value)
        {
            switch (value)
            {
                case 0:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_0);
                    break;
                case 1:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_1);
                    break;
                case 2:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_2);
                    break;
                case 3:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_3);
                    break;
                case 4:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_4);
                    break;
                case 5:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_5);
                    break;
                case 6:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_6);
                    break;
                case 7:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_7);
                    break;
                case 8:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_8);
                    break;
                case -1:
                    _ilGenerator.Emit(OpCodes.Ldc_I4_M1);
                    break;
                default:
                    if (value >= -128 && value <= 127)
                        _ilGenerator.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                    else
                        _ilGenerator.Emit(OpCodes.Ldc_I4, value);
                    break;
            }
            return this;
        }

        public IILGen Ldarg(ushort parameterIndex)
        {
            switch (parameterIndex)
            {
                case 0:
                    _ilGenerator.Emit(OpCodes.Ldarg_0);
                    break;
                case 1:
                    _ilGenerator.Emit(OpCodes.Ldarg_1);
                    break;
                case 2:
                    _ilGenerator.Emit(OpCodes.Ldarg_2);
                    break;
                case 3:
                    _ilGenerator.Emit(OpCodes.Ldarg_3);
                    break;
                default:
                    if (parameterIndex <= 255)
                        _ilGenerator.Emit(OpCodes.Ldarg_S, (byte)parameterIndex);
                    else
                        _ilGenerator.Emit(OpCodes.Ldarg, parameterIndex);
                    break;
            }
            return this;
        }

        public IILGen Ldfld(FieldInfo fieldInfo)
        {
            _ilGenerator.Emit(OpCodes.Ldfld, fieldInfo);
            return this;
        }

        public IILGen Ldflda(FieldInfo fieldInfo)
        {
            _ilGenerator.Emit(OpCodes.Ldflda, fieldInfo);
            return this;
        }

        public IILGen Ldsfld(FieldInfo fieldInfo)
        {
            _ilGenerator.Emit(OpCodes.Ldsfld, fieldInfo);
            return this;
        }

        public IILGen Stfld(FieldInfo fieldInfo)
        {
            _ilGenerator.Emit(OpCodes.Stfld, fieldInfo);
            return this;
        }

        public IILGen Stsfld(FieldInfo fieldInfo)
        {
            _ilGenerator.Emit(OpCodes.Stsfld, fieldInfo);
            return this;
        }

        public IILGen Stloc(ushort localVariableIndex)
        {
            switch (localVariableIndex)
            {
                case 0:
                    _ilGenerator.Emit(OpCodes.Stloc_0);
                    break;
                case 1:
                    _ilGenerator.Emit(OpCodes.Stloc_1);
                    break;
                case 2:
                    _ilGenerator.Emit(OpCodes.Stloc_2);
                    break;
                case 3:
                    _ilGenerator.Emit(OpCodes.Stloc_3);
                    break;
                case 65535:
                    throw new ArgumentOutOfRangeException("localVariableIndex");
                default:
                    if (localVariableIndex <= 255)
                        _ilGenerator.Emit(OpCodes.Stloc_S, (byte)localVariableIndex);
                    else
                        _ilGenerator.Emit(OpCodes.Stloc, localVariableIndex);
                    break;
            }
            return this;
        }

        public IILGen Stloc(IILLocal localBuilder)
        {
            _ilGenerator.Emit(OpCodes.Stloc, ((ILLocalImpl)localBuilder).LocalBuilder);
            return this;
        }

        public IILGen Ldloc(ushort localVariableIndex)
        {
            switch (localVariableIndex)
            {
                case 0:
                    _ilGenerator.Emit(OpCodes.Ldloc_0);
                    break;
                case 1:
                    _ilGenerator.Emit(OpCodes.Ldloc_1);
                    break;
                case 2:
                    _ilGenerator.Emit(OpCodes.Ldloc_2);
                    break;
                case 3:
                    _ilGenerator.Emit(OpCodes.Ldloc_3);
                    break;
                case 65535:
                    throw new ArgumentOutOfRangeException("localVariableIndex");
                default:
                    if (localVariableIndex <= 255)
                        _ilGenerator.Emit(OpCodes.Ldloc_S, (byte)localVariableIndex);
                    else
                        _ilGenerator.Emit(OpCodes.Ldloc, localVariableIndex);
                    break;
            }
            return this;
        }

        public IILGen Ldloc(IILLocal localBuilder)
        {
            _ilGenerator.Emit(OpCodes.Ldloc, ((ILLocalImpl)localBuilder).LocalBuilder);
            return this;
        }

        public IILGen Ldloca(IILLocal localBuilder)
        {
            _ilGenerator.Emit(OpCodes.Ldloca, ((ILLocalImpl)localBuilder).LocalBuilder);
            return this;
        }

        public IILGen Mark(IILLabel label)
        {
            _ilGenerator.MarkLabel(((ILLabelImpl)label).Label);
            return this;
        }

        public IILGen Brfalse(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Brfalse, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen BrfalseS(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Brfalse_S, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen Brtrue(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Brtrue, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen BrtrueS(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Brtrue_S, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen Br(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Br, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen BrS(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Br_S, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen BneUnS(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Bne_Un_S, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen BeqS(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Beq_S, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen BgeUnS(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Bge_Un_S, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen BgeUn(IILLabel targetLabel)
        {
            _ilGenerator.Emit(OpCodes.Bge_Un, ((ILLabelImpl)targetLabel).Label);
            return this;
        }

        public IILGen Newobj(ConstructorInfo constructorInfo)
        {
            _ilGenerator.Emit(OpCodes.Newobj, constructorInfo);
            return this;
        }

        public IILGen Callvirt(MethodInfo methodInfo)
        {
            if (methodInfo.IsStatic) throw new ArgumentException("Method in Callvirt cannot be static");
            _forbidenInstructions.Emit(_ilGenerator, OpCodes.Callvirt, methodInfo);
            return this;
        }

        public IILGen Call(MethodInfo methodInfo)
        {
            _forbidenInstructions.Emit(_ilGenerator, OpCodes.Call, methodInfo);
            return this;
        }

        public IILGen Call(ConstructorInfo constructorInfo)
        {
            _ilGenerator.Emit(OpCodes.Call, constructorInfo);
            return this;
        }

        public IILGen Ldftn(MethodInfo methodInfo)
        {
            _ilGenerator.Emit(OpCodes.Ldftn, methodInfo);
            return this;
        }

        public IILGen Ldftn(IILMethod method)
        {
            _ilGenerator.Emit(OpCodes.Ldftn, ((ILMethodImpl)method).MethodInfo);
            return this;
        }

        public IILGen Ldstr(string str)
        {
            _ilGenerator.Emit(OpCodes.Ldstr, str);
            return this;
        }

        public IILGen Ldnull()
        {
            _ilGenerator.Emit(OpCodes.Ldnull);
            return this;
        }

        public IILGen Throw()
        {
            _ilGenerator.Emit(OpCodes.Throw);
            return this;
        }

        public IILGen Ret()
        {
            _ilGenerator.Emit(OpCodes.Ret);
            return this;
        }

        public IILGen Pop()
        {
            _ilGenerator.Emit(OpCodes.Pop);
            return this;
        }

        public IILGen Castclass(Type toType)
        {
            _ilGenerator.Emit(OpCodes.Castclass,toType);
            return this;
        }

        public IILGen Isinst(Type asType)
        {
            _ilGenerator.Emit(OpCodes.Isinst, asType);
            return this;
        }

        public IILGen ConvU1()
        {
            _ilGenerator.Emit(OpCodes.Conv_U1);
            return this;
        }

        public IILGen ConvU2()
        {
            _ilGenerator.Emit(OpCodes.Conv_U2);
            return this;
        }

        public IILGen ConvU4()
        {
            _ilGenerator.Emit(OpCodes.Conv_U4);
            return this;
        }

        public IILGen ConvU8()
        {
            _ilGenerator.Emit(OpCodes.Conv_U8);
            return this;
        }

        public IILGen ConvI1()
        {
            _ilGenerator.Emit(OpCodes.Conv_I1);
            return this;
        }

        public IILGen ConvI2()
        {
            _ilGenerator.Emit(OpCodes.Conv_I2);
            return this;
        }

        public IILGen ConvI4()
        {
            _ilGenerator.Emit(OpCodes.Conv_I4);
            return this;
        }

        public IILGen ConvI8()
        {
            _ilGenerator.Emit(OpCodes.Conv_I8);
            return this;
        }

        public IILGen ConvR4()
        {
            _ilGenerator.Emit(OpCodes.Conv_R4);
            return this;
        }

        public IILGen ConvR8()
        {
            _ilGenerator.Emit(OpCodes.Conv_R8);
            return this;
        }

        public IILGen Tail()
        {
            _ilGenerator.Emit(OpCodes.Tailcall);
            return this;
        }

        public IILGen LdelemRef()
        {
            _ilGenerator.Emit(OpCodes.Ldelem_Ref);
            return this;
        }

        public IILGen StelemRef()
        {
            _ilGenerator.Emit(OpCodes.Stelem_Ref);
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

        public IILGen Add()
        {
            _ilGenerator.Emit(OpCodes.Add);
            return this;
        }

        public IILGen Sub()
        {
            _ilGenerator.Emit(OpCodes.Sub);
            return this;
        }

        public IILGen Mul()
        {
            _ilGenerator.Emit(OpCodes.Mul);
            return this;
        }

        public IILGen Div()
        {
            _ilGenerator.Emit(OpCodes.Div);
            return this;
        }

        public IILGen Dup()
        {
            _ilGenerator.Emit(OpCodes.Dup);
            return this;
        }

        public IILGen Ldtoken(Type type)
        {
            _ilGenerator.Emit(OpCodes.Ldtoken, type);
            return this;
        }
    }
}