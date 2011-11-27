using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILGenDebugImpl : IILGen
    {
        readonly ILGenerator _ilGenerator;
        readonly SourceCodeWriter _sourceCodeWriter;
        int _labelCounter;

        public ILGenDebugImpl(ILGenerator ilGenerator, SourceCodeWriter sourceCodeWriter)
        {
            _ilGenerator = ilGenerator;
            _sourceCodeWriter = sourceCodeWriter;
        }

        public IILLabel DefineLabel(string name)
        {
            if (name == null)
            {
                _labelCounter++;
                name = "label" + _labelCounter;
            }
            return new ILLabelImpl(_ilGenerator.DefineLabel(), name);
        }

        class ILLabelImpl : IILLabel
        {
            readonly Label _label;
            readonly string _name;

            public ILLabelImpl(Label label, string name)
            {
                _label = label;
                _name = name;
            }

            public Label Label
            {
                get { return _label; }
            }

            public string Name
            {
                get { return _name; }
            }
        }

        public IILLocal DeclareLocal(Type type, string name, bool pinned = false)
        {
            var localBuilder = _ilGenerator.DeclareLocal(type, pinned);
            if (name == null)
            {
                name = "local" + localBuilder.LocalIndex;
            }
            localBuilder.SetLocalSymInfo(name);
            _sourceCodeWriter.WriteLine(string.Format("{3}{0} {1} // index {2}", type.ToSimpleName(), name, localBuilder.LocalIndex, pinned ? "pinned " : ""));
            return new ILLocalImpl(localBuilder, name);
        }

        class ILLocalImpl : IILLocal
        {
            readonly LocalBuilder _localBuilder;
            readonly string _name;

            public ILLocalImpl(LocalBuilder localBuilder, string name)
            {
                _localBuilder = localBuilder;
                _name = name;
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

            public string Name
            {
                get { return _name; }
            }
        }

        public IILGen Do(Action<IILGen> action)
        {
            action(this);
            return this;
        }

        public IILGen Comment(string text)
        {
            _sourceCodeWriter.WriteLine("// " + text);
            return this;
        }

        void Emit(OpCode opCode)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, opCode.ToString());
            _ilGenerator.Emit(opCode);
        }

        void Emit(OpCode opCode, sbyte param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        void Emit(OpCode opCode, byte param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        void Emit(OpCode opCode, ushort param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        void Emit(OpCode opCode, int param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        void Emit(OpCode opCode, FieldInfo param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} field {1} {2}", opCode, param.FieldType.ToSimpleName(), param.Name));
            _ilGenerator.Emit(opCode, param);
        }

        void Emit(OpCode opCode, ConstructorInfo param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} ctor {1}({2})", opCode, param.DeclaringType.ToSimpleName(), FormatParams(param.GetParameters())));
            _ilGenerator.Emit(opCode, param);
        }

        void Emit(OpCode opCode, MethodInfo param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {3} {1}({2})", opCode, param.Name, FormatParams(param.GetParameters()), param.ReturnType.ToSimpleName()));
            _ilGenerator.Emit(opCode, param);
        }

        void Emit(OpCode opCode, Type type)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, type.ToSimpleName()));
            _ilGenerator.Emit(opCode, type);
        }

        static string FormatParams(IEnumerable<ParameterInfo> pars)
        {
            return string.Join(", ", pars.Select(par => string.Format("{0} {1}", par.ParameterType.ToSimpleName(), par.Name)));
        }

        void Emit(OpCode opCode, IILLocal ilLocal)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1} {3} {2}", opCode, ilLocal.Index, ((ILLocalImpl)ilLocal).Name, ilLocal.LocalType.ToSimpleName()));
            _ilGenerator.Emit(opCode, ((ILLocalImpl)ilLocal).LocalBuilder);
        }

        void Emit(OpCode opCode, IILLabel ilLabel)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, ((ILLabelImpl)ilLabel).Name));
            _ilGenerator.Emit(opCode, ((ILLabelImpl)ilLabel).Label);
        }

        public IILGen LdcI4(int value)
        {
            switch (value)
            {
                case 0:
                    Emit(OpCodes.Ldc_I4_0);
                    break;
                case 1:
                    Emit(OpCodes.Ldc_I4_1);
                    break;
                case 2:
                    Emit(OpCodes.Ldc_I4_2);
                    break;
                case 3:
                    Emit(OpCodes.Ldc_I4_3);
                    break;
                case 4:
                    Emit(OpCodes.Ldc_I4_4);
                    break;
                case 5:
                    Emit(OpCodes.Ldc_I4_5);
                    break;
                case 6:
                    Emit(OpCodes.Ldc_I4_6);
                    break;
                case 7:
                    Emit(OpCodes.Ldc_I4_7);
                    break;
                case 8:
                    Emit(OpCodes.Ldc_I4_8);
                    break;
                case -1:
                    Emit(OpCodes.Ldc_I4_M1);
                    break;
                default:
                    if (value >= -128 && value <= 127)
                        Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                    else
                        Emit(OpCodes.Ldc_I4, value);
                    break;
            }
            return this;
        }

        public IILGen Ldarg(ushort parameterIndex)
        {
            switch (parameterIndex)
            {
                case 0:
                    Emit(OpCodes.Ldarg_0);
                    break;
                case 1:
                    Emit(OpCodes.Ldarg_1);
                    break;
                case 2:
                    Emit(OpCodes.Ldarg_2);
                    break;
                case 3:
                    Emit(OpCodes.Ldarg_3);
                    break;
                default:
                    if (parameterIndex <= 255)
                        Emit(OpCodes.Ldarg_S, (byte)parameterIndex);
                    else
                        Emit(OpCodes.Ldarg, parameterIndex);
                    break;
            }
            return this;
        }

        public IILGen Ldfld(FieldInfo fieldInfo)
        {
            Emit(OpCodes.Ldfld, fieldInfo);
            return this;
        }

        public IILGen Ldflda(FieldInfo fieldInfo)
        {
            Emit(OpCodes.Ldflda, fieldInfo);
            return this;
        }

        public IILGen Stfld(FieldInfo fieldInfo)
        {
            Emit(OpCodes.Stfld, fieldInfo);
            return this;
        }

        public IILGen Stloc(ushort localVariableIndex)
        {
            switch (localVariableIndex)
            {
                case 0:
                    Emit(OpCodes.Stloc_0);
                    break;
                case 1:
                    Emit(OpCodes.Stloc_1);
                    break;
                case 2:
                    Emit(OpCodes.Stloc_2);
                    break;
                case 3:
                    Emit(OpCodes.Stloc_3);
                    break;
                case 65535:
                    throw new ArgumentOutOfRangeException("localVariableIndex");
                default:
                    if (localVariableIndex <= 255)
                        Emit(OpCodes.Stloc_S, (byte)localVariableIndex);
                    else
                        Emit(OpCodes.Stloc, localVariableIndex);
                    break;
            }
            return this;
        }

        public IILGen Stloc(IILLocal localBuilder)
        {
            Emit(OpCodes.Stloc, localBuilder);
            return this;
        }

        public IILGen Ldloc(ushort localVariableIndex)
        {
            switch (localVariableIndex)
            {
                case 0:
                    Emit(OpCodes.Ldloc_0);
                    break;
                case 1:
                    Emit(OpCodes.Ldloc_1);
                    break;
                case 2:
                    Emit(OpCodes.Ldloc_2);
                    break;
                case 3:
                    Emit(OpCodes.Ldloc_3);
                    break;
                case 65535:
                    throw new ArgumentOutOfRangeException("localVariableIndex");
                default:
                    if (localVariableIndex <= 255)
                        Emit(OpCodes.Ldloc_S, (byte)localVariableIndex);
                    else
                        Emit(OpCodes.Ldloc, localVariableIndex);
                    break;
            }
            return this;
        }

        public IILGen Ldloc(IILLocal localBuilder)
        {
            Emit(OpCodes.Ldloc, localBuilder);
            return this;
        }

        public IILGen Ldloca(IILLocal localBuilder)
        {
            Emit(OpCodes.Ldloca, localBuilder);
            return this;
        }

        public IILGen Mark(IILLabel label)
        {
            _sourceCodeWriter.Indent--;
            _sourceCodeWriter.WriteLine(((ILLabelImpl)label).Name + ":");
            _sourceCodeWriter.Indent++;
            _ilGenerator.MarkLabel(((ILLabelImpl)label).Label);
            return this;
        }

        public IILGen Brfalse(IILLabel targetLabel)
        {
            Emit(OpCodes.Brfalse, targetLabel);
            return this;
        }

        public IILGen BrfalseS(IILLabel targetLabel)
        {
            Emit(OpCodes.Brfalse_S, targetLabel);
            return this;
        }

        public IILGen Brtrue(IILLabel targetLabel)
        {
            Emit(OpCodes.Brtrue, targetLabel);
            return this;
        }

        public IILGen BrtrueS(IILLabel targetLabel)
        {
            Emit(OpCodes.Brtrue_S, targetLabel);
            return this;
        }

        public IILGen Br(IILLabel targetLabel)
        {
            Emit(OpCodes.Br, targetLabel);
            return this;
        }

        public IILGen BrS(IILLabel targetLabel)
        {
            Emit(OpCodes.Br_S, targetLabel);
            return this;
        }

        public IILGen BneUnS(IILLabel targetLabel)
        {
            Emit(OpCodes.Bne_Un_S, targetLabel);
            return this;
        }

        public IILGen BeqS(IILLabel targetLabel)
        {
            Emit(OpCodes.Beq_S, targetLabel);
            return this;
        }

        public IILGen BgeUnS(IILLabel targetLabel)
        {
            Emit(OpCodes.Bge_Un_S, targetLabel);
            return this;
        }

        public IILGen BgeUn(IILLabel targetLabel)
        {
            Emit(OpCodes.Bge_Un, targetLabel);
            return this;
        }

        public IILGen Newobj(ConstructorInfo constructorInfo)
        {
            Emit(OpCodes.Newobj, constructorInfo);
            return this;
        }

        public IILGen Callvirt(MethodInfo methodInfo)
        {
            if (methodInfo.IsStatic) throw new ArgumentException("Method in Callvirt cannot be static");
            Emit(OpCodes.Callvirt, methodInfo);
            return this;
        }

        public IILGen Call(MethodInfo methodInfo)
        {
            Emit(OpCodes.Call, methodInfo);
            return this;
        }

        public IILGen Call(ConstructorInfo constructorInfo)
        {
            Emit(OpCodes.Call, constructorInfo);
            return this;
        }

        public IILGen Ldftn(MethodInfo methodInfo)
        {
            Emit(OpCodes.Ldftn, methodInfo);
            return this;
        }

        public IILGen Ldftn(IILMethod method)
        {
            var meth = ((ILMethodDebugImpl)method);
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {2} {1}({3})",
                OpCodes.Ldftn,
                meth.Name,
                meth.ReturnType.ToSimpleName(),
                string.Join(", ",meth.Parameters.Select(p=>p.ToSimpleName()))));
            _ilGenerator.Emit(OpCodes.Ldftn, meth.MethodInfo);
            return this;
        }

        public IILGen Ldstr(string str)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} \"{1}\"", OpCodes.Ldstr, str));
            _ilGenerator.Emit(OpCodes.Ldstr, str);
            return this;
        }

        public IILGen Ldnull()
        {
            Emit(OpCodes.Ldnull);
            return this;
        }

        public IILGen Throw()
        {
            Emit(OpCodes.Throw);
            return this;
        }

        public IILGen Ret()
        {
            Emit(OpCodes.Ret);
            return this;
        }

        public IILGen Pop()
        {
            Emit(OpCodes.Pop);
            return this;
        }

        public IILGen Castclass(Type toType)
        {
            Emit(OpCodes.Castclass, toType);
            return this;
        }

        public IILGen Isinst(Type asType)
        {
            Emit(OpCodes.Isinst, asType);
            return this;
        }

        public IILGen ConvU1()
        {
            Emit(OpCodes.Conv_U1);
            return this;
        }

        public IILGen ConvU2()
        {
            Emit(OpCodes.Conv_U2);
            return this;
        }

        public IILGen ConvU4()
        {
            Emit(OpCodes.Conv_U4);
            return this;
        }

        public IILGen ConvU8()
        {
            Emit(OpCodes.Conv_U8);
            return this;
        }

        public IILGen ConvI1()
        {
            Emit(OpCodes.Conv_I1);
            return this;
        }

        public IILGen ConvI2()
        {
            Emit(OpCodes.Conv_I2);
            return this;
        }

        public IILGen ConvI4()
        {
            Emit(OpCodes.Conv_I4);
            return this;
        }

        public IILGen ConvI8()
        {
            Emit(OpCodes.Conv_I8);
            return this;
        }

        public IILGen ConvR4()
        {
            Emit(OpCodes.Conv_R4);
            return this;
        }

        public IILGen ConvR8()
        {
            Emit(OpCodes.Conv_R8);
            return this;
        }

        public IILGen Tail()
        {
            Emit(OpCodes.Tailcall);
            return this;
        }

        public IILGen LdelemRef()
        {
            Emit(OpCodes.Ldelem_Ref);
            return this;
        }

        public IILGen Try()
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, "try");
            _sourceCodeWriter.OpenScope();
            _ilGenerator.BeginExceptionBlock();
            return this;
        }

        public IILGen Catch(Type exceptionType)
        {
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("catch ({0})", exceptionType.FullName));
            _sourceCodeWriter.OpenScope();
            _ilGenerator.BeginCatchBlock(exceptionType);
            return this;
        }

        public IILGen Finally()
        {
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, "finally");
            _sourceCodeWriter.OpenScope();
            _ilGenerator.BeginFinallyBlock();
            return this;
        }

        public IILGen EndTry()
        {
            _sourceCodeWriter.CloseScope();
            _ilGenerator.EndExceptionBlock();
            return this;
        }

        public IILGen Add()
        {
            Emit(OpCodes.Add);
            return this;
        }

        public IILGen Sub()
        {
            Emit(OpCodes.Sub);
            return this;
        }

        public IILGen Mul()
        {
            Emit(OpCodes.Mul);
            return this;
        }

        public IILGen Div()
        {
            Emit(OpCodes.Div);
            return this;
        }

        public IILGen Dup()
        {
            Emit(OpCodes.Dup);
            return this;
        }

        public IILGen Ldtoken(Type type)
        {
            Emit(OpCodes.Ldtoken, type);
            return this;
        }
    }
}
