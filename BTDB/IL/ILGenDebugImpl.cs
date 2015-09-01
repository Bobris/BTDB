using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    class ILGenDebugImpl : IILGen
    {
        readonly ILGenerator _ilGenerator;
        readonly IILGenForbidenInstructions _forbidenInstructions;
        readonly SourceCodeWriter _sourceCodeWriter;
        int _labelCounter;

        public ILGenDebugImpl(ILGenerator ilGenerator, IILGenForbidenInstructions forbidenInstructions, SourceCodeWriter sourceCodeWriter)
        {
            _ilGenerator = ilGenerator;
            _forbidenInstructions = forbidenInstructions;
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

        public IILGen Comment(string text)
        {
            _sourceCodeWriter.WriteLine("// " + text);
            return this;
        }

        public void Emit(OpCode opCode)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, opCode.ToString());
            _ilGenerator.Emit(opCode);
        }

        public void Emit(OpCode opCode, sbyte param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        public void Emit(OpCode opCode, byte param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        public void Emit(OpCode opCode, ushort param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        public void Emit(OpCode opCode, int param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, param));
            _ilGenerator.Emit(opCode, param);
        }

        public void Emit(OpCode opCode, FieldInfo param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} field {1} {2}", opCode, param.FieldType.ToSimpleName(), param.Name));
            _ilGenerator.Emit(opCode, param);
        }

        public void Emit(OpCode opCode, ConstructorInfo param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} ctor {1}({2})", opCode, param.DeclaringType.ToSimpleName(), FormatParams(param.GetParameters())));
            _forbidenInstructions.Emit(_ilGenerator, opCode, param);
        }

        public void Emit(OpCode opCode, MethodInfo param)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {3} {1}({2})", opCode, param.Name, FormatParams(param.GetParameters()), param.ReturnType.ToSimpleName()));
            _forbidenInstructions.Emit(_ilGenerator, opCode, param);
        }

        public void Emit(OpCode opCode, Type type)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, type.ToSimpleName()));
            _ilGenerator.Emit(opCode, type);
        }

        static string FormatParams(IEnumerable<ParameterInfo> pars)
        {
            return string.Join(", ", pars.Select(par => string.Format("{0} {1}", par.ParameterType.ToSimpleName(), par.Name)));
        }

        public void Emit(OpCode opCode, IILLocal ilLocal)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1} {3} {2}", opCode, ilLocal.Index, ((ILLocalImpl)ilLocal).Name, ilLocal.LocalType.ToSimpleName()));
            _ilGenerator.Emit(opCode, ((ILLocalImpl)ilLocal).LocalBuilder);
        }

        public void Emit(OpCode opCode, IILLabel ilLabel)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {1}", opCode, ((ILLabelImpl)ilLabel).Name));
            _ilGenerator.Emit(opCode, ((ILLabelImpl)ilLabel).Label);
        }

        public IILGen Mark(IILLabel label)
        {
            _sourceCodeWriter.Indent--;
            _sourceCodeWriter.WriteLine(((ILLabelImpl)label).Name + ":");
            _sourceCodeWriter.Indent++;
            _ilGenerator.MarkLabel(((ILLabelImpl)label).Label);
            return this;
        }

        public IILGen Ldftn(IILMethod method)
        {
            var meth = ((ILMethodDebugImpl)method);
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} {2} {1}({3})",
                OpCodes.Ldftn,
                meth.Name,
                meth.ReturnType.ToSimpleName(),
                string.Join(", ", meth.Parameters.Select(p => p.ToSimpleName()))));
            _ilGenerator.Emit(OpCodes.Ldftn, meth.MethodInfo);
            return this;
        }

        public IILGen Ldstr(string str)
        {
            _sourceCodeWriter.MarkAndWriteLine(_ilGenerator, string.Format("{0} \"{1}\"", OpCodes.Ldstr, str));
            _ilGenerator.Emit(OpCodes.Ldstr, str);
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
    }
}
