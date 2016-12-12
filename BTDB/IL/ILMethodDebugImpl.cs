using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    class ILMethodDebugImpl : IILMethodPrivate
    {
        int _expectedLength;
        IILGen _gen;
        readonly MethodBuilder _method;
        readonly SourceCodeWriter _sourceCodeWriter;
        readonly string _name;
        readonly Type _returnType;
        readonly Type[] _parameters;
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILMethodDebugImpl(MethodBuilder method, SourceCodeWriter sourceCodeWriter, string name, Type returnType, Type[] parameters, IILGenForbidenInstructions forbidenInstructions)
        {
            _method = method;
            _sourceCodeWriter = sourceCodeWriter;
            _name = name;
            _returnType = returnType;
            _parameters = parameters;
            _forbidenInstructions = forbidenInstructions;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator => _gen ?? (_gen = new ILGenDebugImpl(_method.GetILGenerator(_expectedLength), _forbidenInstructions, _sourceCodeWriter));

        public string Name => _name;

        public MethodInfo TrueMethodInfo => _method;

        public MethodBuilder MethodBuilder => _method;

        public Type ReturnType => _returnType;

        public Type[] Parameters => _parameters;
    }
}