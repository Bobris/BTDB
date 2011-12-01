using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILConstructorDebugImpl : IILMethod
    {
        readonly ConstructorBuilder _constructor;
        readonly SourceCodeWriter _sourceCodeWriter;
        int _expectedLength;
        IILGen _gen;
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILConstructorDebugImpl(ConstructorBuilder constructor, IILGenForbidenInstructions forbidenInstructions, SourceCodeWriter sourceCodeWriter)
        {
            _constructor = constructor;
            _sourceCodeWriter = sourceCodeWriter;
            _forbidenInstructions = forbidenInstructions;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenDebugImpl(_constructor.GetILGenerator(_expectedLength), _forbidenInstructions, _sourceCodeWriter)); }
        }
    }
}