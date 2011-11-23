using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILConstructorDebugImpl : IILMethod
    {
        readonly ConstructorBuilder _constructor;
        readonly SourceCodeWriter _sourceCodeWriter;
        int _expectedLength;
        IILGen _gen;

        public ILConstructorDebugImpl(ConstructorBuilder constructor, SourceCodeWriter sourceCodeWriter)
        {
            _constructor = constructor;
            _sourceCodeWriter = sourceCodeWriter;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenDebugImpl(_constructor.GetILGenerator(_expectedLength), _sourceCodeWriter)); }
        }
    }
}