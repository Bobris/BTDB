using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    class ILConstructorImpl : IILMethod
    {
        readonly ConstructorBuilder _constructor;
        int _expectedLength;
        IILGen _gen;
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILConstructorImpl(ConstructorBuilder constructor, IILGenForbidenInstructions forbidenInstructions)
        {
            _constructor = constructor;
            _forbidenInstructions = forbidenInstructions;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator => _gen ?? (_gen = new ILGenImpl(_constructor.GetILGenerator(_expectedLength), _forbidenInstructions));
    }
}