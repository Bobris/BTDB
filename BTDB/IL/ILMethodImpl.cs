using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    class ILMethodImpl : IILMethodPrivate
    {
        int _expectedLength;
        IILGen _gen;
        readonly MethodBuilder _method;
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILMethodImpl(MethodBuilder method, IILGenForbidenInstructions forbidenInstructions)
        {
            _method = method;
            _forbidenInstructions = forbidenInstructions;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator => _gen ?? (_gen = new ILGenImpl(_method.GetILGenerator(_expectedLength), _forbidenInstructions));

        public MethodInfo TrueMethodInfo => _method;

        public MethodBuilder MethodBuilder => _method;
    }
}