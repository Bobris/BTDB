using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILMethodImpl : IILMethod
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

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_method.GetILGenerator(_expectedLength), _forbidenInstructions)); }
        }

        public MethodInfo MethodInfo
        {
            get { return _method; }
        }

        public MethodBuilder MethodBuilder
        {
            get { return _method; }
        }
    }
}