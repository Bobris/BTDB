using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILMethodImpl : IILMethod
    {
        int _expectedLength;
        IILGen _gen;
        readonly MethodBuilder _method;

        public ILMethodImpl(MethodBuilder method)
        {
            _method = method;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_method.GetILGenerator(_expectedLength))); }
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