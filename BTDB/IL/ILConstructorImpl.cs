using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILConstructorImpl : IILMethod
    {
        readonly ConstructorBuilder _constructor;
        int _expectedLength;
        IILGen _gen;

        public ILConstructorImpl(ConstructorBuilder constructor)
        {
            _constructor = constructor;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_constructor.GetILGenerator(_expectedLength))); }
        }
    }
}