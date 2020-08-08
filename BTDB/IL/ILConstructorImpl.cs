using System.Reflection.Emit;

namespace BTDB.IL
{
    class ILConstructorImpl : IILMethod
    {
        readonly ConstructorBuilder _constructor;
        int _expectedLength;
        IILGen? _gen;
        readonly IILGenForbiddenInstructions _forbiddenInstructions;

        public ILConstructorImpl(ConstructorBuilder constructor, IILGenForbiddenInstructions forbiddenInstructions)
        {
            _constructor = constructor;
            _forbiddenInstructions = forbiddenInstructions;
            _expectedLength = 64;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public bool InitLocals
        {
            get => _constructor.InitLocals;
            set => _constructor.InitLocals = value;
        }

        public IILGen Generator => _gen ??= new ILGenImpl(_constructor.GetILGenerator(_expectedLength), _forbiddenInstructions);
    }
}
