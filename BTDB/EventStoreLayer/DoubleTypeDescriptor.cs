using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class DoubleTypeDescriptor : SimpleTypeDescriptor
    {
        public DoubleTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDouble()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDouble()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDouble(0)))
        {
        }

        public override string Name
        {
            get { return "Double"; }
        }
    }
}