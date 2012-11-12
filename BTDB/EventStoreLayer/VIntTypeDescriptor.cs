using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class VIntTypeDescriptor : SimpleTypeDescriptor
    {
        public VIntTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVInt64()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVInt64()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVInt64(0)))
        {
        }

        public override string Name
        {
            get { return "VInt"; }
        }
    }
}