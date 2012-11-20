using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class VInt32TypeDescriptor : SimpleTypeDescriptor
    {
        public VInt32TypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVInt32()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVInt32()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVInt32(0)))
        {
        }

        public override string Name
        {
            get { return "VInt32"; }
        }
    }
}