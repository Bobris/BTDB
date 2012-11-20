using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class VInt16TypeDescriptor : SimpleTypeDescriptor
    {
        public VInt16TypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVInt16()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVInt16()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVInt16(0)))
        {
        }

        public override string Name
        {
            get { return "VInt16"; }
        }
    }
}