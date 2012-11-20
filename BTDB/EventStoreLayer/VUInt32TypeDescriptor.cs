using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class VUInt32TypeDescriptor : SimpleTypeDescriptor
    {
        public VUInt32TypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVUInt32()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVUInt32()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVUInt32(0)))
        {
        }

        public override string Name
        {
            get { return "VUInt32"; }
        }
    }
}