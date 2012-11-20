using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class VUInt64TypeDescriptor : SimpleTypeDescriptor
    {
        public VUInt64TypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVUInt64()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVUInt64()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVUInt64(0)))
        {
        }

        public override string Name
        {
            get { return "VUInt64"; }
        }
    }
}