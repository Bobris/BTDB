using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class VUInt16TypeDescriptor : SimpleTypeDescriptor
    {
        public VUInt16TypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVUInt16()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVUInt16()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVUInt16(0)))
        {
        }

        public override string Name
        {
            get { return "VUInt16"; }
        }
    }
}