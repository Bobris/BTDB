using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class Uint8TypeDescriptor : SimpleTypeDescriptor
    {
        public Uint8TypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadUInt8()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipUInt8()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteUInt8(0)))
        {
        }

        public override string Name
        {
            get { return "UInt8"; }
        }
    }
}