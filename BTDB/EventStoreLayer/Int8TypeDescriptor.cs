using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class Int8TypeDescriptor : SimpleTypeDescriptor
    {
        public Int8TypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadInt8()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipInt8()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteInt8(0)))
        {
        }

        public override string Name
        {
            get { return "Int8"; }
        }
    }
}