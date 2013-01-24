using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class ByteArrayTypeDescriptor : SimpleTypeDescriptor
    {
        public ByteArrayTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadByteArray()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipByteArray()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteByteArray(null)))
        {
        }

        public override string Name
        {
            get { return "Byte[]"; }
        }
    }
}