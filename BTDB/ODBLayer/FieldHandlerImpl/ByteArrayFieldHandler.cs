using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class ByteArrayFieldHandler : SimpleFieldHandlerBase
    {
        public ByteArrayFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadByteArray()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipByteArray()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteByteArray(null)))
        {
        }

        public override string Name
        {
            get { return "Byte[]"; }
        }
    }
}