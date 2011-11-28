using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class ByteArrayFieldHandler : SimpleFieldHandlerBase
    {
        public ByteArrayFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadByteArray()),
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