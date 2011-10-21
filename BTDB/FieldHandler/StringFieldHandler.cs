using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class StringFieldHandler : SimpleFieldOrderableHandlerBase
    {
        public StringFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadString()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipString()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteString(null)),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadStringOrdered()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipStringOrdered()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteStringOrdered(null)))
        {
        }

        public override string Name
        {
            get { return "String"; }
        }
    }
}