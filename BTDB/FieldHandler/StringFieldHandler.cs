using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class StringFieldHandler : SimpleFieldHandlerBase
    {
        public StringFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadString()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipString()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteString(null)))
        {
        }

        public override string Name
        {
            get { return "String"; }
        }
    }
}