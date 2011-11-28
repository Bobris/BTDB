using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class BoolFieldHandler : SimpleFieldHandlerBase
    {
        public BoolFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadBool()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipBool()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteBool(false)))
        {
        }

        public override string Name
        {
            get { return "Bool"; }
        }
    }
}