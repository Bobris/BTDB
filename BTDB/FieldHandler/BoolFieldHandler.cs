using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class BoolFieldHandler : SimpleFieldOrderableHandlerBase
    {
        public BoolFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadBool()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipBool()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteBool(false)))
        {
        }

        public override string Name
        {
            get { return "Bool"; }
        }
    }
}