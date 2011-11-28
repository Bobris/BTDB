using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class DecimalFieldHandler : SimpleFieldHandlerBase
    {
        public DecimalFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDecimal()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDecimal()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDecimal(0)))
        {
        }

        public override string Name
        {
            get { return "Decimal"; }
        }
    }
}