using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class StringOrderableFieldHandler : SimpleFieldHandlerJustOrderableBase
    {
        public StringOrderableFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadStringOrdered()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipStringOrdered()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteStringOrdered(null)))
        {
        }

        public override string Name
        {
            get { return "StringOrderable"; }
        }
    }
}