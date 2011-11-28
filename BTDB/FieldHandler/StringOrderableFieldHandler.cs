using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class StringOrderableFieldHandler : SimpleFieldHandlerJustOrderableBase
    {
        public StringOrderableFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadStringOrdered()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipStringOrdered()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteStringOrdered(null)))
        {
        }

        public override string Name
        {
            get { return "StringOrderable"; }
        }
    }
}