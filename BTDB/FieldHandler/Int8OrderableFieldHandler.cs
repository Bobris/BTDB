using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class Int8OrderableFieldHandler : SimpleFieldHandlerJustOrderableBase
    {
        public Int8OrderableFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadInt8Ordered()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipInt8Ordered()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteInt8Ordered(0)))
        {
        }

        public override string Name
        {
            get { return "Int8Orderable"; }
        }
    }
}