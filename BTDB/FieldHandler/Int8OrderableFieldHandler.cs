using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class Int8OrderableFieldHandler : SimpleFieldHandlerJustOrderableBase
    {
        public Int8OrderableFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadInt8Ordered()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipInt8Ordered()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteInt8Ordered(0)))
        {
        }

        public override string Name
        {
            get { return "Int8Orderable"; }
        }
    }
}