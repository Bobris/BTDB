using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class BoolFieldHandler : SimpleFieldHandlerBase
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