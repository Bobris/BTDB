using System;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class StringFieldHandler : SimpleFieldHandlerBase
    {
        public StringFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadString()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipString()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteString(null)))
        {
        }

        public override string Name
        {
            get { return "String"; }
        }
    }
}