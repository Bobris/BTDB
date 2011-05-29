using System;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class GuidFieldHandler : SimpleFieldHandlerBase
    {
        public GuidFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadGuid()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipGuid()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteGuid(new Guid())))
        {
        }

        public override string Name
        {
            get { return "Guid"; }
        }
    }
}