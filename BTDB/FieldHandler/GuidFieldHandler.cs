using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class GuidFieldHandler : SimpleFieldOrderableHandlerBase
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