using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class GuidFieldHandler : SimpleFieldHandlerBase
    {
        public GuidFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadGuid()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipGuid()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteGuid(new Guid())))
        {
        }

        public override string Name
        {
            get { return "Guid"; }
        }
    }
}