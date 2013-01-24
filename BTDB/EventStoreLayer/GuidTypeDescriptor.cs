using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class GuidTypeDescriptor : SimpleTypeDescriptor
    {
        public GuidTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadGuid()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipGuid()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteGuid(Guid.Empty)))
        {
        }

        public override string Name
        {
            get { return "Guid"; }
        }
    }
}