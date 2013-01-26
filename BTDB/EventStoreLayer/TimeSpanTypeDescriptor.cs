using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class TimeSpanTypeDescriptor : SimpleTypeDescriptor
    {
        public TimeSpanTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadTimeSpan()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipTimeSpan()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteTimeSpan(new TimeSpan())))
        {
        }

        public override string Name
        {
            get { return "TimeSpan"; }
        }
    }
}