using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class TimeSpanFieldHandler : SimpleFieldHandlerBase
    {
        public TimeSpanFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadTimeSpan()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipTimeSpan()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteTimeSpan(new TimeSpan())))
        {
        }

        public override string Name
        {
            get { return "TimeSpan"; }
        }
    }
}