using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class DateTimeFieldHandler : SimpleFieldHandlerBase
    {
        public DateTimeFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDateTime()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDateTime()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDateTime(new DateTime())))
        {
        }

        public override string Name
        {
            get { return "DateTime"; }
        }
    }
}