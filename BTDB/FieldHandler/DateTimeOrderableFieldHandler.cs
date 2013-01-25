using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class DateTimeOrderableFieldHandler : SimpleFieldHandlerJustOrderableBase
    {
        public DateTimeOrderableFieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDateTime()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDateTime()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDateTimeForbidUnspecifiedKind(new DateTime())))
        {
        }

        public override string Name
        {
            get { return "DateTime"; }
        }
    }
}