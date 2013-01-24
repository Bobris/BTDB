using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class DateTimeTypeDescriptor : SimpleTypeDescriptor
    {
        public DateTimeTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDateTime()),
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