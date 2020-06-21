using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class SignedFieldHandler : SimpleFieldHandlerBase
    {
        public SignedFieldHandler(): base("Signed",
            EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVInt64()),
            EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVInt64()),
            EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVInt64(0)))
        {
        }

        public static bool IsCompatibleWith(Type type)
        {
            if (type == typeof(sbyte)) return true;
            if (type == typeof(short)) return true;
            if (type == typeof(int)) return true;
            if (type == typeof(long)) return true;
            return false;
        }

        public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }
    }
}
