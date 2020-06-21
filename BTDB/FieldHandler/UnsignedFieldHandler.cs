using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class UnsignedFieldHandler : SimpleFieldHandlerBase
    {
        public UnsignedFieldHandler(): base("Unsigned",
            EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVUInt64()),
            EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVUInt64()),
            EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVUInt64(0)))
        {
        }

        public static bool IsCompatibleWith(Type type)
        {
            if (type == typeof(byte)) return true;
            if (type == typeof(ushort)) return true;
            if (type == typeof(uint)) return true;
            if (type == typeof(ulong)) return true;
            return false;
        }

        public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }
    }
}
