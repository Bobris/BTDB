using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class SignedFieldHandler : SimpleFieldHandlerBase, IFieldHandler
    {
        public SignedFieldHandler(): base("Signed",
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVInt64()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVInt64()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVInt64(0)))
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

        bool IFieldHandler.IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }
    }
}