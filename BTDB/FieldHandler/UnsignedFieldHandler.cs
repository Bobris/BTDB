using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class UnsignedFieldHandler : SimpleFieldHandlerBase, IFieldHandler
    {
        public UnsignedFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVUInt64()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVUInt64()),
            EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVUInt64(0)))
        {
        }

        public override string Name
        {
            get { return "Unsigned"; }
        }

        public static bool IsCompatibleWith(Type type)
        {
            if (type == typeof(byte)) return true;
            if (type == typeof(ushort)) return true;
            if (type == typeof(uint)) return true;
            if (type == typeof(ulong)) return true;
            return false;
        }

        bool IFieldHandler.IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }
    }
}