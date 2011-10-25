using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class UnsignedFieldHandler : SimpleFieldHandlerBase, IFieldHandler
    {
        public UnsignedFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadVUInt64()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipVUInt64()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteVUInt64(0)))
        {
        }

        public override string Name
        {
            get { return "Unsigned"; }
        }

        public new static bool IsCompatibleWith(Type type)
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