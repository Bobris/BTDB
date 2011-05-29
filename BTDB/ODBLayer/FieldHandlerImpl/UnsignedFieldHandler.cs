using System;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class UnsignedFieldHandler : SimpleFieldHandlerBase
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

        public override bool IsCompatibleWith(Type type)
        {
            if (type == typeof(byte)) return true;
            if (type == typeof(ushort)) return true;
            if (type == typeof(uint)) return true;
            if (type == typeof(ulong)) return true;
            return false;
        }
    }
}