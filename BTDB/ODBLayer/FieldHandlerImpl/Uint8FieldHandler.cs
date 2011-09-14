using System;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class Uint8FieldHandler : SimpleFieldHandlerBase, IFieldHandler
    {
        public Uint8FieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadUInt8()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipUInt8()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteUInt8(0)))
        {
        }

        public override string Name
        {
            get { return "UInt8"; }
        }

        public new static bool IsCompatibleWith(Type type)
        {
            return type == typeof(byte);
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }
    }
}