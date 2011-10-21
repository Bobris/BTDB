using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class SignedFieldHandler : SimpleFieldOrderableHandlerBase, IFieldHandler
    {
        public SignedFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadVInt64()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipVInt64()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteVInt64(0)))
        {
        }

        public override string Name
        {
            get { return "Signed"; }
        }


        public new static bool IsCompatibleWith(Type type)
        {
            if (type == typeof(sbyte)) return true;
            if (type == typeof(short)) return true;
            if (type == typeof(int)) return true;
            if (type == typeof(long)) return true;
            return false;
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }
    }
}