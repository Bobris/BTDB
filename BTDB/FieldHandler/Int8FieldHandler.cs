using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class Int8FieldHandler : SimpleFieldHandlerBase, IFieldHandler
    {
        public Int8FieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadInt8()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipInt8()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteInt8(0)))
        {
        }

        public override string Name
        {
            get { return "Int8"; }
        }

        public static bool IsCompatibleWith(Type type)
        {
            return type == typeof(sbyte);
        }

        bool IFieldHandler.IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }
    }
}