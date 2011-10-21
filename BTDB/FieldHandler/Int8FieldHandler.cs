using System;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class Int8FieldHandler : SimpleFieldOrderableHandlerBase, IFieldHandler
    {
        public Int8FieldHandler()
            : base(
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadInt8()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipInt8()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteInt8(0)),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadInt8Ordered()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipInt8Ordered()),
                EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteInt8Ordered(0)))
        {
        }

        public override string Name
        {
            get { return "Int8"; }
        }

        public new static bool IsCompatibleWith(Type type)
        {
            return type == typeof(sbyte);
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }
    }
}