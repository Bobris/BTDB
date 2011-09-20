using System;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class DoubleFieldHandler : SimpleFieldHandlerBase
    {
        public DoubleFieldHandler(): base(
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadDouble()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipDouble()),
            EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteDouble(0)))
        {
        }

        public override string Name
        {
            get { return "Double"; }
        }

        public override bool IsCompatibleWith(Type type)
        {
            return type == typeof(double) || type == typeof(float);
        }
    }
}