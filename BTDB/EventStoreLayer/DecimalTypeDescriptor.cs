using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class DecimalTypeDescriptor : SimpleTypeDescriptor
    {
        public DecimalTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDecimal()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDecimal()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDecimal(0)))
        {
        }

        public override string Name
        {
            get { return "Decimal"; }
        }
    }
}