using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class StringTypeDescriptor : SimpleTypeDescriptor
    {
        public StringTypeDescriptor()
            : base(EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadString()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipString()),
                   EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteString("")))
        {
        }

        public override string Name
        {
            get { return "String"; }
        }
    }
}