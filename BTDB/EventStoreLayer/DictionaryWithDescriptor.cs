using System.Collections.Generic;

namespace BTDB.EventStoreLayer
{
    public class DictionaryWithDescriptor<TK, TV> : Dictionary<TK, TV>, IKnowDescriptor
    {
        readonly ITypeDescriptor _descriptor;

        public DictionaryWithDescriptor(int capacity, ITypeDescriptor descriptor)
            : base(capacity)
        {
            _descriptor = descriptor;
        }

        public ITypeDescriptor GetDescriptor()
        {
            return _descriptor;
        }
    }
}