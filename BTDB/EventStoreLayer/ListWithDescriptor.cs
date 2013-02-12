using System.Collections.Generic;

namespace BTDB.EventStoreLayer
{
    public class ListWithDescriptor<T> : List<T>, IKnowDescriptor
    {
        readonly ITypeDescriptor _descriptor;

        public ListWithDescriptor(int capacity, ITypeDescriptor descriptor)
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