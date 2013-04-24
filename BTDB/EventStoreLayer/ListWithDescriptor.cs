using System.Collections.Generic;
using System.Text;

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

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("[ ");
            var first = true;
            foreach (var o in this)
            {
                if (first) first = false; else sb.Append(", ");
                sb.AppendJsonLike(o);
            }
            sb.Append(" ]");
            return sb.ToString();
        }
    }
}