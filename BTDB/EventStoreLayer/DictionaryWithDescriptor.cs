using System.Collections.Generic;
using System.Text;

namespace BTDB.EventStoreLayer;

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

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("{ ");
        var first = true;
        foreach (var p in this)
        {
            if (first) first = false; else sb.Append(", ");
            sb.AppendJsonLike(p.Key).Append(": ").AppendJsonLike(p.Value);
        }
        sb.Append(" }");
        return sb.ToString();
    }
}
