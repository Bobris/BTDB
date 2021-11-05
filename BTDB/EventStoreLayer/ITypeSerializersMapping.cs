using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeSerializersMapping : IDescriptorSerializerContext
{
    void LoadTypeDescriptors(ref SpanReader reader);
    object? LoadObject(ref SpanReader reader);
    void Reset();
}
