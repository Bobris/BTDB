using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeSerializersMapping : IDescriptorSerializerContext
{
    void LoadTypeDescriptors(ref MemReader reader);
    object? LoadObject(ref MemReader reader);
    void Reset();
}
