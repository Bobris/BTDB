using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface IPersistTypeDescriptor
{
    void Persist(ref SpanWriter writer, DescriptorWriter nestedDescriptorWriter);
}
