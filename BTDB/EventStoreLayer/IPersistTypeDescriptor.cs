using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface IPersistTypeDescriptor
{
    void Persist(ref MemWriter writer, DescriptorWriter nestedDescriptorWriter);
}
