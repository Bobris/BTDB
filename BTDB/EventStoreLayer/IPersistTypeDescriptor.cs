using System;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public interface IPersistTypeDescriptor
    {
        void Persist(AbstractBufferedWriter writer, Action<AbstractBufferedWriter, ITypeDescriptor> nestedDescriptorWriter);
    }
}