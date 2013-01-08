using System;

namespace BTDB.EventStoreLayer
{
    internal interface ITypeSerializersLightMapping
    {
        void GetDescriptorAndTypeId(Type type, out TypeSerializers typeSerializers, out ITypeDescriptor descriptor, out int typeId);
    }
}