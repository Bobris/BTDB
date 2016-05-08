using System;
using BTDB.EventStoreLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer
{
    class SerializerTypeInfo
    {
        public uint Id;
        public ITypeDescriptor Descriptor;
        public Action<object, IDescriptorSerializerLiteContext> NewTypeDiscoverer;
        public Action<AbstractBufferedWriter, object> SimpleSaver;
        public Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> ComplexSaver;
    }
}