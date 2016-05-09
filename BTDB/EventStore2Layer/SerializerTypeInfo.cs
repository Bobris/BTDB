using System;
using BTDB.EventStoreLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer
{
    class SerializerTypeInfo
    {
        /// <summary>
        /// Negative value means new descriptor. 0, 1 are invalid Ids. 2-49 are build-in types. 50+ are user types
        /// </summary>
        public int Id;
        public ITypeDescriptor Descriptor;
        public Action<object, IDescriptorSerializerLiteContext> NestedObjGatherer;
        public Action<AbstractBufferedWriter, object> SimpleSaver;
        public Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> ComplexSaver;
    }
}