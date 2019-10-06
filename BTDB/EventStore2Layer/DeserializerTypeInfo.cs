using System;
using BTDB.EventStoreLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer
{
    class DeserializerTypeInfo
    {
        /// <summary>
        /// Negative value means new descriptor. 0, 1 are invalid Ids. 2-49 are build-in types. 50+ are user types
        /// </summary>
        public int Id;
        public ITypeDescriptor? Descriptor;
        public Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeDescriptor, object>? Loader;
    }
}
