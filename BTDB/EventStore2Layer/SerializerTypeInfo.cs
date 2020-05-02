using System;
using System.Collections.Generic;
using BTDB.Collections;
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
        public ITypeDescriptor? Descriptor;
        public readonly RefDictionary<EquatableType, Action<object, IDescriptorSerializerLiteContext>> NestedObjGatherers = new RefDictionary<EquatableType, Action<object, IDescriptorSerializerLiteContext>>();
        public readonly RefDictionary<EquatableType, Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>?> ComplexSaver = new RefDictionary<EquatableType, Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>?>();
    }
}
