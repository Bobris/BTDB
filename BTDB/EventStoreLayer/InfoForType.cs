using System;
using System.Collections.Generic;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class InfoForType
    {
        public int Id;
        public ITypeDescriptor Descriptor;
        public Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object>? Loader;
        public readonly RefDictionary<EquatableType, Actions> Type2Actions = new RefDictionary<EquatableType, Actions>();

        public struct Actions
        {
            public Action<object, IDescriptorSerializerLiteContext>? NewTypeDiscoverer;
            public Action<AbstractBufferedWriter, object>? SimpleSaver;
            public Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>? ComplexSaver;
            public bool KnownNewTypeDiscoverer;
            public bool KnownSimpleSaver;
            public bool KnownComplexSaver;
        }
    }
}
