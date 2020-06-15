using System;
using System.Collections.Generic;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    delegate object LoaderObjectByDescriptor(ref SpanReader reader, ITypeBinaryDeserializerContext ctx, ITypeSerializersId2LoaderMapping mapping, ITypeDescriptor descriptor);
    delegate void SaverObjectSimple(ref SpanWriter writer, object @object);
    delegate void SaverObjectComplex(ref SpanWriter writer, ITypeBinarySerializerContext ctx, object @object);

    class InfoForType
    {
        public int Id;
        public ITypeDescriptor Descriptor;
        public LoaderObjectByDescriptor? Loader;
        public readonly RefDictionary<EquatableType, Actions> Type2Actions = new RefDictionary<EquatableType, Actions>();

        public struct Actions
        {
            public Action<object, IDescriptorSerializerLiteContext>? NewTypeDiscoverer;
            public SaverObjectSimple? SimpleSaver;
            public SaverObjectComplex? ComplexSaver;
            public bool KnownNewTypeDiscoverer;
            public bool KnownSimpleSaver;
            public bool KnownComplexSaver;
        }
    }
}
