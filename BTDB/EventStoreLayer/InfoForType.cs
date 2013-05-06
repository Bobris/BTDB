using System;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class InfoForType
    {
        public int Id;
        public ITypeDescriptor Descriptor;
        public Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object> Loader;
        public Action<object, IDescriptorSerializerLiteContext> NewTypeDiscoverer;
        public Action<AbstractBufferedWriter, object> SimpleSaver;
        public Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> ComplexSaver;
        public bool KnownNewTypeDiscoverer;
        public bool KnownSimpleSaver;
        public bool KnownComplexSaver;
    }
}