using System;
using BTDB.Collections;

namespace BTDB.EventStoreLayer;

class InfoForType
{
    public int Id;
    public ITypeDescriptor Descriptor;
    public Layer1Loader? Loader;
    public readonly RefDictionary<EquatableType, Actions> Type2Actions = new RefDictionary<EquatableType, Actions>();

    public struct Actions
    {
        public Action<object, IDescriptorSerializerLiteContext>? NewTypeDiscoverer;
        public Layer1SimpleSaver? SimpleSaver;
        public Layer1ComplexSaver? ComplexSaver;
        public bool KnownNewTypeDiscoverer;
        public bool KnownSimpleSaver;
        public bool KnownComplexSaver;
    }
}
