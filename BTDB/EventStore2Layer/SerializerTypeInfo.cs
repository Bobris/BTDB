using System;
using BTDB.Collections;
using BTDB.EventStoreLayer;

namespace BTDB.EventStore2Layer;

class SerializerTypeInfo
{
    /// <summary>
    /// Negative value means new descriptor. 0, 1 are invalid Ids. 2-49 are build-in types. 50+ are user types
    /// </summary>
    public int Id;
    public ITypeDescriptor? Descriptor;
    public readonly RefDictionary<EquatableType, Action<object, IDescriptorSerializerLiteContext>> NestedObjGatherers = new RefDictionary<EquatableType, Action<object, IDescriptorSerializerLiteContext>>();
    public readonly RefDictionary<EquatableType, Layer1ComplexSaver?> ComplexSaver = new RefDictionary<EquatableType, Layer1ComplexSaver?>();
}
