using BTDB.EventStoreLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer;

delegate object Layer2Loader(ref SpanReader reader, ITypeBinaryDeserializerContext ctx,
    ITypeDescriptor descriptor);
class DeserializerTypeInfo
{
    /// <summary>
    /// Negative value means new descriptor. 0, 1 are invalid Ids. 2-49 are build-in types. 50+ are user types
    /// </summary>
    public int Id;
    public ITypeDescriptor? Descriptor;
    public Layer2Loader? Loader;
}
