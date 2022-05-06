namespace BTDB.EventStoreLayer;

public interface ITypeSerializerMappingFactory
{
    ITypeSerializersMapping CreateMapping();
}
