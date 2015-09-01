namespace BTDB.EventStoreLayer
{
    interface ITypeSerializersLightMapping
    {
        InfoForType GetInfoFromObject(object obj, out TypeSerializers typeSerializers);
    }
}