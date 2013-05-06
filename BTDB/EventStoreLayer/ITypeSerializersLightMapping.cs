namespace BTDB.EventStoreLayer
{
    internal interface ITypeSerializersLightMapping
    {
        InfoForType GetInfoFromObject(object obj, out TypeSerializers typeSerializers);
    }
}