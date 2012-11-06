namespace BTDB.EventStoreLayer
{
    public interface IEventStoreManager
    {
        void SetNewTypeNameMapper(ITypeNameMapper mapper);
        void ForgotAllTypesAndSerializers();
        IReadEventStore OpenReadOnlyStore(IEventFileStorage file);
        IWriteEventStore AppendToStore(IEventFileStorage file);
    }
}
