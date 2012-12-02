namespace BTDB.EventStoreLayer
{
    public class EventStoreManager : IEventStoreManager
    {
        readonly TypeSerializers _typeSerializers = new TypeSerializers();

        public void SetNewTypeNameMapper(ITypeNameMapper mapper)
        {
            _typeSerializers.SetTypeNameMapper(mapper);
        }

        public void ForgotAllTypesAndSerializers()
        {
            _typeSerializers.ForgotAllTypesAndSerializers();
        }

        public IReadEventStore OpenReadOnlyStore(IEventFileStorage file)
        {
            return new ReadOnlyEventStore(file, _typeSerializers.CreateMapping());
        }

        public IWriteEventStore AppendToStore(IEventFileStorage file)
        {
            return new AppendingEventStore(file, _typeSerializers.CreateMapping());
        }
    }
}