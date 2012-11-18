namespace BTDB.EventStoreLayer
{
    public class EventStoreManager : IEventStoreManager
    {
        ITypeNameMapper _mapper;
        readonly TypeSerializers _typeSerializers = new TypeSerializers();

        public void SetNewTypeNameMapper(ITypeNameMapper mapper)
        {
            _mapper = mapper;
            _typeSerializers.SetTypeNameMapper(mapper);
        }

        public void ForgotAllTypesAndSerializers()
        {
            _typeSerializers.ForgotAllTypesAndSerializers();
        }

        public IReadEventStore OpenReadOnlyStore(IEventFileStorage file)
        {
            return new ReadOnlyEventStore(this, file);
        }

        public IWriteEventStore AppendToStore(IEventFileStorage file)
        {
            throw new System.NotImplementedException();
        }
    }
}