namespace BTDB.EventStoreLayer
{
    public class EventStoreManager : IEventStoreManager
    {
        readonly TypeSerializers _typeSerializers = new TypeSerializers();

        public ICompressionStrategy CompressionStrategy { get; set; }

        public EventStoreManager()
        {
            CompressionStrategy = new SnappyCompressionStrategy();
        }

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
            return new ReadOnlyEventStore(file, _typeSerializers.CreateMapping(), CompressionStrategy);
        }

        public IWriteEventStore AppendToStore(IEventFileStorage file)
        {
            return new AppendingEventStore(file, _typeSerializers.CreateMapping(), CompressionStrategy);
        }

        public ITypeDescriptor DescriptorOf(object obj)
        {
            return _typeSerializers.DescriptorOf(obj);
        }
    }
}