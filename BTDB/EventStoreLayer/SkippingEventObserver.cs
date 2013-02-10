namespace BTDB.EventStoreLayer
{
    public class SkippingEventObserver : IEventStoreObserver
    {
        public bool ObservedMetadata(object metadata, uint eventCount)
        {
            return false;
        }

        public void ObservedEvents(object[] events)
        {
        }
    }
}