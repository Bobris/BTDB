namespace BTDB.EventStoreLayer
{
    public interface IEventStoreObserver
    {
        // return true if you want to observe Events belonging to provided metadata
        bool ObservedMetadata(object metadata, uint eventCount);
        void ObservedEvents(object[] events);
    }
}