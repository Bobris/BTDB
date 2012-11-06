namespace BTDB.EventStoreLayer
{
    public interface IEventStoreObserver
    {
        // return true if you want to observe Events belonging to provided metadata
        bool ObservedMetadata(object metadata);
        void ObservedEvents(object[] events);
    }
}