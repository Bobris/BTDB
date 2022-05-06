namespace BTDB.EventStoreLayer;

public interface IEventStoreObserver
{
    // return true if you want to observe Events belonging to provided metadata
    bool ObservedMetadata(object? metadata, uint eventCount);
    bool ShouldStopReadingNextEvents();
    void ObservedEvents(object[] events);
}
