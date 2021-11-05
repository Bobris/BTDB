namespace BTDB.EventStoreLayer;

public interface IReadEventStore
{
    void ReadFromStartToEnd(IEventStoreObserver observer);
    void ReadToEnd(IEventStoreObserver observer);
    bool IsKnownAsCorrupted();
    bool IsKnownAsFinished();
    bool IsKnownAsAppendable();
}
