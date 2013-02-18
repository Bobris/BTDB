using System.Threading.Tasks;

namespace BTDB.EventStoreLayer
{
    public interface IReadEventStore
    {
        Task ReadFromStartToEnd(IEventStoreObserver observer);
        Task ReadToEnd(IEventStoreObserver observer);
        bool IsKnownAsCorrupted();
        bool IsKnownAsFinished();
        bool IsKnownAsAppendable();
    }
}