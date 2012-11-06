using System.Threading.Tasks;

namespace BTDB.EventStoreLayer
{
    public interface IWriteEventStore : IReadEventStore
    {
        Task Store(object metadata, object[] events);
    }
}