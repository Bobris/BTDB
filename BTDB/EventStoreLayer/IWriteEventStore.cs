using System.Collections.Generic;

namespace BTDB.EventStoreLayer;

public interface IWriteEventStore : IReadEventStore
{
    void Store(object? metadata, IReadOnlyList<object> events);
    void FinalizeStore();
    ulong KnownAppendablePosition();
    IEventFileStorage CurrentFileStorage { get; }
}
