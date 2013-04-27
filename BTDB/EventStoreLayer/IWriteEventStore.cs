namespace BTDB.EventStoreLayer
{
    public interface IWriteEventStore : IReadEventStore
    {
        void Store(object metadata, object[] events);
        void FinalizeStore();
        ulong KnownAppendablePosition();
    }
}