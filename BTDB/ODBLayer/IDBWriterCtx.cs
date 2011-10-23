using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    public interface IDBWriterCtx : IWriterCtx, IInstanceRegistry
    {
        IInternalObjectDBTransaction GetTransaction();
    }
}