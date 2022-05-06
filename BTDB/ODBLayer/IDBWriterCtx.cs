using BTDB.FieldHandler;

namespace BTDB.ODBLayer;

public interface IDBWriterCtx : IWriterCtx
{
    IInternalObjectDBTransaction GetTransaction();
}
