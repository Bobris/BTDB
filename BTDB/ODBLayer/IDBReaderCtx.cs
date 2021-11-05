using BTDB.FieldHandler;

namespace BTDB.ODBLayer;

public interface IDBReaderCtx : IReaderCtx
{
    IInternalObjectDBTransaction GetTransaction();
}
