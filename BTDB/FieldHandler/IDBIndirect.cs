using BTDB.ODBLayer;

namespace BTDB.FieldHandler;

public interface IDBIndirect : IIndirect
{
    public IObjectDBTransaction? Transaction { get; }
}
