using BTDB.FieldHandler;
using BTDB.IOC;

namespace BTDB.ODBLayer;

public interface IRelationInfoResolver
{
    IFieldHandlerFactory FieldHandlerFactory { get; }
    ITypeConvertorGenerator TypeConvertorGenerator { get; }
    IContainer? Container { get; }
    IFieldHandlerLogger? FieldHandlerLogger { get; }
    DBOptions ActualOptions { get; }
}
