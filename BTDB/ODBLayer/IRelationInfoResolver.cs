using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.Serialization;

namespace BTDB.ODBLayer;

public interface IRelationInfoResolver
{
    IFieldHandlerFactory FieldHandlerFactory { get; }
    ITypeConvertorGenerator TypeConvertorGenerator { get; }
    ITypeConverterFactory TypeConverterFactory { get; }
    IContainer? Container { get; }
    IFieldHandlerLogger? FieldHandlerLogger { get; }
    DBOptions ActualOptions { get; }
}
