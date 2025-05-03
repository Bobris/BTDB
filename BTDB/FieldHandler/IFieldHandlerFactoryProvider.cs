using BTDB.Serialization;

namespace BTDB.FieldHandler;

public interface IFieldHandlerFactoryProvider
{
    ITypeConvertorGenerator TypeConvertorGenerator { get; }
    ITypeConverterFactory TypeConverterFactory { get; }
    IFieldHandlerFactory FieldHandlerFactory { get; }
}
