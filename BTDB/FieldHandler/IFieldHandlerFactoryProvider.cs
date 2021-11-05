namespace BTDB.FieldHandler;

public interface IFieldHandlerFactoryProvider
{
    ITypeConvertorGenerator TypeConvertorGenerator { get; }
    IFieldHandlerFactory FieldHandlerFactory { get; }
}
