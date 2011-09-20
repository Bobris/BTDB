namespace BTDB.ODBLayer
{
    public interface IFieldHandlerFactoryProvider
    {
        ITypeConvertorGenerator TypeConvertorGenerator { get; }
        IFieldHandlerFactory FieldHandlerFactory { get; }
    }
}