namespace BTDB.ODBLayer.FieldHandlerIface
{
    public interface IFieldHandlerFactoryProvider
    {
        ITypeConvertorGenerator TypeConvertorGenerator { get; }
        IFieldHandlerFactory FieldHandlerFactory { get; }
    }
}