using BTDB.ODBLayer;

namespace BTDB.ServiceLayer
{
    public interface IService : IServiceClient, IServiceServer, IFieldHandlerFactoryProvider
    {
        IChannel Channel { get; }
        ITypeConvertorGenerator TypeConvertorGenerator { get; set; }
        IFieldHandlerFactory FieldHandlerFactory { get; set; }
    }
}
