using BTDB.FieldHandler;

namespace BTDB.Service
{
    public interface IService : IServiceClient, IServiceServer, IFieldHandlerFactoryProvider
    {
        IChannel Channel { get; }
        ITypeConvertorGenerator TypeConvertorGenerator { get; set; }
        IFieldHandlerFactory FieldHandlerFactory { get; set; }
    }
}
