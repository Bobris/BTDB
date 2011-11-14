using BTDB.FieldHandler;

namespace BTDB.Service
{
    public interface IService : IServiceClient, IServiceServer, IFieldHandlerFactoryProvider
    {
        IChannel Channel { get; }
    }
}
