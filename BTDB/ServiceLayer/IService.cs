namespace BTDB.ServiceLayer
{
    public interface IService : IServiceClient, IServiceServer
    {
        IChannel Channel { get; }
    }
}
