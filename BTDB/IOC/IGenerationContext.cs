using BTDB.IL;

namespace BTDB.IOC
{
    internal interface IGenerationContext
    {
        IILGen IL { get; }
        ContainerImpl Container { get; }
        T GetSpecific<T>() where T : class, new();
    }
}