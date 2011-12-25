using System;

namespace BTDB.IOC
{
    public interface IAsTrait
    {
        void As(Type type);
        void AsSelf();
        void AsImplementedInterfaces();
    }
}