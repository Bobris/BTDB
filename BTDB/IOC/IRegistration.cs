using System;

namespace BTDB.IOC
{
    public interface IRegistration
    {
        IRegistration As(Type type);
        IRegistration SingleInstance();
    }
}