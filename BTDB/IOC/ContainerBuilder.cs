using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    public class ContainerBuilder
    {
        readonly List<IRegistration> _registrations = new List<IRegistration>();

        public IRegistration Register(Type type)
        {
            var registration = new SingleRegistration(type);
            _registrations.Add(registration);
            return registration;
        }

        public IContainer Build()
        {
            return new ContainerImpl(_registrations);
        }
    }

    public interface IRegistration
    {
        IRegistration As(Type type);
        IRegistration SingleInstance();
    }
}
