using System;
using System.Collections.Generic;
using System.Reflection;

namespace BTDB.IOC
{
    public class ContainerBuilder
    {
        readonly List<IRegistration> _registrations = new List<IRegistration>();

        public IRegistration<IAsTraitAndLiveScopeTrait> RegisterType(Type type)
        {
            var registration = new SingleRegistration(type);
            _registrations.Add(registration);
            return registration;
        }

        public IRegistration<IAsTrait> RegisterInstance<T>(T instance) where T : class
        {
            var registration = new SingleInstanceRegistration(instance, typeof(T));
            _registrations.Add(registration);
            return registration;
        }

        public IRegistration<IAsTraitAndLiveScopeTrait> RegisterFactory<T>(Func<IContainer, T> factory) where T : class
        {
            var registration = new SingleFactoryRegistration(factory, typeof (T));
            _registrations.Add(registration);
            return registration;
        }

        public IRegistration<IAsTraitAndLiveScopeTraitAndScanTrait> RegisterAssemblyTypes(Assembly from)
        {
            return RegisterAssemblyTypes(new[] {from});
        }

        public IRegistration<IAsTraitAndLiveScopeTraitAndScanTrait> RegisterAssemblyTypes(params Assembly[] froms)
        {
            var registration = new MultiRegistration(froms);
            _registrations.Add(registration);
            return registration;
        }

        public IContainer Build()
        {
            return new ContainerImpl(_registrations);
        }

    }
}
