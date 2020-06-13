using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.Collections;

namespace BTDB.IOC
{
    public class ContainerBuilder
    {
        StructList<IRegistration> _registrations;

        public IRegistration<IAsLiveScopeConstructorPropertiesTrait> RegisterType(Type type)
        {
            var registration = new SingleRegistration(type);
            _registrations.Add(registration);
            return registration;
        }

        public IRegistration<IAsTrait> RegisterInstance(object instance)
        {
            var instanceType = instance.GetType();
            var registration = new SingleInstanceRegistration(instance, instanceType);
            _registrations.Add(registration);
            return registration;
        }

        public IRegistration<IAsTrait> RegisterInstance<T>(object instance)
        {
            var instanceType = typeof(T);
            var registration = new SingleInstanceRegistration(instance, instanceType);
            _registrations.Add(registration);
            return registration;
        }

        public IRegistration<IAsLiveScopeTrait> RegisterFactory<T>(Func<IContainer, T> factory) where T : class
        {
            var registration = new SingleFactoryRegistration(factory, typeof(T));
            _registrations.Add(registration);
            return registration;
        }

        public IRegistration<IAsLiveScopeConstructorPropertiesScanTrait> RegisterAssemblyTypes(Assembly from)
        {
            return RegisterAssemblyTypes(new[] { from });
        }

        public IRegistration<IAsLiveScopeConstructorPropertiesScanTrait> RegisterAssemblyTypes(params Assembly[] froms)
        {
            var registration = new MultiRegistration(froms);
            _registrations.Add(registration);
            return registration;
        }

        public IContainer Build()
        {
            return new ContainerImpl(_registrations.AsReadOnlySpan());
        }

    }
}
