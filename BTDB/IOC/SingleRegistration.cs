using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal class SingleRegistration : IRegistration, IContanerRegistration
    {
        readonly Type _implementationType;
        readonly List<Type> _asTypes = new List<Type>();
        Lifetime _lifetime;

        public SingleRegistration(Type implementationType)
        {
            _implementationType = implementationType;
        }

        public IRegistration As(Type type)
        {
            _asTypes.Add(type);
            return this;
        }

        public IRegistration SingleInstance()
        {
            _lifetime = Lifetime.Singleton;
            return this;
        }

        public void Register(ContanerRegistrationContext context)
        {
            ICReg reg;
            switch(_lifetime)
            {
                case Lifetime.AlwaysNew:
                    reg = new AlwaysNewImpl(_implementationType,ContainerImpl.FindBestConstructor(_implementationType));
                    break;
                case Lifetime.Singleton:
                    reg = new SingletonImpl(_implementationType, ContainerImpl.FindBestConstructor(_implementationType),context.SingletonCount);
                    context.SingletonCount++;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            foreach (var asType in _asTypes)
            {
                context.AddCReg(asType, reg);
            }
        }
    }
}