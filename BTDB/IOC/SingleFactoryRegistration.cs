using System;
using System.Collections.Generic;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    internal class SingleFactoryRegistration : IRegistration, IContanerRegistration
    {
        readonly Func<IContainer, object> _factory;
        readonly Type _type;
        readonly List<Type> _asTypes = new List<Type>();
        Lifetime _lifetime;

        public SingleFactoryRegistration(Func<IContainer, object> factory, Type type)
        {
            _factory = factory;
            _type = type;
            _lifetime = Lifetime.AlwaysNew;
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
            ICRegILGen reg = new FactoryImpl(context.AddInstance(_factory), _type);
            if (_lifetime==Lifetime.Singleton)
            {
                reg = new SingletonImpl(_type, reg, context.SingletonCount);
                context.SingletonCount++;
            }
            context.AddCReg(_asTypes, (ICReg)reg);
        }
    }
}