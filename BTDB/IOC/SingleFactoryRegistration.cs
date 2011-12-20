using System;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    internal class SingleFactoryRegistration : SingleRegistrationBase, IContanerRegistration
    {
        readonly Func<IContainer, object> _factory;

        public SingleFactoryRegistration(Func<IContainer, object> factory, Type type):base(type)
        {
            _factory = factory;
        }

        public void Register(ContanerRegistrationContext context)
        {
            FinalizeAsTypes();
            ICRegILGen reg = new FactoryImpl(context.AddInstance(_factory), ImplementationType);
            if (Lifetime == Lifetime.Singleton)
            {
                reg = new SingletonImpl(ImplementationType, reg, context.SingletonCount);
                context.SingletonCount++;
            }
            context.AddCReg(AsTypes, (ICReg)reg);
        }
    }
}
