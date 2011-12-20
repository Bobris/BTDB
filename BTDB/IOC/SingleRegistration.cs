using System;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    internal class SingleRegistration : SingleRegistrationBase, IContanerRegistration
    {
        public SingleRegistration(Type implementationType): base(implementationType)
        {
        }

        public void Register(ContanerRegistrationContext context)
        {
            FinalizeAsTypes();
            ICReg reg;
            switch (Lifetime)
            {
                case Lifetime.AlwaysNew:
                    reg = new AlwaysNewImpl(ImplementationType, ContainerImpl.FindBestConstructor(ImplementationType));
                    break;
                case Lifetime.Singleton:
                    reg = new SingletonImpl(ImplementationType, new AlwaysNewImpl(ImplementationType, ContainerImpl.FindBestConstructor(ImplementationType)), context.SingletonCount);
                    context.SingletonCount++;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            context.AddCReg(AsTypes, reg);
        }
    }
}