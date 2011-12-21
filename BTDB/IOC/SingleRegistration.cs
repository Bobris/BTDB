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
            var bestConstructor = ContainerImpl.FindBestConstructor(ImplementationType);
            if (bestConstructor == null) return;
            switch (Lifetime)
            {
                case Lifetime.AlwaysNew:
                    reg = new AlwaysNewImpl(ImplementationType, bestConstructor);
                    break;
                case Lifetime.Singleton:
                    reg = new SingletonImpl(ImplementationType, new AlwaysNewImpl(ImplementationType, bestConstructor), context.SingletonCount);
                    context.SingletonCount++;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            context.AddCReg(AsTypes, reg);
        }
    }
}