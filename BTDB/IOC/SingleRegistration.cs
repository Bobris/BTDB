using System;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    internal class SingleRegistration : RegistrationBaseImpl<IAsTraitAndLiveScopeTrait>, IContanerRegistration
    {
        readonly Type _implementationType;
        readonly AsTraitImpl _asTrait;
        readonly LiveScopeTraitImpl _liveScopeTrait;

        public SingleRegistration(Type implementationType)
        {
            _liveScopeTrait = new LiveScopeTraitImpl();
            _asTrait = new AsTraitImpl();
            _implementationType = implementationType;
        }

        internal SingleRegistration(Type implementationType, AsTraitImpl asTrait, LiveScopeTraitImpl liveScopeTrait)
        {
            _implementationType = implementationType;
            _asTrait = asTrait;
            _liveScopeTrait = liveScopeTrait;
        }

        public void Register(ContanerRegistrationContext context)
        {
            ICReg reg;
            var bestConstructor = ContainerImpl.FindBestConstructor(_implementationType);
            if (bestConstructor == null) return;
            switch (_liveScopeTrait.Lifetime)
            {
                case Lifetime.AlwaysNew:
                    reg = new AlwaysNewImpl(_implementationType, bestConstructor);
                    break;
                case Lifetime.Singleton:
                    reg = new SingletonImpl(_implementationType, new AlwaysNewImpl(_implementationType, bestConstructor), context.SingletonCount);
                    context.SingletonCount++;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            context.AddCReg(_asTrait.GetAsTypesFor(_implementationType), _asTrait.PreserveExistingDefaults, reg);
        }

        public override object InternalTraits(Type trait)
        {
            if (trait == typeof(IAsTrait)) return _asTrait;
            if (trait == typeof(ILiveScopeTrait)) return _liveScopeTrait;
            throw new ArgumentOutOfRangeException();
        }
    }
}