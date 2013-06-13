using System;
using System.Linq;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    internal class SingleRegistration : RegistrationBaseImpl<IAsLiveScopeConstructorTrait>, IContanerRegistration
    {
        readonly Type _implementationType;
        readonly AsTraitImpl _asTrait;
        readonly LiveScopeTraitImpl _liveScopeTrait;
        readonly ConstructorTraitImpl _constructorTrait;

        public SingleRegistration(Type implementationType)
        {
            _liveScopeTrait = new LiveScopeTraitImpl();
            _asTrait = new AsTraitImpl();
            _constructorTrait = new ConstructorTraitImpl();
            _implementationType = implementationType;
        }

        internal SingleRegistration(Type implementationType, AsTraitImpl asTrait, LiveScopeTraitImpl liveScopeTrait, ConstructorTraitImpl constructorTrait)
        {
            _implementationType = implementationType;
            _asTrait = asTrait;
            _liveScopeTrait = liveScopeTrait;
            _constructorTrait = constructorTrait;
        }

        public void Register(ContanerRegistrationContext context)
        {
            ICReg reg;
            var possibleConstructors = _constructorTrait.ReturnPossibleConstructors(_implementationType).ToList();
            var bestConstructor = _constructorTrait.ChooseConstructor(_implementationType, possibleConstructors);
            if (bestConstructor == null)
            {
                throw new ArgumentException(string.Format("Cannot find public constructor for {0}", _implementationType.FullName));
            }
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
            if (trait == typeof(IConstructorTrait)) return _constructorTrait;
            throw new ArgumentOutOfRangeException();
        }
    }
}