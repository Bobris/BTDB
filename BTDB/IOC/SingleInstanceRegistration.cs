using System;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    class SingleInstanceRegistration : RegistrationBaseImpl<IAsTrait>, IContanerRegistration
    {
        readonly object _instance;
        readonly Type _implementationType;
        readonly AsTraitImpl _asTrait = new AsTraitImpl();

        public SingleInstanceRegistration(object instance, Type type)
        {
            _instance = instance;
            _implementationType = type;
        }

        public void Register(ContanerRegistrationContext context)
        {
            var reg = new InstanceImpl(_instance, context.AddInstance(_instance));
            context.AddCReg(_asTrait.GetAsTypesFor(_implementationType), _asTrait.PreserveExistingDefaults, reg);
        }

        public override object InternalTraits(Type trait)
        {
            if (trait == typeof(IAsTrait)) return _asTrait;
            throw new ArgumentOutOfRangeException();
        }
    }
}