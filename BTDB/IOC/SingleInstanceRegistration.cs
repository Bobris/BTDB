using System;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    internal class SingleInstanceRegistration : SingleRegistrationBase, IContanerRegistration
    {
        readonly object _instance;

        public SingleInstanceRegistration(object instance, Type type): base(type)
        {
            _instance = instance;
        }

        public void Register(ContanerRegistrationContext context)
        {
            FinalizeAsTypes();
            var reg = new InstanceImpl(_instance, context.AddInstance(_instance));
            context.AddCReg(AsTypes, reg);
        }
    }
}