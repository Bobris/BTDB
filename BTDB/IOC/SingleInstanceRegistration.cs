using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal class SingleInstanceRegistration : IRegistration, IContanerRegistration
    {
        readonly object _instance;
        readonly Type _type;
        readonly List<Type> _asTypes = new List<Type>();

        public SingleInstanceRegistration(object instance, Type type)
        {
            _instance = instance;
            _type = type;
        }

        public IRegistration As(Type type)
        {
            _asTypes.Add(type);
            return this;
        }

        public IRegistration SingleInstance()
        {
            return this;
        }

        public void Register(ContanerRegistrationContext context)
        {
            var reg = new InstanceImpl(_instance, context.AddInstance(_instance));
            context.AddCReg(_asTypes, reg);
        }
    }
}