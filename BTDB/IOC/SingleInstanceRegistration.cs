using System;
using System.Collections.Generic;
using BTDB.IL;

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
        }
    }

    internal class InstanceImpl : ICReg, ICRegILGen
    {
        readonly object _instance;
        readonly int _instanceIndex;

        public InstanceImpl(object instance, int instanceIndex)
        {
            _instance = instance;
            _instanceIndex = instanceIndex;
        }

        public bool Single
        {
            get { return true; }
        }

        public string GenFuncName
        {
            get { return "Instance_" + _instance.GetType().ToSimpleName(); }
        }

        public void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
        }

        public IILLocal GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {

            il
                .Ldarg(0)
                .Ldfld(() => default(ContainerImpl).Instances)
                .LdelemRef()
                .Castclass(_instance.GetType());
            return null;
        }
    }
}