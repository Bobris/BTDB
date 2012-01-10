using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs
{
    internal class FactoryImpl : ICReg, ICRegILGen
    {
        readonly Type _type;
        readonly int _instanceIndex;

        public FactoryImpl(ContainerImpl container, Func<ContainerImpl, object> buildFunc, Type type)
        {
            _type = type;
            _instanceIndex = container.AddInstance(buildFunc);
        }

        public FactoryImpl(int instanceIndex, Type type)
        {
            _instanceIndex = instanceIndex;
            _type = type;
        }

        string ICRegILGen.GenFuncName(IGenerationContext context)
        {
            return "Factory_" + _type.ToSimpleName();
        }

        public void GenInitialization(IGenerationContext context)
        {
            context.GetSpecific<InstancesLocal>().Prepare();
        }

        internal class InstancesLocal : IGenerationContextSetter
        {
            IGenerationContext _context;

            public void Set(IGenerationContext context)
            {
                _context = context;
            }

            internal void Prepare()
            {
                if (MainLocal != null) return;
                MainLocal = _context.IL.DeclareLocal(typeof(object[]), "instances");
                _context.IL
                    .Ldarg(0)
                    .Ldfld(() => default(ContainerImpl).Instances)
                    .Stloc(MainLocal);
            }

            internal IILLocal MainLocal { get; private set; }
        }


        public bool IsCorruptingILStack(IGenerationContext context)
        {
            return false;
        }

        public IILLocal GenMain(IGenerationContext context)
        {
            var localInstances = context.GetSpecific<InstancesLocal>().MainLocal;
            var localInstance = context.IL.DeclareLocal(_type, "instance");
            context.IL
                .Ldloc(localInstances)
                .LdcI4(_instanceIndex)
                .LdelemRef()
                .Castclass(typeof(Func<ContainerImpl, object>));
            context.PushToILStack(this, Need.ContainerNeed);
            context.IL
                .Call(() => default(Func<ContainerImpl, object>).Invoke(null))
                .Castclass(_type)
                .Stloc(localInstance);
            return localInstance;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            yield return Need.ContainerNeed;
        }
    }
}