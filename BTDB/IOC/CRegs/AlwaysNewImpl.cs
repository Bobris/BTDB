using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC.CRegs
{
    internal class AlwaysNewImpl : ICReg, ICRegILGen
    {
        readonly Type _implementationType;
        readonly ConstructorInfo _constructorInfo;

        internal AlwaysNewImpl(Type implementationType, ConstructorInfo constructorInfo)
        {
            _implementationType = implementationType;
            _constructorInfo = constructorInfo;
        }

        public string GenFuncName
        {
            get { return "AlwaysNew_" + _implementationType.ToSimpleName(); }
        }

        public void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            container.CallInjectingInitializations(_constructorInfo, il, context);
        }

        public bool CorruptingILStack
        {
            get { return false; }
        }

        public IILLocal GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            container.CallInjectedConstructor(_constructorInfo, il, context);
            return null;
        }
    }
}