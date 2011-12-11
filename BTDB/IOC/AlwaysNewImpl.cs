using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC
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

        public bool Single
        {
            get { return true; }
        }

        public string GenFuncName
        {
            get { return "AlwaysNew_" + _implementationType.ToSimpleName(); }
        }

        public void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            var pars = _constructorInfo.GetParameters();
            foreach (var parameterInfo in pars)
            {
                var regILGen = container.FindCRegILGen(parameterInfo.ParameterType);
                regILGen.GenInitialization(container, il, context);
            }
        }

        public IILLocal GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            var pars = _constructorInfo.GetParameters();
            var parsLocals = new List<IILLocal>(pars.Length);
            foreach (var parameterInfo in pars)
            {
                var regILGen = container.FindCRegILGen(parameterInfo.ParameterType);
                parsLocals.Add(regILGen.GenMain(container, il, context));
            }
            foreach (var parLocal in parsLocals)
            {
                il.Ldloc(parLocal);
            }
            var localResult = il.DeclareLocal(_implementationType);
            il
                .Newobj(_constructorInfo)
                .Stloc(localResult);
            return localResult;
        }
    }
}