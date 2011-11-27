using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC
{
    internal class ContainerImpl : IContainer
    {
        readonly Func<Type, object> _mainFunc;

        public ContainerImpl(IEnumerable<IRegistration> registrations)
        {
            var mainMethod = ILBuilder.Instance.NewMethod<Func<Type, object>>("IOCMain");
            var il = mainMethod.Generator;
            foreach (var registration in registrations)
            {
                var singleReg = registration as SingleRegistration;
                foreach (var asType in singleReg.AsTypes)
                {
                    var label = il.DefineLabel();
                    il
                        .Ldarg(0)
                        .Ldtoken(asType)
                        .Call(() => Type.GetTypeFromHandle(default(RuntimeTypeHandle)))
                        .Call(()=>default(Type).Equals(default(Type)))
                        .BrfalseS(label)
                        .Newobj(singleReg.ImplementationType.GetConstructor(Type.EmptyTypes))
                        .Ret()
                        .Mark(label);
                }
            }
            il.Ldnull().Ret();
            _mainFunc = mainMethod.Create();
        }

        public object Resolve(Type type)
        {
            return _mainFunc(type);
        }
    }
}