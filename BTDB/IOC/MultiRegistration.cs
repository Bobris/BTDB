using System;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC
{
    internal class MultiRegistration : IMultiRegistration, IContanerRegistration
    {
        readonly Assembly[] _froms;

        public MultiRegistration(Assembly[] froms)
        {
            _froms = froms;
        }

        public IRegistration As(Type type)
        {
            throw new NotImplementedException();
        }

        public IRegistration SingleInstance()
        {
            throw new NotImplementedException();
        }

        public IRegistration AsSelf()
        {
            throw new NotImplementedException();
        }

        public IRegistration AsImplementedInterfaces()
        {
            throw new NotImplementedException();
        }

        public void Register(ContanerRegistrationContext context)
        {
            foreach (var assembly in _froms)
            {
                foreach (var type in assembly.GetTypes())
                {
                    if (!type.IsClass) continue;
                    if (type.IsAbstract) continue;
                    if (type.IsGenericTypeDefinition) continue;
                    if (type.IsDelegate()) continue;
                    ((IContanerRegistration)new SingleRegistration(type)).Register(context);
                }
            }
        }
    }
}