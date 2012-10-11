using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.IL;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    public class ContainerImpl : IContainer
    {
        readonly ConcurrentDictionary<KeyAndType, Func<object>> _workers = new ConcurrentDictionary<KeyAndType, Func<object>>();
        readonly object _buildingLock = new object();
        internal readonly Dictionary<KeyAndType, ICReg> Registrations = new Dictionary<KeyAndType, ICReg>();
        // ReSharper disable MemberCanBePrivate.Global
        public readonly object[] SingletonLocks;
        public readonly object[] Singletons;
        public object[] Instances;
        // ReSharper restore MemberCanBePrivate.Global

        internal ContainerImpl(IEnumerable<IRegistration> registrations)
        {
            var context = new ContanerRegistrationContext(this, Registrations);
            foreach (var registration in registrations)
            {
                ((IContanerRegistration)registration).Register(context);
            }
            SingletonLocks = new object[context.SingletonCount];
            for (int i = 0; i < context.SingletonCount; i++)
            {
                SingletonLocks[i] = new object();
            }
            Singletons = new object[context.SingletonCount];
            Instances = context.Instances.ToArray();
            context.AddCReg(Enumerable.Repeat(new KeyAndType(null, typeof(IContainer)), 1), true, new ContainerInjectImpl());
        }

        internal static ConstructorInfo FindBestConstructor(Type type)
        {
            return type.GetConstructors().OrderByDescending(ci => ci.GetParameters().Length).FirstOrDefault();
        }

        public object Resolve(Type type)
        {
            return ResolveKeyed(null, type);
        }

        public object ResolveNamed(string name, Type type)
        {
            return ResolveKeyed(name, type);
        }

        public object ResolveKeyed(object key, Type type)
        {
            Func<object> worker;
            if (_workers.TryGetValue(new KeyAndType(key, type), out worker))
            {
                return worker();
            }
            lock (_buildingLock)
            {
                worker = TryBuild(key, type);
            }
            if (worker == null) throwNotResolvable(key, type);
            return worker();
        }

        void throwNotResolvable(object key, Type type)
        {
            if (key == null)
            {
                throw new ArgumentException(string.Format("Type {0} cannot be resolved", type.ToSimpleName()));
            }
            throw new ArgumentException(string.Format("Type {0} with key {1} cannot be resolved", type.ToSimpleName(), key));
        }

        Func<object> TryBuild(object key, Type type)
        {
            Func<object> worker;
            if (!_workers.TryGetValue(new KeyAndType(key, type), out worker))
            {
                worker = Build(key, type);
                if (worker == null) return null;
                _workers.TryAdd(new KeyAndType(key, type), worker);
            }
            return worker;
        }

        internal Func<object> BuildFromRegistration(ICRegILGen registration, IBuildContext buildContext)
        {
            var regOpt = registration as ICRegFuncOptimized;
            if (regOpt != null)
            {
                var result = (Func<object>)regOpt.BuildFuncOfT(this, typeof(Func<object>));
                if (result != null)
                {
                    return result;
                }
            }
            var context = new GenerationContext(this, registration, buildContext);
            return (Func<object>)context.GenerateFunc(typeof(Func<object>));
        }

        Func<object> Build(object key, Type type)
        {
            var buildContext = new BuildContext(this);
            var registration = buildContext.ResolveNeedBy(type, key);
            if (registration != null)
            {
                return BuildFromRegistration(registration, buildContext);
            }
            return null;
        }

        public int AddInstance(object instance)
        {
            var result = Instances.Length;
            Array.Resize(ref Instances, result + 1);
            Instances[result] = instance;
            return result;
        }
    }
}
