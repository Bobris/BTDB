using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC
{
    internal class GenerationContext : IGenerationContext
    {
        readonly ContainerImpl _container;
        readonly Dictionary<Type, object> _specifics = new Dictionary<Type, object>();

        public GenerationContext(ContainerImpl container)
        {
            _container = container;
        }

        public IILGen IL { get; internal set; }

        public ContainerImpl Container
        {
            get { return _container; }
        }

        public T GetSpecific<T>() where T : class, new()
        {
            object specific;
            if (!_specifics.TryGetValue(typeof(T), out specific))
            {
                specific = new T();
                var contextSetter = specific as IGenerationContextSetter;
                if (contextSetter != null)
                    contextSetter.Set(this);
                _specifics.Add(typeof(T), specific);
            }
            return (T)specific;
        }

        public IEnumerable<INeed> NeedsForConstructor(ConstructorInfo constructor)
        {
            foreach (var parameter in constructor.GetParameters())
            {
                yield return new Need
                    {
                        Kind = NeedKind.ConstructorParameter,
                        ParentType = constructor.ReflectedType,
                        ClrType = parameter.ParameterType,
                        Optional = false,
                        ForcedKey = false,
                        Key = parameter.Name
                    };
            }
        }

        public void PushToILStack(INeed need)
        {
            var regIL = ResolveNeed(need);
            var local = regIL.GenMain(this);
            if (local!=null)
            {
                IL.Ldloc(local);
            }
        }

        public void PushToILStack(IEnumerable<INeed> needsEnumerable)
        {
            var needs = needsEnumerable.ToArray();
            var regs = needs.Select(ResolveNeed).ToArray();
            var parsLocals = new List<IILLocal>(regs.Length);
            int index = 0;
            foreach (var reg in regs)
            {
                if (reg.IsCorruptingILStack(this))
                {
                    var local = reg.GenMain(this);
                    if (local == null)
                    {
                        local = IL.DeclareLocal(needs[index].ClrType);
                        IL.Stloc(local);
                    }
                    parsLocals.Add(local);
                }
                else
                {
                    parsLocals.Add(null);
                }
                index++;
            }
            for (int i = 0; i < regs.Length; i++)
            {
                if (parsLocals[i] != null)
                {
                    IL.Ldloc(parsLocals[i]);
                }
                else
                {
                    var local = regs[i].GenMain(this);
                    if (local != null)
                    {
                        IL.Ldloc(local);
                    }
                }
            }
        }

        public bool AnyCorruptingStack(IEnumerable<INeed> needs)
        {
            foreach (var regILGen in needs.Select(ResolveNeed))
            {
                if (regILGen.IsCorruptingILStack(this)) return true;
            }
            return false;
        }

        public ICRegILGen ResolveNeed(INeed need)
        {
            if (need == Need.ContainerNeed)
            {
                return ContainerInArg0Impl.Instance;
            }
            return _container.FindCRegILGen(null, need.ClrType);
        }
    }
}