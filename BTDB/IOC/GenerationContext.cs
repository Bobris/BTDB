using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.IL;
using BTDB.IOC.CRegs;
using BTDB.ODBLayer;

namespace BTDB.IOC
{
    internal class GenerationContext : IGenerationContext
    {
        readonly ContainerImpl _container;
        readonly Dictionary<Type, object> _specifics = new Dictionary<Type, object>();
        readonly ParameterInfo[] _parameterInfos;

        public GenerationContext(ContainerImpl container)
        {
            _container = container;
            _parameterInfos = null;
        }

        public GenerationContext(ContainerImpl container, ParameterInfo[] parameterInfos)
        {
            _container = container;
            _parameterInfos = parameterInfos;
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

        public void PushToILStack(ICRegILGen inCReg, INeed need)
        {
            var regIL = ResolveNeed(inCReg, need);
            var local = regIL.GenMain(this);
            if (local != null)
            {
                IL.Ldloc(local);
            }
        }

        public void PushToILStack(ICRegILGen inCReg, IEnumerable<INeed> needsEnumerable)
        {
            var needs = needsEnumerable.ToArray();
            var regs = needs.Select(n => ResolveNeed(inCReg, n)).ToArray();
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

        public bool AnyCorruptingStack(ICRegILGen inCReg, IEnumerable<INeed> needs)
        {
            foreach (var regILGen in needs.Select(n => ResolveNeed(inCReg, n)))
            {
                if (regILGen.IsCorruptingILStack(this)) return true;
            }
            return false;
        }

        public ICRegILGen ResolveNeed(ICRegILGen inCReg, INeed need)
        {
            return _resolvers[new Tuple<ICRegILGen, INeed>(inCReg, need)];
        }

        public void GenerateBody(IILGen il, ICRegILGen regILGen)
        {
            IL = il;
            GatherNeeds(regILGen, new HashSet<ICRegILGen>());
            regILGen.GenInitialization(this);
            var local = regILGen.GenMain(this);
            if (local != null)
            {
                il.Ldloc(local);
            }
        }

        readonly Dictionary<Tuple<ICRegILGen, INeed>, ICRegILGen> _resolvers = new Dictionary<Tuple<ICRegILGen, INeed>, ICRegILGen>(new Comparer());

        class Comparer : IEqualityComparer<Tuple<ICRegILGen, INeed>>
        {
            public bool Equals(Tuple<ICRegILGen, INeed> x, Tuple<ICRegILGen, INeed> y)
            {
                if (x.Item1 != y.Item1) return false;
                var nx = x.Item2;
                var ny = y.Item2;
                if (nx.Kind != ny.Kind) return false;
                if (nx.ClrType != ny.ClrType) return false;
                if (nx.Key != ny.Key) return false;
                if (nx.ForcedKey != ny.ForcedKey) return false;
                if (nx.Optional != ny.Optional) return false;
                if (nx.ParentType != ny.ParentType) return false;
                return true;
            }

            public int GetHashCode(Tuple<ICRegILGen, INeed> obj)
            {
                return obj.Item1.GetHashCode()*33 + obj.Item2.ClrType.GetHashCode();
            }
        }

        internal void GatherNeeds(ICRegILGen regILGen, HashSet<ICRegILGen> processed)
        {
            if (processed.Contains(regILGen)) return;
            processed.Add(regILGen);
            foreach (var need in regILGen.GetNeeds(this))
            {
                if (need == Need.ContainerNeed)
                {
                    _resolvers.Add(new Tuple<ICRegILGen, INeed>(regILGen, need), ArgXImpl.GetInstance(0));
                    continue;
                }
                if (need.Kind == NeedKind.CReg)
                {
                    GatherNeeds((ICRegILGen) need.Key, processed);
                    continue;
                }
                if (need.Kind == NeedKind.ConstructorParameter)
                {
                    var reg = ResolveNeedBy(need.ClrType, need.Key, need.ParentType);
                    if (reg == null && !need.ForcedKey)
                        reg = ResolveNeedBy(need.ClrType, null, need.ParentType);
                    if (reg == null)
                    {
                        throw new ArgumentException(string.Format("Cannot resolve {0} in {1} with key {2}", need.ClrType.ToSimpleName(), need.ParentType.ToSimpleName(), need.Key));
                    }
                    _resolvers.Add(new Tuple<ICRegILGen, INeed>(regILGen, need), reg);
                    GatherNeeds(reg, processed);
                }
            }
        }

        ICRegILGen ResolveNeedBy(Type clrType, object key, Type parentType)
        {
            if (_parameterInfos != null)
            {
                foreach (var parameterInfo in _parameterInfos)
                {
                    if (clrType == parameterInfo.ParameterType && (key as string == parameterInfo.Name || key == null))
                    {
                        return ArgXImpl.GetInstance((ushort)(parameterInfo.Position + 1));
                    }
                }
            }
            return _container.FindCRegILGen(key, clrType);
        }
    }
}