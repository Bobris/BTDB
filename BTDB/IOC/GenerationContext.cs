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
        readonly ICRegILGen _registration;
        IBuildContext _buildContext;
        readonly Dictionary<Type, object> _specifics = new Dictionary<Type, object>();
        readonly ParameterInfo[] _parameterInfos;
        readonly List<Tuple<object, Type>> _constants = new List<Tuple<object, Type>>();
        readonly Stack<Tuple<ICReg, string>> _cycleDetectionStack = new Stack<Tuple<ICReg, string>>();

        public GenerationContext(ContainerImpl container, ICRegILGen registration, IBuildContext buildContext)
        {
            _container = container;
            _registration = registration;
            _buildContext = buildContext;
            _parameterInfos = null;
        }

        public GenerationContext(ContainerImpl container, ICRegILGen registration, IBuildContext buildContext, ParameterInfo[] parameterInfos)
        {
            _container = container;
            _registration = registration;
            _buildContext = buildContext;
            _parameterInfos = parameterInfos;
        }

        public IILGen IL { get; private set; }

        public ContainerImpl Container
        {
            get { return _container; }
        }

        public IBuildContext BuildContext { get { return _buildContext; } set { _buildContext = value; } }

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
            if (local != null)
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
                var local = parsLocals[i];
                if (local != null)
                {
                    IL.Ldloc(local);
                }
                else
                {
                    local = regs[i].GenMain(this);
                    if (local != null)
                    {
                        IL.Ldloc(local);
                    }
                }
                if (local == null) continue;
                if (local.LocalType != needs[i].ClrType && local.LocalType.IsClass)
                {
                    IL.UnboxAny(needs[i].ClrType);
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
            return _resolvers[new Tuple<IBuildContext, INeed>(_buildContext, need)];
        }

        public void PushToCycleDetector(ICReg reg, string name)
        {
            if (_cycleDetectionStack.Any(t => t.Item1 == reg))
            {
                throw new InvalidOperationException("Cycle detected in registrations: " + string.Join(", ", _cycleDetectionStack.Select(t => t.Item2)) + ". Consider using Lazy<> to break cycle.");
            }
            _cycleDetectionStack.Push(new Tuple<ICReg, string>(reg, name));
        }

        public void PopFromCycleDetector()
        {
            _cycleDetectionStack.Pop();
        }

        readonly Dictionary<Tuple<IBuildContext, INeed>, ICRegILGen> _resolvers = new Dictionary<Tuple<IBuildContext, INeed>, ICRegILGen>(Comparer.Instance);

        class Comparer : IEqualityComparer<Tuple<IBuildContext, INeed>>
        {
            internal static readonly Comparer Instance = new Comparer();

            public bool Equals(Tuple<IBuildContext, INeed> x, Tuple<IBuildContext, INeed> y)
            {
                if (x.Item1 != y.Item1) return false;
                var nx = x.Item2;
                var ny = y.Item2;
                if (nx.Kind != ny.Kind) return false;
                if (nx.ClrType != ny.ClrType) return false;
                if (nx.Key != ny.Key) return false;
                if (nx.ForcedKey != ny.ForcedKey) return false;
                if (nx.Optional != ny.Optional) return false;
                return true;
            }

            public int GetHashCode(Tuple<IBuildContext, INeed> obj)
            {
                return obj.Item1.GetHashCode() * 33 + obj.Item2.ClrType.GetHashCode();
            }
        }

        class ComparerConst : IEqualityComparer<Tuple<object, Type>>
        {
            internal static readonly ComparerConst Instance = new ComparerConst();

            public bool Equals(Tuple<object, Type> x, Tuple<object, Type> y)
            {
                return x.Item1 == y.Item1 && x.Item2 == y.Item2;
            }

            public int GetHashCode(Tuple<object, Type> obj)
            {
                return obj.Item1.GetHashCode() * 33 + obj.Item2.GetHashCode();
            }
        }

        internal void GatherNeeds(ICRegILGen regILGen, HashSet<ICRegILGen> processed)
        {
            if (processed.Contains(regILGen)) return;
            processed.Add(regILGen);
            foreach (var need in regILGen.GetNeeds(this))
            {
                if (need.Kind == NeedKind.CReg)
                {
                    GatherNeeds((ICRegILGen)need.Key, processed);
                    continue;
                }
                var k = new Tuple<IBuildContext, INeed>(_buildContext, need);
                if (_resolvers.ContainsKey(k))
                    continue;
                if (need == Need.ContainerNeed)
                {
                    _resolvers.Add(k, AddConstant(_container, need.ClrType));
                    continue;
                }
                if (need.Kind == NeedKind.Constant)
                {
                    _resolvers.Add(k, AddConstant(need.Key, need.ClrType));
                    continue;
                }
                if (need.Kind == NeedKind.ConstructorParameter)
                {
                    var reg = ResolveNeedBy(need.ClrType, need.Key);
                    if (reg == null && !need.ForcedKey)
                        reg = ResolveNeedBy(need.ClrType, null);
                    if (reg == null)
                    {
                        throw new ArgumentException(string.Format("Cannot resolve {0} with key {1}", need.ClrType.ToSimpleName(), need.Key));
                    }
                    _resolvers.Add(new Tuple<IBuildContext, INeed>(_buildContext, need), reg);
                    GatherNeeds(reg, processed);
                }
            }
        }

        ICRegILGen AddConstant(object obj, Type type)
        {
            var tuple = new Tuple<object, Type>(obj, type);
            var comp = ComparerConst.Instance;
            foreach (var constant in _constants)
            {
                if (comp.Equals(constant, tuple))
                {
                    tuple = constant;
                    goto found;
                }
            }
            _constants.Add(tuple);
        found:
            return new ConstantImpl(tuple);
        }

        class ConstantImpl : ICRegILGen
        {
            readonly Tuple<object, Type> _tuple;

            public ConstantImpl(Tuple<object, Type> tuple)
            {
                _tuple = tuple;
            }

            public string GenFuncName(IGenerationContext context)
            {
                throw new InvalidOperationException();
            }

            public void GenInitialization(IGenerationContext context)
            {
            }

            public bool IsCorruptingILStack(IGenerationContext context)
            {
                return false;
            }

            public IILLocal GenMain(IGenerationContext context)
            {
                var consts = ((GenerationContext)context)._constants;
                if (consts.Count == 1)
                {
                    context.IL.Ldarg(0);
                    return null;
                }
                var idx = consts.FindIndex(t => ReferenceEquals(t, _tuple));
                context.IL.Ldarg(0).LdcI4(idx).LdelemRef().Castclass(_tuple.Item2);
                return null;
            }

            public IEnumerable<INeed> GetNeeds(IGenerationContext context)
            {
                yield break;
            }

            public INeed PreResolveNeed(IGenerationContext context, INeed need)
            {
                return need;
            }
        }

        class SimpleParamImpl : ICRegILGen
        {
            readonly int _idx;

            public SimpleParamImpl(int idx)
            {
                _idx = idx;
            }

            public string GenFuncName(IGenerationContext context)
            {
                throw new InvalidOperationException();
            }

            public void GenInitialization(IGenerationContext context)
            {
            }

            public bool IsCorruptingILStack(IGenerationContext context)
            {
                return false;
            }

            public IILLocal GenMain(IGenerationContext context)
            {
                var consts = ((GenerationContext)context)._constants;
                context.IL.Ldarg((ushort)(_idx + (consts.Count > 0 ? 1 : 0)));
                return null;
            }

            public IEnumerable<INeed> GetNeeds(IGenerationContext context)
            {
                yield break;
            }

            public INeed PreResolveNeed(IGenerationContext context, INeed need)
            {
                return need;
            }
        }

        ICRegILGen ResolveNeedBy(Type clrType, object key)
        {
            if (_parameterInfos != null)
            {
                foreach (var parameterInfo in _parameterInfos)
                {
                    if (clrType == parameterInfo.ParameterType && (key as string == parameterInfo.Name || key == null))
                    {
                        return new SimpleParamImpl(parameterInfo.Position);
                    }
                }
            }
            return _buildContext.ResolveNeedBy(clrType, key);
        }

        public object GenerateFunc(Type funcType)
        {
            GatherNeeds(_registration, new HashSet<ICRegILGen>());
            if (_constants.Count == 0)
            {
                var method = ILBuilder.Instance.NewMethod(_registration.GenFuncName(this), funcType);
                IL = method.Generator;
                GenerateBody();
                return method.Create();
            }
            if (_constants.Count == 1)
            {
                var method = ILBuilder.Instance.NewMethod(_registration.GenFuncName(this), funcType, _constants[0].Item2);
                IL = method.Generator;
                GenerateBody();
                return method.Create(_constants[0].Item1);
            }
            else
            {
                var method = ILBuilder.Instance.NewMethod(_registration.GenFuncName(this), funcType, typeof(object[]));
                IL = method.Generator;
                GenerateBody();
                return method.Create(_constants.Select(t => t.Item1).ToArray());
            }
        }

        void GenerateBody()
        {
            _registration.GenInitialization(this);
            var local = _registration.GenMain(this);
            if (local != null)
            {
                IL.Ldloc(local);
            }
            IL.Ret();
        }
    }
}