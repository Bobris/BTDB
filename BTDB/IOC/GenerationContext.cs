using BTDB.IL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.Collections;
using BTDB.KVDBLayer;

namespace BTDB.IOC;

class GenerationContext : IGenerationContext
{
    readonly ContainerImpl _container;
    readonly ICRegILGen _registration;
    IBuildContext? _buildContext;
    readonly Dictionary<Type, object> _specifics = new Dictionary<Type, object>();
    readonly ParameterInfo[]? _parameterInfos;
    readonly List<Tuple<object, Type>> _constants = new List<Tuple<object, Type>>();
    readonly Stack<Tuple<ICReg, string>> _cycleDetectionStack = new Stack<Tuple<ICReg, string>>();

    public GenerationContext(ContainerImpl container, ICRegILGen registration, IBuildContext buildContext)
    {
        _container = container;
        _registration = registration;
        _buildContext = buildContext;
        _parameterInfos = null;
    }

    public GenerationContext(ContainerImpl container, ICRegILGen registration, IBuildContext buildContext,
        ParameterInfo[] parameterInfos)
    {
        _container = container;
        _registration = registration;
        _buildContext = buildContext;
        _parameterInfos = parameterInfos;
    }

    public IILGen? IL { get; private set; }

    public ContainerImpl Container => _container;

    public IBuildContext? BuildContext
    {
        get => _buildContext;
        set => _buildContext = value;
    }

    public T GetSpecific<T>() where T : class, new()
    {
        if (!_specifics.TryGetValue(typeof(T), out var specific))
        {
            specific = new T();
            if (specific is IGenerationContextSetter contextSetter)
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
                Optional = parameter.IsOptional,
                OptionalValue = parameter.RawDefaultValue,
                ForcedKey = false,
                Key = string.Intern(parameter.Name ?? "")
            };
        }
    }

    public IEnumerable<INeed> NeedsForProperties(Type type, bool autowired)
    {
        foreach (var propertyInfo in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (propertyInfo.GetAnySetMethod() == null) continue;
            var dependencyAttribute = propertyInfo.GetCustomAttribute<DependencyAttribute>();
            if (dependencyAttribute == null && !autowired) continue;
            yield return new Need
            {
                Kind = NeedKind.Property,
                ParentType = type,
                ClrType = propertyInfo.PropertyType,
                Optional = EmitHelpers.IsNullable(type, propertyInfo),
                ForcedKey = false,
                Key = string.Intern(dependencyAttribute?.Name ?? propertyInfo.Name),
                PropertyInfo = propertyInfo
            };
        }
    }

    public void PushToILStack(INeed need)
    {
        var regIL = ResolveNeed(need);
        var local = regIL.GenMain(this);
        if (local != null)
        {
            IL!.Ldloc(local);
        }
    }

    public void PushToILStack(IEnumerable<INeed> needsEnumerable)
    {
        var needs = needsEnumerable.ToArray();
        var regs = needs.Select(ResolveNeed).ToArray();
        var parsLocals = new StructList<IILLocal?>();
        parsLocals.Reserve((uint)regs.Length);
        var index = 0;
        foreach (var reg in regs)
        {
            if (reg.IsCorruptingILStack(this))
            {
                var local = reg.GenMain(this);
                if (local == null)
                {
                    local = IL!.DeclareLocal(needs[index].ClrType);
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

        for (var i = 0; i < regs.Length; i++)
        {
            var local = parsLocals[i];
            if (local != null)
            {
                IL!.Ldloc(local);
            }
            else
            {
                local = regs[i].GenMain(this);
                if (local != null)
                {
                    IL!.Ldloc(local);
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

    public bool IsResolvableNeed(INeed need)
    {
        return _resolvers.ContainsKey(new Tuple<IBuildContext, INeed>(_buildContext, need));
    }

    public ICRegILGen ResolveNeed(INeed need)
    {
        return _resolvers[new Tuple<IBuildContext, INeed>(_buildContext, need)];
    }

    public void PushToCycleDetector(ICReg reg, string name)
    {
        if (_cycleDetectionStack.Any(t => t.Item1 == reg))
        {
            throw new InvalidOperationException("Cycle detected in registrations: " +
                                                string.Join(", ", _cycleDetectionStack.Select(t => t.Item2)) +
                                                ". Consider using Lazy<> to break cycle.");
        }

        _cycleDetectionStack.Push(new Tuple<ICReg, string>(reg, name));
    }

    public void PopFromCycleDetector()
    {
        _cycleDetectionStack.Pop();
    }

    readonly Dictionary<Tuple<IBuildContext, INeed>, ICRegILGen> _resolvers =
        new Dictionary<Tuple<IBuildContext, INeed>, ICRegILGen>(Comparer.Instance);

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

    class ComparerProcessingContext : IEqualityComparer<Tuple<IBuildContext, ICRegILGen>>
    {
        internal static readonly ComparerProcessingContext Instance = new ComparerProcessingContext();

        public bool Equals(Tuple<IBuildContext, ICRegILGen> x, Tuple<IBuildContext, ICRegILGen> y)
        {
            return x.Item1 == y.Item1 && x.Item2 == y.Item2;
        }

        public int GetHashCode(Tuple<IBuildContext, ICRegILGen> obj)
        {
            return obj.Item1.GetHashCode() * 33 + obj.Item2.GetHashCode();
        }
    }

    void GatherNeeds(ICRegILGen regILGen, HashSet<Tuple<IBuildContext, ICRegILGen>> processed)
    {
        var processingContext = new Tuple<IBuildContext, ICRegILGen>(_buildContext, regILGen);
        if (processed.Contains(processingContext)) return;
        processed.Add(processingContext);
        foreach (var need in regILGen.GetNeeds(this))
        {
            if (need.Kind == NeedKind.CReg)
            {
                GatherNeeds(((ICRegILGen)need.Key)!, processed);
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
                if (reg == null && need.Optional)
                    reg = new OptionalImpl(need.OptionalValue!, need.ClrType);
                if (reg == null)
                {
                    throw new ArgumentException(
                        $"Cannot resolve {need.ClrType.ToSimpleName()} with key {need.Key}");
                }

                _resolvers.Add(new Tuple<IBuildContext, INeed>(_buildContext, need), reg);
                GatherNeeds(reg, processed);
            }

            if (need.Kind == NeedKind.Property)
            {
                var reg = ResolveNeedBy(need.ClrType, need.Key);
                if (reg == null && !need.ForcedKey)
                    reg = ResolveNeedBy(need.ClrType, null);
                if (reg == null)
                {
                    if (!need.Optional)
                        throw new ArgumentException(
                            $"Cannot resolve {need.ClrType.ToSimpleName()} with key {need.Key}");
                    return;
                }

                _resolvers.Add(new Tuple<IBuildContext, INeed>(_buildContext, need), reg);
                GatherNeeds(reg, processed);
            }
        }
    }

    class OptionalImpl : ICRegILGen
    {
        readonly object? _value;
        readonly Type _type;

        public OptionalImpl(object value, Type type)
        {
            _type = type;
            _value = value;
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

        public IILLocal? GenMain(IGenerationContext context)
        {
            // For some reason struct's RawDefaultValue is null
            // Partial explanation is that structs are not really compile time constants
            // so they are nullable during compilation and then assigned at runtime
            if (_type.IsValueType && _value == null)
            {
                var local = context.IL.DeclareLocal(_type);
                context.IL
                    .Ldloca(local)
                    .InitObj(_type)
                    .Ldloc(local);
            }
            else if (_type.IsValueType && _value != null && !_type.IsPrimitive && !_type.IsEnum)
            {
                var ctor = _type.GetConstructors()[0];
                context.IL
                    .Ld(_value)
                    .Newobj(ctor);
            }
            else
                context.IL.Ld(_value);

            return null;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            yield break;
        }

        public bool IsSingletonSafe()
        {
            return true;
        }
    }

    ICRegILGen AddConstant(object? obj, Type type)
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

        public IILLocal? GenMain(IGenerationContext context)
        {
            var constants = ((GenerationContext)context)._constants;
            if (constants.Count == 1)
            {
                context.IL.Ldarg(0);
                return null;
            }

            var idx = constants.FindIndex(t => ReferenceEquals(t, _tuple));
            context.IL.Ldarg(0).LdcI4(idx).LdelemRef().Castclass(_tuple.Item2);
            return null;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            yield break;
        }

        public bool IsSingletonSafe()
        {
            return true;
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

        public IILLocal? GenMain(IGenerationContext context)
        {
            var constants = ((GenerationContext)context)._constants;
            context.IL.Ldarg((ushort)(_idx + (constants.Count > 0 ? 1 : 0)));
            return null;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            yield break;
        }

        public bool IsSingletonSafe()
        {
            return true;
        }
    }

    ICRegILGen? ResolveNeedBy(Type clrType, object? key)
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

        return _buildContext!.ResolveNeedBy(clrType, key);
    }

    public object GenerateFunc(Type funcType)
    {
        GatherNeeds(_registration,
            new HashSet<Tuple<IBuildContext, ICRegILGen>>(ComparerProcessingContext.Instance));
        if (_constants.Count == 0)
        {
            var method = ILBuilder.Instance.NewMethod(_registration.GenFuncName(this), funcType);
            IL = method.Generator;
            GenerateBody();
            return method.Create();
        }

        if (_constants.Count == 1)
        {
            var method =
                ILBuilder.Instance.NewMethod(_registration.GenFuncName(this), funcType, _constants[0].Item2);
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
            IL!.Ldloc(local);
        }

        IL!.Ret();
    }

    public void VerifySingletonUsingOnlySingletons(Type singletonType)
    {
        GatherNeeds(_registration,
            new HashSet<Tuple<IBuildContext, ICRegILGen>>(ComparerProcessingContext.Instance));
        foreach (var need in _registration.GetNeeds(this))
        {
            if (need.Kind == NeedKind.CReg)
            {
                continue;
            }

            var k = new Tuple<IBuildContext, INeed>(_buildContext, need);
            if (!_resolvers[k].IsSingletonSafe())
            {
                throw new BTDBException("Singleton " + singletonType.ToSimpleName() + " dependency " +
                                        need.ClrType.ToSimpleName() + " is not singleton");
            }
        }
    }
}
