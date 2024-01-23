using BTDB.IOC;
using BTDB.KVDBLayer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using BTDB;
using Xunit;

namespace BTDBTest;

using IOCDomain;

public partial class IocTests
{
    [Generate]
    public interface IAmLazy
    {
    }

    public class SuperLazy : IAmLazy
    {
    }

    [Generate]
    public class WantLazy1
    {
        readonly Lazy<IAmLazy> _v;

        public WantLazy1(Lazy<IAmLazy> v)
        {
            _v = v;
        }

        public IAmLazy Materialize => _v.Value;
    }

    [Generate]
    public class WantLazy2
    {
        readonly Lazy<IAmLazy> _v;

        public WantLazy2(Lazy<IAmLazy> v)
        {
            _v = v;
        }

        public IAmLazy Materialize => _v.Value;
    }

    [Fact]
    public void LazySingletonsAreThreadSafe()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<SuperLazy>().As<IAmLazy>().SingleInstance();
        builder.RegisterType<WantLazy1>().AsSelf();
        builder.RegisterType<WantLazy2>().AsSelf();

        var container = builder.Build();
        var a = container.Resolve<WantLazy1>();
        var b = container.Resolve<WantLazy2>();

        IAmLazy? l1 = null;
        IAmLazy? l2 = null;
        var task1 = Task.Run(() => { l1 = a.Materialize; });
        var task2 = Task.Run(() => { l2 = b.Materialize; });

        Task.WaitAll(task1, task2);
        Assert.NotNull(l1);
        Assert.Same(l1, l2);
    }

    [Fact]
    public void AlwaysNew()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>();
        var container = builder.BuildAndVerify();
        var log1 = container.Resolve<ILogger>();
        Assert.NotNull(log1);
        var log2 = container.Resolve<ILogger>();
        Assert.NotNull(log2);
        Assert.NotSame(log1, log2);
    }

    [Fact]
    public void Singleton()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        var container = builder.BuildAndVerify();
        var log1 = container.Resolve<ILogger>();
        Assert.NotNull(log1);
        var log2 = container.Resolve<ILogger>();
        Assert.NotNull(log2);
        Assert.Same(log1, log2);
    }

    [Fact]
    public void CreatesFuncFactory()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>();
        var container = builder.Build();
        var logFactory = container.Resolve<Func<ILogger>>();
        var log1 = logFactory();
        Assert.NotNull(log1);
        var log2 = logFactory();
        Assert.NotNull(log2);
        Assert.NotSame(log1, log2);
    }

    [Fact]
    public void CreatesLazyFactory()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>();
        var container = builder.Build();
        var lazyLog = container.Resolve<Lazy<ILogger>>();
        var log = lazyLog.Value;
        Assert.NotNull(log);
    }

    [Fact]
    public void InjectionToConstructorWithOneParameterAlwaysNew()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>();
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>();
        var container = builder.Build();
        var obj = container.Resolve<IErrorHandler>();
        Assert.NotNull(obj);
        Assert.NotNull(obj.Logger);
        var obj2 = container.Resolve<IErrorHandler>();
        Assert.NotNull(obj2);
        Assert.NotNull(obj2.Logger);
        Assert.NotSame(obj, obj2);
        Assert.NotSame(obj.Logger, obj2.Logger);
    }

    [Fact]
    public void InjectionToConstructorWithOneParameterSingleton()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>();
        var container = builder.Build();
        var obj = container.Resolve<IErrorHandler>();
        Assert.NotNull(obj);
        Assert.NotNull(obj.Logger);
        var obj2 = container.Resolve<IErrorHandler>();
        Assert.NotNull(obj2);
        Assert.NotNull(obj2.Logger);
        Assert.NotSame(obj, obj2);
        Assert.Same(obj.Logger, obj2.Logger);
    }

    [Fact]
    public void ReusingSingletonMultipleTimesInOneResolve()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>();
        builder.RegisterType<Database>().As<IDatabase>();
        var container = builder.Build();
        var obj = container.Resolve<IDatabase>();
        Assert.NotNull(obj);
        Assert.NotNull(obj.ErrorHandler);
        Assert.NotNull(obj.Logger);
        Assert.Same(obj.Logger, obj.ErrorHandler.Logger);
    }

    [Generate]
    public class SpecialCase
    {
        public SpecialCase(IErrorHandler simple, IDatabase complex)
        {
        }
    }

    [Fact]
    public void SingletonInSecondParameterTransientAsSecondParameterToTransient()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        builder.RegisterInstance<IErrorHandler>(null!);
        builder.RegisterType<Database>().As<IDatabase>();
        builder.RegisterType<SpecialCase>();
        var container = builder.Build();
        Assert.NotNull(container.Resolve<SpecialCase>());
    }

    [Fact]
    public void ReusingSingletonMultipleTimesInOneResolveOnceInSingleton()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>().SingleInstance();
        builder.RegisterType<Database>().As<IDatabase>();
        var container = builder.BuildAndVerify();
        var obj = container.Resolve<IDatabase>();
        Assert.NotNull(obj);
        Assert.NotNull(obj.ErrorHandler);
        Assert.NotNull(obj.Logger);
        Assert.Same(obj.Logger, obj.ErrorHandler.Logger);
        var obj2 = container.Resolve<IDatabase>();
        Assert.NotNull(obj2);
        Assert.NotSame(obj, obj2);
        Assert.Same(obj.ErrorHandler, obj2.ErrorHandler);
        Assert.Same(obj.Logger, obj2.Logger);
    }

    [Fact]
    public void CreatesFastFuncFactory()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        var container = builder.Build();
        var obj = container.Resolve<ILogger>();
        var fastFactory = container.Resolve<Func<ILogger>>();
        var obj2 = fastFactory();
        Assert.Same(obj, obj2);
    }

    [Fact]
    public void InjectionToConstructorWithOneParameterSingletonWithOptimization()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>();
        var container = builder.Build();
        var obj = container.Resolve<ILogger>();
        Assert.NotNull(obj);
        var obj2 = container.Resolve<IErrorHandler>();
        Assert.NotNull(obj2);
        Assert.NotNull(obj2.Logger);
        Assert.NotSame(obj, obj2);
        Assert.Same(obj, obj2.Logger);
    }

    [Fact]
    public void CanRegisterInstance()
    {
        var builder = new ContainerBuilder();
        var instance = new Logger();
        builder.RegisterInstance(instance).As<ILogger>();
        var container = builder.Build();
        var obj = container.Resolve<ILogger>();
        Assert.Same(instance, obj);
    }

    [Generate]
    public interface ICycle1
    {
        ICycle2 Cycle2Prop { get; }
    }

    [Generate]
    public interface ICycle2
    {
        ICycle1 Cycle1Prop { get; }
    }

    public class Cycle1 : ICycle1
    {
        readonly Lazy<ICycle2> _cycle2;

        public Cycle1(Lazy<ICycle2> cycle2)
        {
            _cycle2 = cycle2;
        }

        public ICycle2 Cycle2Prop => _cycle2.Value;
    }

    public class Cycle2 : ICycle2
    {
        readonly Lazy<ICycle1> _cycle1;

        public Cycle2(Lazy<ICycle1> cycle1)
        {
            _cycle1 = cycle1;
        }

        public ICycle1 Cycle1Prop => _cycle1.Value;
    }

    [Fact]
    public void CanBuildLazyCycle()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Cycle1>().As<ICycle1>().SingleInstance();
        builder.RegisterType<Cycle2>().As<ICycle2>();
        var container = builder.Build();
        var obj1 = container.Resolve<ICycle1>();
        var obj2 = obj1.Cycle2Prop;
        Assert.Same(obj1, obj2.Cycle1Prop);
    }

    [Generate]
    public class InjectingContainer
    {
        readonly IContainer _container;

        public InjectingContainer(IContainer container)
        {
            _container = container;
        }

        public IContainer Container => _container;
    }

    [Fact]
    public void CanInjectContainer()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<InjectingContainer>().As<InjectingContainer>();
        var container = builder.Build();
        var obj = container.Resolve<InjectingContainer>();
        Assert.Same(container, obj.Container);
    }

    [Fact]
    public void RegisterFactory()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory<InjectingContainer>((_, _) => (c, _) => new InjectingContainer(c))
            .As<InjectingContainer>();
        var container = builder.Build();
        var obj = container.Resolve<InjectingContainer>();
        Assert.Same(container, obj.Container);
        Assert.NotSame(obj, container.Resolve<InjectingContainer>());
    }

    [Fact]
    public void RegisterFactoryAsSingleton()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory<InjectingContainer>((_, _) => (c, _) => new InjectingContainer(c))
            .As<InjectingContainer>().SingleInstance();
        var container = builder.Build();
        var obj = container.Resolve<InjectingContainer>();
        Assert.Same(container, obj.Container);
        Assert.Same(obj, container.Resolve<InjectingContainer>());
    }

    [Fact]
    public void RegisterFactorySpecificInstanceType()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory((_, _) => (c, _) => new InjectingContainer(c), typeof(InjectingContainer))
            .As<InjectingContainer>();
        var container = builder.Build();
        var obj = container.Resolve<InjectingContainer>();
        Assert.Same(container, obj.Container);
        Assert.NotSame(obj, container.Resolve<InjectingContainer>());
    }

    [Fact]
    public void RegisterFactorySpecificInstanceTypeAsSingleton()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory((_, _) => (c, _) => new InjectingContainer(c), typeof(InjectingContainer))
            .As<InjectingContainer>().SingleInstance();
        var container = builder.Build();
        var obj = container.Resolve<InjectingContainer>();
        Assert.Same(container, obj.Container);
        Assert.Same(obj, container.Resolve<InjectingContainer>());
    }

    [Fact]
    public void RegisterAsImplementedInterfaces()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().AsImplementedInterfaces();
        var container = builder.Build();
        var log = container.Resolve<ILogger>();
        Assert.NotNull(log);
    }

    [Fact]
    public void RegisterAsSelf()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().AsSelf();
        var container = builder.Build();
        var log = container.Resolve<Logger>();
        Assert.NotNull(log);
    }

    [Fact]
    public void RegisterDefaultAsSelf()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>();
        var container = builder.Build();
        var log = container.Resolve<Logger>();
        Assert.NotNull(log);
    }

    [Fact]
    public void UnresolvableThrowsException()
    {
        var builder = new ContainerBuilder();
        var container = builder.Build();
        Assert.Throws<ArgumentException>(() => container.Resolve<string>());
    }

    [Fact]
    public void RegisterAssemblyTypes()
    {
        var builder = new ContainerBuilder();
        builder.AutoRegisterTypes();
        var container = builder.Build();
        var log = container.Resolve<Logger>();
        Assert.NotNull(log);
    }

    [Fact]
    public void AutoRegisterTypesWithWhereAndAsImplementedInterfaces()
    {
        var builder = new ContainerBuilder();
        builder.AutoRegisterTypes()
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.Contains("WithProps"))
            .AsImplementedInterfaces();
        var container = builder.Build();
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.NotSame(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void AutoRegisterTypesWithWhereAndAsImplementedInterfaces2()
    {
        var builder = new ContainerBuilder();
        builder.AutoRegisterTypes()
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && t.Name != "Database").AsImplementedInterfaces();
        var container = builder.Build();
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.NotSame(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void AutoRegisterTypesWithWhereAndAsImplementedInterfacesAsSingleton()
    {
        var builder = new ContainerBuilder();
        builder.AutoRegisterTypes()
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.Contains("WithProps"))
            .AsImplementedInterfaces().SingleInstance();
        var container = builder.Build();
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.Same(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void AutoRegisterTypesWithWhereAndAsImplementedInterfacesAsSingleton2()
    {
        var builder = new ContainerBuilder();
        builder.AutoRegisterTypes()
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && t.Name != "Database").AsImplementedInterfaces()
            .SingleInstance();
        var container = builder.Build();
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.Same(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void RegisterNamedService()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().Named<ILogger>("log");
        var container = builder.Build();
        var log = container.ResolveNamed<ILogger>("log");
        Assert.NotNull(log);
    }

    [Fact]
    public void RegisterKeyedService()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().Keyed<ILogger>(true);
        var container = builder.Build();
        var log = container.ResolveKeyed<ILogger>(true);
        Assert.NotNull(log);
    }

    [Fact]
    public void LastRegisteredHasPriority()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>();
        builder.RegisterInstance<ILogger>(null!).As<ILogger>();
        var container = builder.Build();
        Assert.Null(container.Resolve<ILogger>());
    }

    [Fact]
    public void CanPreserveExistingDefaults()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>();
        builder.RegisterInstance<ILogger>(default(ILogger)).As<ILogger>().PreserveExistingDefaults();
        var container = builder.Build();
        Assert.NotNull(container.Resolve<ILogger>());
    }

    [Fact]
    public void BasicEnumerableResolve()
    {
        var builder = new ContainerBuilder();
        builder.RegisterInstance("one").Keyed<string>(true);
        builder.RegisterInstance("bad").Keyed<string>(false);
        builder.RegisterInstance("two").Keyed<string>(true);
        var container = builder.Build();
        var result = container.ResolveKeyed<IEnumerable<string>>(true);
        Assert.Equal(new[] { "one", "two" }, result.ToArray());
    }

    [Fact]
    public void NullInstanceResolvedAsConstructorParameter()
    {
        var builder = new ContainerBuilder();
        builder.RegisterInstance<ILogger>(null);
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>();
        var container = builder.Build();
        var obj = container.Resolve<IErrorHandler>();
        Assert.Null(obj.Logger);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FuncWithOneObjectParameter(bool overload)
    {
        var builder = new ContainerBuilder();
        if (overload) builder.RegisterType<Logger>().As<ILogger>();
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>();
        var container = builder.Build();
        var factory = container.Resolve<Func<ILogger, IErrorHandler>>();
        var logger = new Logger();
        var obj = factory(logger);
        Assert.Equal(logger, obj.Logger);
    }

    [Generate]
    internal delegate IDatabase CreateDatabase(IErrorHandler errorHandler, ILogger logger);

    [Fact]
    public void FuncWithTwoObjectParameters()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Database>().As<IDatabase>();
        var container = builder.Build();
        var factory = container.Resolve<Func<IErrorHandler, ILogger, IDatabase>>();
        var obj = factory(null, null);
        Assert.NotNull(obj);
        var factory2 = container.Resolve<CreateDatabase>();
        obj = factory2(null!, null!);
        Assert.NotNull(obj);
    }

    [Fact]
    public void FuncWithTwoObjectParametersWithProps()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<DatabaseWithProps>().As<IDatabase>();
        var container = builder.Build();
        var factory = container.Resolve<Func<IErrorHandler, ILogger, IDatabase>>();
        var logger = new Logger();
        var obj = factory(null, logger);
        Assert.NotNull(obj);
        Assert.Same(logger, obj.Logger);
    }

    [Fact]
    public void AutowiredWithPropsRequired()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<DatabaseWithProps>().As<IDatabase>();
        var container = builder.Build();
        Assert.Throws<ArgumentException>(() => container.Resolve<Func<IErrorHandler, IDatabase>>());
        Assert.Throws<ArgumentException>(() => container.Resolve<Func<ILogger, IDatabase>>());
    }

    public partial class DatabaseWithOptionalProps : IDatabase
    {
        [BTDB.IOC.Dependency] public ILogger? Logger { get; private set; }
        [BTDB.IOC.Dependency] public IErrorHandler? ErrorHandler { get; private set; }
    }

    [Fact]
    public void FuncWithOptionalProps()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<DatabaseWithOptionalProps>().As<IDatabase>();
        var container = builder.Build();
        var factory = container.Resolve<Func<IDatabase>>();
        var obj = factory();
        Assert.NotNull(obj);
    }

    public partial class DatabaseWithDependencyProps : IDatabase
    {
        [BTDB.IOC.Dependency] public ILogger Logger { get; private set; }
        [BTDB.IOC.Dependency] public IErrorHandler? ErrorHandler { get; private set; }
    }

    [Fact]
    public void FuncWithDependencyProps()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<DatabaseWithDependencyProps>().As<IDatabase>();
        var container = builder.Build();
        var factory = container.Resolve<Func<ILogger, IDatabase>>();
        var logger = new Logger();
        var obj = factory(logger);
        Assert.NotNull(obj);
        Assert.Same(logger, obj.Logger);
    }

    [Generate]
    public class ClassWithRenamedDependencyProps
    {
        [BTDB.IOC.Dependency] public ILogger Logger { get; set; }
        [BTDB.IOC.Dependency("SuperLogger")] public ILogger Logger2 { get; set; }
    }

    [Fact]
    public void RenamingDependencies()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<ClassWithRenamedDependencyProps>().AsSelf();
        builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
        builder.RegisterType<Logger>().Named<ILogger>("SuperLogger").SingleInstance();
        var container = builder.Build();
        var obj = container.Resolve<ClassWithRenamedDependencyProps>();
        Assert.NotNull(obj);
        Assert.Same(container.Resolve<ILogger>(), obj.Logger);
        Assert.Same(container.ResolveNamed<ILogger>("SuperLogger"), obj.Logger2);
        Assert.NotSame(obj.Logger, obj.Logger2);
    }

    [Generate]
    public class KlassWith2IntParams
    {
        public int Param1 { get; }
        public int Param2 { get; }

        public KlassWith2IntParams(int param1, int param2)
        {
            Param1 = param1;
            Param2 = param2;
        }
    }

    [Generate]
    internal delegate KlassWith2IntParams KlassWith2IntParamsFactory(int param2, int param1);

    [Fact]
    public void DelegateWithNamedParameters()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<KlassWith2IntParams>();
        var container = builder.Build();
        var factory = container.Resolve<KlassWith2IntParamsFactory>();
        var obj = factory(22, 11);
        Assert.Equal(11, obj.Param1);
        Assert.Equal(22, obj.Param2);
    }

    public class Logger1 : ILogger
    {
    }

    public class Logger2 : ILogger
    {
    }

    static IContainer BuildContainerWithTwoLoggers()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger1>().AsImplementedInterfaces();
        builder.RegisterType<Logger2>().AsImplementedInterfaces();
        return builder.Build();
    }

    static void AssertTwoLoggers(IEnumerable<string> enumTypes)
    {
        Assert.Equal(new[] { "Logger1", "Logger2" }, enumTypes);
    }

    [Fact]
    public void AnythingCouldBeEnumerated()
    {
        var builder = new ContainerBuilder();
        var container = builder.Build();
        var allInstances = container.Resolve<IEnumerable<ILogger>>();
        Assert.NotNull(allInstances);
        Assert.Empty(allInstances);
    }

    [Fact]
    public void EnumerateAllInstances()
    {
        var container = BuildContainerWithTwoLoggers();
        var allInstances = container.Resolve<IEnumerable<ILogger>>();
        var enumTypes = allInstances.Select(i => i.GetType().Name);
        AssertTwoLoggers(enumTypes);
    }

    [Fact]
    public void EnumerateAllInstanceFactories()
    {
        var container = BuildContainerWithTwoLoggers();
        var allInstances = container.Resolve<IEnumerable<Func<ILogger>>>();
        var enumTypes = allInstances.Select(i => i().GetType().Name);
        AssertTwoLoggers(enumTypes);
    }

    [Fact]
    public void EnumerateAllLazyInstances()
    {
        var container = BuildContainerWithTwoLoggers();
        var allInstances = container.Resolve<IEnumerable<Lazy<ILogger>>>();
        var enumTypes = allInstances.Select(i => i.Value.GetType().Name);
        AssertTwoLoggers(enumTypes);
    }

    [Fact]
    public void ArrayOfInstances()
    {
        var container = BuildContainerWithTwoLoggers();
        var allInstances = container.Resolve<ILogger[]>();
        var enumTypes = allInstances.Select(i => i.GetType().Name);
        AssertTwoLoggers(enumTypes);
    }

    [Fact]
    public void TupleResolvable()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger1>().AsImplementedInterfaces();
        builder.RegisterInstance("hello");
        var container = builder.Build();
        var tuple = container.Resolve<Tuple<ILogger, string>>();
        Assert.Equal("Logger1", tuple.Item1.GetType().Name);
        Assert.Equal("hello", tuple.Item2);
    }

    static IContainer BuildContainerWithTwoLoggersAndTwoStrings()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger1>().AsImplementedInterfaces();
        builder.RegisterType<Logger2>().AsImplementedInterfaces();
        builder.RegisterInstance("A");
        builder.RegisterInstance("B");
        return builder.Build();
    }

    [Fact]
    public void EnumerateAllCombinations()
    {
        var container = BuildContainerWithTwoLoggersAndTwoStrings();
        var tuples = container.Resolve<IEnumerable<Tuple<ILogger, string>>>();
        var names = tuples.Select(t => t.Item1.GetType().Name + t.Item2);
        Assert.Equal(new[] { "Logger1A", "Logger1B", "Logger2A", "Logger2B" }, names);
    }

    [Fact]
    public void EnumerateAllCombinationsNested()
    {
        var container = BuildContainerWithTwoLoggersAndTwoStrings();
        var tuples = container.Resolve<IEnumerable<Tuple<ILogger, IEnumerable<string>>>>().ToArray();
        var enumTypes = tuples.Select(t => t.Item1.GetType().Name);
        AssertTwoLoggers(enumTypes);
        var names = tuples.SelectMany(t => t.Item2);
        Assert.Equal(new[] { "A", "B", "A", "B" }, names);
    }

    class PrivateLogger : ILogger
    {
    }

    [Fact]
    public void PrivateClassesCouldStillBeResolvedByFastReflection()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<PrivateLogger>().As<ILogger>().SingleInstance();
        var c = builder.BuildAndVerify(ContainerVerification.None);
        Assert.IsType<PrivateLogger>(c.Resolve<ILogger>());
    }

    public partial class PublicClassWithPrivateConstructor : ILogger
    {
        PublicClassWithPrivateConstructor()
        {
        }
    }

    [Fact]
    public void BuildingContainerWithRegisteredPartialClassWithPrivateConstructorWorks()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<PublicClassWithPrivateConstructor>().As<ILogger>();
        var container = builder.Build();
        Assert.NotNull(container.Resolve<ILogger>());
    }

    public class HardCycle1 : ICycle1
    {
        readonly ICycle2 _cycle2;

        public HardCycle1(ICycle2 cycle2)
        {
            _cycle2 = cycle2;
        }

        public ICycle2 Cycle2Prop => _cycle2;
    }

    public class HardCycle2 : ICycle2
    {
        readonly ICycle1 _cycle1;

        public HardCycle2(ICycle1 cycle1)
        {
            _cycle1 = cycle1;
        }

        public ICycle1 Cycle1Prop => _cycle1;
    }

    [Fact]
    public void ResolvingHardCycleShouldThrowException()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<HardCycle1>().As<ICycle1>().SingleInstance();
        builder.RegisterType<HardCycle2>().As<ICycle2>().SingleInstance();
        var container = builder.Build();
        Assert.Throws<InvalidOperationException>(() => container.Resolve<ICycle1>());
    }

    [Fact]
    public void SingletonByTwoInterfacesIsStillSameInstance()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<LoggerWithErrorHandler>().As<ILogger>().As<IErrorHandler>().SingleInstance();
        var container = builder.Build();
        var log1 = container.Resolve<IErrorHandler>();
        Assert.NotNull(log1);
        var log2 = container.Resolve<ILogger>();
        Assert.NotNull(log2);
        Assert.Same(log1, log2);
    }

    [Generate]
    public class MultipleConstructors
    {
        public readonly String Desc;

        public MultipleConstructors()
        {
            Desc = "";
        }

        public MultipleConstructors(int i)
        {
            Desc = "Int " + i.ToString(CultureInfo.InvariantCulture);
        }

        public MultipleConstructors(string s)
        {
            Desc = "String " + s;
        }

        public MultipleConstructors(int i, int j)
        {
            Desc = "Int " + i.ToString(CultureInfo.InvariantCulture) + ", Int " +
                   j.ToString(CultureInfo.InvariantCulture);
        }
    }

    [Fact]
    public void UsingConstructorWorks()
    {
        var builder = new ContainerBuilder();
        builder.RegisterInstance(7).Named<int>("i");
        builder.RegisterInstance(3).Named<int>("j");
        builder.RegisterInstance("A").Named<string>("s");
        builder.RegisterType<MultipleConstructors>().Keyed<MultipleConstructors>(1);
        builder.RegisterFactory<MultipleConstructors>((_, _) => (_, _) => new MultipleConstructors())
            .Keyed<MultipleConstructors>(2);
        builder.RegisterFactory<MultipleConstructors>((c, r) =>
        {
            var pi = c.CreateFactory(r, typeof(int), "i");
            if (pi == null) throw new Exception("Value for parameter i is not registered");
            return (c2, r2) => new MultipleConstructors((int)pi(c2, r2));
        }).Keyed<MultipleConstructors>(3);
        builder.RegisterFactory<MultipleConstructors>((c, r) =>
        {
            var ps = c.CreateFactory(r, typeof(string), "s");
            if (ps == null) throw new Exception("Value for parameter s is not registered");
            return (c2, r2) => new MultipleConstructors((string)ps(c2, r2));
        }).Keyed<MultipleConstructors>(4);
        builder.RegisterFactory<MultipleConstructors>((c, r) =>
        {
            var pi = c.CreateFactory(r, typeof(int), "i");
            if (pi == null) throw new Exception("Value for parameter i is not registered");
            var pj = c.CreateFactory(r, typeof(int), "j");
            if (pj == null) throw new Exception("Value for parameter j is not registered");
            return (c2, r2) => new MultipleConstructors((int)pi(c2, r2), (int)pj(c2, r2));
        }).Keyed<MultipleConstructors>(5);
        var container = builder.Build();
        Assert.Equal("Int 7, Int 3", container.ResolveKeyed<MultipleConstructors>(1).Desc);
        Assert.Equal("", container.ResolveKeyed<MultipleConstructors>(2).Desc);
        Assert.Equal("Int 7", container.ResolveKeyed<MultipleConstructors>(3).Desc);
        Assert.Equal("String A", container.ResolveKeyed<MultipleConstructors>(4).Desc);
        Assert.Equal("Int 7, Int 3", container.ResolveKeyed<MultipleConstructors>(5).Desc);
    }

    [Generate]
    public interface ISupport
    {
    }

    public class Support : ISupport
    {
    }

    [Generate]
    public interface INotify
    {
    }

    public class Notification : INotify
    {
        public Notification(ISupport support)
        {
        }
    }

    public class NotificationOverride : INotify
    {
        public NotificationOverride(ISupport support)
        {
        }
    }

    [Generate]
    public interface IRefinable
    {
    }

    public class RefinePreview : IRefinable
    {
        public RefinePreview(Lazy<IWorld> world, INotify notify)
        {
            Assert.IsType<NotificationOverride>(notify);
        }
    }

    [Generate]
    public interface IOwinServer
    {
    }

    public class WorldHttpHandler : IOwinServer
    {
        public WorldHttpHandler(Lazy<IWorld> world, IEnumerable<IRefinable> refinables)
        {
            Assert.Single(refinables);
        }
    }

    [Generate]
    public interface IWorld
    {
    }

    public class World : IWorld
    {
        public World(IOwinServer server, ISupport support, INotify notifyChanges)
        {
        }
    }

    [Fact]
    public void DependenciesInEnumerablesWorks()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<RefinePreview>().AsImplementedInterfaces().SingleInstance();
        builder.RegisterType<WorldHttpHandler>().AsImplementedInterfaces().SingleInstance();
        builder.RegisterType<World>().AsImplementedInterfaces().SingleInstance();
        builder.RegisterType<Notification>().AsImplementedInterfaces().SingleInstance();
        builder.RegisterType<NotificationOverride>().AsImplementedInterfaces().SingleInstance();
        builder.RegisterType<Support>().AsImplementedInterfaces().SingleInstance();

        var container = builder.Build();
        var notificationOverride = container.Resolve<INotify>();
        Assert.IsType<NotificationOverride>(notificationOverride);
        var world = container.Resolve<IWorld>();
    }

    [Generate]
    public interface IHandler
    {
    }

    public class Handler : IHandler
    {
        public Handler(Func<ILogger> logger)
        {
            logger();
        }
    }

    [Fact]
    public void FunctionDependencyWithSubdependency()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<Logger>().AsImplementedInterfaces();
        containerBuilder.RegisterType<Handler>().AsImplementedInterfaces().SingleInstance();

        var container = containerBuilder.Build();
        var handler = container.Resolve<IHandler>();
        Assert.NotNull(handler);
    }

    public class EnhancedLogger : ILogger
    {
        readonly ILogger _parent;

        public EnhancedLogger(ILogger parent)
        {
            _parent = parent;
        }

        public ILogger Parent => _parent;
    }

    [Fact]
    public void EnhancingImplementationPossible()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<Logger>().AsImplementedInterfaces().Named<ILogger>("parent");
        containerBuilder.RegisterType<EnhancedLogger>().AsImplementedInterfaces();
        var container = containerBuilder.Build();
        var handler = container.Resolve<ILogger>();
        Assert.IsType<EnhancedLogger>(handler);
        Assert.IsType<Logger>(((EnhancedLogger)handler).Parent);
    }

    [Fact]
    public void RegisterInstanceUsesGenericParameterForRegistration()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterInstance<ILogger>(new Logger());
        var container = containerBuilder.Build();
        Assert.NotNull(container.Resolve<ILogger>());
    }

    [Fact]
    public void RegisterInstanceWithObjectParamUsesRealObjectType()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterInstance((object)new Logger());
        var container = containerBuilder.Build();
        Assert.NotNull(container.Resolve<Logger>());
    }

    internal class ClassDependency
    {
    }

    internal struct StructDependency
    {
    }

    internal enum EnumDependency
    {
        Foo,
        Bar,
        FooBar = Foo | Bar
    }

    [Generate]
    internal abstract class OptionalClass<T>
    {
        public T Value { get; }

        protected OptionalClass(T t) => Value = t;

        public override bool Equals(object obj)
        {
            var @class = obj as OptionalClass<T>;
            return @class != null &&
                   EqualityComparer<T>.Default.Equals(Value, @class.Value);
        }

        public override int GetHashCode()
        {
            return -1937169414 + EqualityComparer<T>.Default.GetHashCode(Value);
        }

        public override string ToString() => $"{Value}";
    }

    internal class ClassWithTrueBool : OptionalClass<bool>
    {
        public ClassWithTrueBool(bool foo = true) : base(foo)
        {
        }
    }

    internal class ClassWithFalseBool : OptionalClass<bool>
    {
        public ClassWithFalseBool(bool foo = false) : base(foo)
        {
        }
    }

    internal class ClassWithInt16 : OptionalClass<Int16>
    {
        public ClassWithInt16(Int16 foo = 11111) : base(foo)
        {
        }
    }

    internal class ClassWithInt32 : OptionalClass<Int32>
    {
        public ClassWithInt32(Int32 foo = Int32.MaxValue) : base(foo)
        {
        }
    }

    internal class ClassWithInt64 : OptionalClass<Int64>
    {
        public ClassWithInt64(Int64 foo = Int64.MaxValue) : base(foo)
        {
        }
    }

    internal class ClassWithFloat : OptionalClass<float>
    {
        public ClassWithFloat(float foo = 1.1f) : base(foo)
        {
        }
    }

    internal class ClassWithDouble : OptionalClass<double>
    {
        public ClassWithDouble(double foo = 2.2d) : base(foo)
        {
        }
    }

    internal class ClassWithDoubleCastedFromFloat : OptionalClass<double>
    {
        public ClassWithDoubleCastedFromFloat(double foo = 2.2f) : base(foo)
        {
        }
    }

    internal class ClassWithDecimal : OptionalClass<decimal>
    {
        public ClassWithDecimal(decimal foo = 3.3m) : base(foo)
        {
        }
    }

    internal class ClassWithString : OptionalClass<string>
    {
        public ClassWithString(string foo = "str") : base(foo)
        {
        }
    }

    internal class ClassWithClass : OptionalClass<ClassDependency>
    {
        public ClassWithClass(ClassDependency foo = default) : base(foo)
        {
        }
    }

    internal class ClassWithStruct : OptionalClass<StructDependency>
    {
        public ClassWithStruct(StructDependency foo = default) : base(foo)
        {
        }
    }

    internal class ClassWithEnum : OptionalClass<EnumDependency>
    {
        public ClassWithEnum(EnumDependency foo = EnumDependency.Foo) : base(foo)
        {
        }
    }

    internal class ClassWithEnum2 : OptionalClass<EnumDependency>
    {
        public ClassWithEnum2(EnumDependency foo = EnumDependency.FooBar) : base(foo)
        {
        }
    }

    internal class ClassWithNullable : OptionalClass<int?>
    {
        public ClassWithNullable(int? foo = default) : base(foo)
        {
        }
    }

    internal class ClassWithNullable2 : OptionalClass<int?>
    {
        public ClassWithNullable2(int? foo = 10) : base(foo)
        {
        }
    }

    internal class ClassWithNullableStruct : OptionalClass<StructDependency?>
    {
        public ClassWithNullableStruct(StructDependency? foo = default) : base(foo)
        {
        }
    }

    internal class ClassWithDateTime : OptionalClass<DateTime>
    {
        public ClassWithDateTime(DateTime foo = default) : base(foo)
        {
        }
    }

    internal class ClassWithNullableDateTime : OptionalClass<DateTime?>
    {
        public ClassWithNullableDateTime(DateTime? foo = default) : base(foo)
        {
        }
    }

    [Theory]
    [InlineData(typeof(ClassWithTrueBool))]
    [InlineData(typeof(ClassWithFalseBool))]
    [InlineData(typeof(ClassWithInt16))]
    [InlineData(typeof(ClassWithInt32))]
    [InlineData(typeof(ClassWithInt64))]
    [InlineData(typeof(ClassWithFloat))]
    [InlineData(typeof(ClassWithDouble))]
    [InlineData(typeof(ClassWithDoubleCastedFromFloat))]
    [InlineData(typeof(ClassWithDecimal))]
    [InlineData(typeof(ClassWithString))]
    [InlineData(typeof(ClassWithClass))]
    [InlineData(typeof(ClassWithStruct))]
    [InlineData(typeof(ClassWithEnum))]
    [InlineData(typeof(ClassWithEnum2))]
    [InlineData(typeof(ClassWithNullable))]
    [InlineData(typeof(ClassWithNullable2))]
    [InlineData(typeof(ClassWithNullableStruct))]
    [InlineData(typeof(ClassWithDateTime))]
    [InlineData(typeof(ClassWithNullableDateTime))]
    public void ResolveWithOptionalParameterWithoutRegister(Type type)
    {
        var expected = Create(type);

        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType(type);
        var container = containerBuilder.Build();

        var actual = container.Resolve(type);

        Assert.Equal(expected, actual);
        return;

        object? Create(Type t) => Activator.CreateInstance(t,
            BindingFlags.CreateInstance | BindingFlags.Public | BindingFlags.Instance |
            BindingFlags.OptionalParamBinding, null, new[] { Type.Missing }, CultureInfo.CurrentCulture);
    }

    internal class ClassWithRegisteredOptionalParam : OptionalClass<ClassWithInt32?>
    {
        public ClassWithRegisteredOptionalParam(ClassWithInt32? t = null) : base(t)
        {
        }
    }

    [Fact]
    public void ResolveWithOptionalParameterWithRegister()
    {
        var expected = new ClassWithRegisteredOptionalParam(new ClassWithInt32(42));

        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<ClassWithRegisteredOptionalParam>();
        containerBuilder.RegisterInstance(expected.Value);
        var container = containerBuilder.Build();

        var actual = container.Resolve<ClassWithRegisteredOptionalParam>();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void CanObtainRawDefaultValueOfDateTime()
    {
        var ctor = typeof(ClassWithDateTime).GetConstructors()[0];
        var dateTimeParameter = ctor.GetParameters()[0];
        Assert.True(dateTimeParameter.HasDefaultValue);
        Assert.Null(dateTimeParameter.RawDefaultValue);
    }

    internal class ClassWithDispose : ILogger, IAsyncDisposable, IDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Task.Delay(0);
        }

        public void Dispose()
        {
        }
    }

    [Fact]
    public void DisposableInterfacesAreNotRegisteredAutomatically()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterInstance(new ClassWithDispose()).AsImplementedInterfaces();
        var container = containerBuilder.Build();
        Assert.NotNull(container.Resolve<ILogger>());
        Assert.Throws<ArgumentException>(() => container.Resolve<IDisposable>());
        Assert.Throws<ArgumentException>(() => container.Resolve<IAsyncDisposable>());
    }

    [Fact]
    public void DisposableInterfacesCanBeRegisteredManually()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterInstance(new ClassWithDispose()).AsImplementedInterfaces().As<IDisposable>()
            .As<IAsyncDisposable>();
        var container = containerBuilder.Build();
        Assert.NotNull(container.Resolve<IDisposable>());
        Assert.NotNull(container.Resolve<IAsyncDisposable>());
    }

    [Fact]
    public void ResolveOptionalWorks()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterInstance(new ClassWithDispose()).AsImplementedInterfaces();
        var container = containerBuilder.Build();
        Assert.NotNull(container.ResolveOptional<ILogger>());
        Assert.Null(container.ResolveOptional<IDatabase>());
        Assert.Null(container.ResolveOptional<Func<IDatabase>>());
    }

    [Fact]
    public void ResolveOptionalKeyedWorks()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().Keyed<ILogger>(true);
        var container = builder.Build();
        Assert.NotNull(container.ResolveOptionalKeyed<ILogger>(true));
        Assert.Null(container.ResolveOptionalKeyed<ILogger>(false));
    }

    [Fact]
    public void ResolveOptionalNamedWorks()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().Named<ILogger>("A");
        var container = builder.Build();
        Assert.NotNull(container.ResolveOptionalNamed<ILogger>("A"));
        Assert.Null(container.ResolveOptionalNamed<ILogger>("B"));
    }

    [Fact]
    public void VerificationFailsWhenSingletonUsesTransient()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>();
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>().SingleInstance();
        Assert.Throws<BTDBException>(() => builder.BuildAndVerify());
    }

    [Generate]
    internal class Foo
    {
        internal TimeSpan? Bar;

        public Foo(TimeSpan? param)
        {
            Bar = param;
        }
    }

    [Fact]
    public void InjectNullableStructDoesNotCrash()
    {
        var builder = new ContainerBuilder();
        TimeSpan? timeSpan = TimeSpan.FromHours(1);
        builder.RegisterInstance<TimeSpan?>(timeSpan);
        builder.RegisterType<Foo>();
        var container = builder.Build();
        Assert.Equal(TimeSpan.FromHours(1), container.Resolve<Foo>().Bar);
    }

    [Fact]
    public void InjectNullableStructWithoutValueDoesNotCrash()
    {
        var builder = new ContainerBuilder();
        builder.RegisterInstance<TimeSpan?>(null!);
        builder.RegisterType<Foo>();
        var container = builder.Build();
        Assert.False(container.Resolve<Foo>().Bar.HasValue);
    }

    [Fact]
    public void InjectStructByFactory()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory<Foo>((_, _) => (_, _) => new Foo(TimeSpan.FromHours(1)));
        var container = builder.Build();
        var foo = container.Resolve<Foo>();
        Assert.Equal(TimeSpan.FromHours(1), foo.Bar);
    }

    [Fact]
    public void UniquenessOfRegistrationsCouldBeEnforced()
    {
        var builder = new ContainerBuilder(ContainerBuilderBehaviour.UniqueRegistrations);
        builder.RegisterType<Logger>().As<ILogger>();
        builder.RegisterType<Logger>().As<ILogger>();
        Assert.Throws<BTDBException>(() => builder.Build());
    }

    [Fact]
    public void UniquenessOfRegistrationsCouldBeEnforcedConfiguredForEveryRegistration()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<Logger>().As<ILogger>().UniqueRegistration(true);
        builder.RegisterType<Logger>().As<ILogger>().UniqueRegistration(true);
        Assert.Throws<BTDBException>(() => builder.Build());
    }

    [Fact]
    public void UniquenessOfRegistrationsCouldBeOverriden()
    {
        var builder = new ContainerBuilder(ContainerBuilderBehaviour.UniqueRegistrations);
        builder.RegisterType<Logger>().As<ILogger>().UniqueRegistration(false);
        builder.RegisterType<Logger>().As<ILogger>().UniqueRegistration(false);
        Assert.NotNull(builder.Build());
    }

    [Fact]
    public void ObsoleteRegisterFactorySpecificInstanceType()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory(c => new InjectingContainer(c), typeof(InjectingContainer));
        var container = builder.Build();
        var obj = container.Resolve<InjectingContainer>();
        Assert.Same(container, obj.Container);
        Assert.NotSame(obj, container.Resolve<InjectingContainer>());
    }

    [Fact]
    public void ObsoleteRegisterFactorySpecificInstanceTypeAsSingleton()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory(c => new InjectingContainer(c), typeof(InjectingContainer)).SingleInstance();
        var container = builder.Build();
        var obj = container.Resolve<InjectingContainer>();
        Assert.Same(container, obj.Container);
        Assert.Same(obj, container.Resolve<InjectingContainer>());
    }

    [Fact]
    public void RegisterAssemblyTypesWithWhereAndAsImplementedInterfaces()
    {
        var builder = new ContainerBuilder();
        builder.RegisterAssemblyTypes(typeof(Logger).Assembly)
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.Contains("WithProps"))
            .AsImplementedInterfaces();
        var container = builder.BuildAndVerify(ContainerVerification.None);
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.NotSame(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void RegisterAssemblyTypesWithWhereAndAsImplementedInterfaces2()
    {
        var builder = new ContainerBuilder();
        builder.RegisterAssemblyTypes(typeof(Logger).Assembly)
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && t.Name != "Database").AsImplementedInterfaces();
        var container = builder.BuildAndVerify(ContainerVerification.None);
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.NotSame(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void RegisterAssemblyTypesWithWhereAndAsImplementedInterfacesAsSingleton()
    {
        var builder = new ContainerBuilder();
        builder.RegisterAssemblyTypes(typeof(Logger).Assembly)
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.Contains("WithProps"))
            .AsImplementedInterfaces().SingleInstance();
        var container = builder.BuildAndVerify(ContainerVerification.None);
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.Same(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void RegisterAssemblyTypesWithWhereAndAsImplementedInterfacesAsSingleton2()
    {
        var builder = new ContainerBuilder();
        builder.RegisterAssemblyTypes(typeof(Logger).Assembly)
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && t.Name != "Database").AsImplementedInterfaces()
            .SingleInstance();
        var container = builder.BuildAndVerify(ContainerVerification.None);
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.Same(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    [Fact]
    public void MoreComplexManualFactoryRegistrationShowcase()
    {
        var builder = new ContainerBuilder();
        builder.AutoRegisterTypes()
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.StartsWith("Database"))
            .AsImplementedInterfaces();
        var callCounter = 0;
        builder.RegisterFactory<Database>((container, ctx) =>
        {
            callCounter++;
            var f0 = container.CreateFactory(ctx, typeof(IErrorHandler), "handler");
            if (f0 == null)
                throw new ArgumentException(
                    "Cannot resolve BTDBTest.IOCDomain.IErrorHandler handler parameter of BTDBTest.IOCDomain.Database");
            var f1 = container.CreateFactory(ctx, typeof(ILogger), "logger");
            if (f1 == null)
                throw new ArgumentException(
                    "Cannot resolve BTDBTest.IOCDomain.ILogger logger parameter of BTDBTest.IOCDomain.Database");
            return (container2, ctx2) =>
            {
                var res = new Database(Unsafe.As<IErrorHandler>(f0(container2, ctx2)),
                    Unsafe.As<ILogger>(f1(container2, ctx2)));
                return res;
            };
        }).AsImplementedInterfaces();
        var container = builder.Build();
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.NotSame(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
        Assert.Equal(2, callCounter);
        var rootFactory = container.Resolve<Func<IWebService>>();
        Assert.Equal(4, callCounter);
        rootFactory();
        root = rootFactory();
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.Equal(4, callCounter);
    }

    [Fact]
    public void RegisterByConstructorParameters()
    {
        var builder = new ContainerBuilder();
        builder.AutoRegisterTypes()
            .Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.StartsWith("Database"))
            .AsImplementedInterfaces();
        builder.RegisterTypeWithConstructorParameters<Database>(typeof(IErrorHandler), typeof(ILogger))
            .AsImplementedInterfaces();
        var container = builder.Build();
        var root = container.Resolve<IWebService>();
        Assert.NotNull(root);
        Assert.NotNull(root.Authenticator.Database.Logger);
        Assert.NotSame(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
    }

    public class NonGeneratedWithLazyDependency
    {
        public NonGeneratedWithLazyDependency(Lazy<ILogger> logger)
        {
            Assert.IsType<Lazy<ILogger>>(logger);
        }
    }

    [Fact]
    public void NonGeneratedClassWithLazyDependency()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<NonGeneratedWithLazyDependency>();
        builder.RegisterType<Logger>().As<ILogger>();
        var container = builder.Build();
        var obj = container.Resolve<NonGeneratedWithLazyDependency>();
        Assert.NotNull(obj);
    }
}
