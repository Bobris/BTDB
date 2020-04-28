using BTDB.IOC;
using BTDB.KVDBLayer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace BTDBTest
{
    using IOCDomain;

    public class IocTests
    {
        [Fact]
        public void AlwaysNew()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<Logger>().As<ILogger>();
            var container = builder.Build();
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
            var container = builder.Build();
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
            builder.RegisterInstance<IErrorHandler>(null);
            builder.RegisterType<Database>().As<IDatabase>();
            builder.RegisterType<SpecialCase>();
            var container = builder.Build();
            container.Resolve<SpecialCase>();
        }

        [Fact]
        public void ReusingSingletonMultipleTimesInOneResolveOnceInSingleton()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<Logger>().As<ILogger>().SingleInstance();
            builder.RegisterType<ErrorHandler>().As<IErrorHandler>().SingleInstance();
            builder.RegisterType<Database>().As<IDatabase>();
            var container = builder.Build();
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

        public interface ICycle1
        {
            ICycle2 Cycle2Prop { get; }
        }

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
            builder.RegisterFactory(c => new InjectingContainer(c)).As<InjectingContainer>();
            var container = builder.Build();
            var obj = container.Resolve<InjectingContainer>();
            Assert.Same(container, obj.Container);
            Assert.NotSame(obj, container.Resolve<InjectingContainer>());
        }

        [Fact]
        public void RegisterFactoryAsSingleton()
        {
            var builder = new ContainerBuilder();
            builder.RegisterFactory(c => new InjectingContainer(c)).As<InjectingContainer>().SingleInstance();
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
            builder.RegisterAssemblyTypes(typeof(Logger).Assembly);
            var container = builder.Build();
            var log = container.Resolve<Logger>();
            Assert.NotNull(log);
        }

        [Fact]
        public void RegisterAssemblyTypesWithWhereAndAsImplementedInterfaces()
        {
            var builder = new ContainerBuilder();
            builder.RegisterAssemblyTypes(typeof(Logger).Assembly)
                .Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.Contains("WithProps"))
                .AsImplementedInterfaces();
            var container = builder.Build();
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
                .Where(t => t.Namespace == "BTDBTest.IOCDomain" && t.Name != "Database").AsImplementedInterfaces().PropertiesAutowired();
            var container = builder.Build();
            var root = container.Resolve<IWebService>();
            Assert.NotNull(root);
            Assert.NotNull(root.Authenticator.Database.Logger);
            Assert.NotSame(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
        }

        [Fact]
        public void RegisterAssemblyTypesWithWhereAndAsImplementedInterfacesAsSingleton()
        {
            var builder = new ContainerBuilder();
            builder.RegisterAssemblyTypes(typeof(Logger).Assembly).Where(t => t.Namespace == "BTDBTest.IOCDomain" && !t.Name.Contains("WithProps")).
                AsImplementedInterfaces().SingleInstance();
            var container = builder.Build();
            var root = container.Resolve<IWebService>();
            Assert.NotNull(root);
            Assert.NotNull(root.Authenticator.Database.Logger);
            Assert.Same(root.StockQuote.ErrorHandler.Logger, root.Authenticator.Database.Logger);
        }

        [Fact]
        public void RegisterAssemblyTypesWithWhereAndAsImplementedInterfacesAsSingleton2()
        {
            var builder = new ContainerBuilder();
            builder.RegisterAssemblyTypes(typeof(Logger).Assembly).Where(t => t.Namespace == "BTDBTest.IOCDomain" && t.Name != "Database").
                AsImplementedInterfaces().SingleInstance().PropertiesAutowired();
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
        public void NullInstanceResovedAsConstructorParameter()
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

        [Fact]
        public void FuncWithTwoObjectParameters()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<Database>().As<IDatabase>();
            var container = builder.Build();
            var factory = container.Resolve<Func<IErrorHandler, ILogger, IDatabase>>();
            var obj = factory(null, null);
            Assert.NotNull(obj);
        }

        [Fact]
        public void FuncWithTwoObjectParametersWithProps()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<DatabaseWithProps>().As<IDatabase>().PropertiesAutowired();
            var container = builder.Build();
            var factory = container.Resolve<Func<IErrorHandler, ILogger, IDatabase>>();
            var logger = new Logger();
            var obj = factory(null, logger);
            Assert.NotNull(obj);
            Assert.Same(logger, obj.Logger);
        }

        [Fact]
        public void FuncWithTwoObjectParametersWithPropsAreRequired()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<DatabaseWithProps>().As<IDatabase>().PropertiesAutowired();
            var container = builder.Build();
            Assert.Throws<ArgumentException>(() => container.Resolve<Func<IErrorHandler, IDatabase>>());
            Assert.Throws<ArgumentException>(() => container.Resolve<Func<ILogger, IDatabase>>());
        }

        public class DatabaseWithOptionalProps : IDatabase
        {
            public ILogger? Logger { get; private set; }
            public IErrorHandler? ErrorHandler { get; private set; }
        }

        [Fact]
        public void FuncWithTwoObjectParametersWithOptionalProps()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<DatabaseWithOptionalProps>().As<IDatabase>().PropertiesAutowired();
            var container = builder.Build();
            var factory = container.Resolve<Func<IDatabase>>();
            var obj = factory();
            Assert.NotNull(obj);
        }

        public class KlassWith2IntParams
        {
            public int Param1 { get; private set; }
            public int Param2 { get; private set; }

            public KlassWith2IntParams(int param1, int param2)
            {
                Param1 = param1;
                Param2 = param2;
            }
        }

        delegate KlassWith2IntParams KlassWith2IntParamsFactory(int param2, int param1);

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
        public void CanInstantiatePrivateClassAsSingleton()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<PrivateLogger>().As<ILogger>().SingleInstance();
            var container = builder.Build();
            var log1 = container.Resolve<ILogger>();
            Assert.NotNull(log1);
        }

        public class PublicClassWithPrivateConstructor : ILogger
        {
            PublicClassWithPrivateConstructor()
            {
            }
        }

        [Fact]
        public void BuildingContainerWithRegisteredTypeWithPrivateConstructorShouldThrow()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<PublicClassWithPrivateConstructor>().As<ILogger>();
            Assert.Throws<ArgumentException>(() => builder.Build());
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
            builder.RegisterType<MultipleConstructors>().UsingConstructor().Keyed<MultipleConstructors>(2);
            builder.RegisterType<MultipleConstructors>().UsingConstructor(typeof(int)).Keyed<MultipleConstructors>(3);
            builder.RegisterType<MultipleConstructors>().UsingConstructor(typeof(string)).Keyed<MultipleConstructors>(4);
            builder.RegisterType<MultipleConstructors>().UsingConstructor(typeof(int), typeof(int)).Keyed<MultipleConstructors>(5);
            var container = builder.Build();
            Assert.Equal("Int 7, Int 3", container.ResolveKeyed<MultipleConstructors>(1).Desc);
            Assert.Equal("", container.ResolveKeyed<MultipleConstructors>(2).Desc);
            Assert.Equal("Int 7", container.ResolveKeyed<MultipleConstructors>(3).Desc);
            Assert.Equal("String A", container.ResolveKeyed<MultipleConstructors>(4).Desc);
            Assert.Equal("Int 7, Int 3", container.ResolveKeyed<MultipleConstructors>(5).Desc);
        }

        public interface ISupport { }
        public class Support : ISupport
        {
        }

        public interface INotify { }
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


        public interface IRefinable { }
        public class RefinePreview : IRefinable
        {
            public RefinePreview(Lazy<IWorld> world, INotify notify)
            {
                Assert.IsType<NotificationOverride>(notify);
            }
        }

        public interface IOwinServer { }
        public class WorldHttpHandler : IOwinServer
        {
            public WorldHttpHandler(Lazy<IWorld> world, IEnumerable<IRefinable> refinables)
            {
                Assert.Single(refinables);
            }
        }

        public interface IWorld { }
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

        public interface IHandler { }

        public class Handler : IHandler
        {
            readonly ILogger _logger;

            public Handler(Func<ILogger> logger)
            {
                _logger = logger();
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

        class GreedyNonPublicClass
        {
            public GreedyNonPublicClass(ILogger a, ILogger b, ILogger c, ILogger d, ILogger e, ILogger f)
            {
            }
        }

        [Fact]
        public void ThrowsExceptionForTooManyArgumentsInNonPublicClass()
        {
            var containerBuilder = new ContainerBuilder();
            containerBuilder.RegisterType<GreedyNonPublicClass>().AsImplementedInterfaces();
            foreach (var name in new[] { "a", "b", "c", "d", "e", "f" })
                containerBuilder.RegisterType<Logger>().AsImplementedInterfaces().Named<ILogger>(name);
            var container = containerBuilder.Build();
            if (Debugger.IsAttached)
            {
                var ex = Assert.Throws<BTDBException>(() => container.Resolve<GreedyNonPublicClass>());
                Assert.Contains("Greedy", ex.Message);
                Assert.Contains("Unsupported", ex.Message);
            }
            else
            {
                Assert.NotNull(container.Resolve<GreedyNonPublicClass>());
            }
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

        class ClassDependency { }
        struct StructDependency { }
        enum EnumDependency { Foo, Bar, FooBar = Foo | Bar }
        abstract class OptionalClass<T>
        {
            public T Value { get; }

            public OptionalClass(T t) => Value = t;

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
        class ClassWithTrueBool : OptionalClass<bool> { public ClassWithTrueBool(bool foo = true) : base(foo) { } }
        class ClassWithFalseBool : OptionalClass<bool> { public ClassWithFalseBool(bool foo = false) : base(foo) { } }
        class ClassWithInt16 : OptionalClass<Int16> { public ClassWithInt16(Int16 foo = 11111) : base(foo) { } }
        class ClassWithInt32 : OptionalClass<Int32> { public ClassWithInt32(Int32 foo = Int32.MaxValue) : base(foo) { } }
        class ClassWithInt64 : OptionalClass<Int64> { public ClassWithInt64(Int64 foo = Int64.MaxValue) : base(foo) { } }
        class ClassWithFloat : OptionalClass<float> { public ClassWithFloat(float foo = 1.1f) : base(foo) { } }
        class ClassWithDouble : OptionalClass<double> { public ClassWithDouble(double foo = 2.2d) : base(foo) { } }
        class ClassWithDoubleCastedFromFloat : OptionalClass<double> { public ClassWithDoubleCastedFromFloat(double foo = 2.2f) : base(foo) { } }
        class ClassWithDecimal : OptionalClass<decimal> { public ClassWithDecimal(decimal foo = 3.3m) : base(foo) { } }
        class ClassWithString : OptionalClass<string> { public ClassWithString(string foo = "str") : base(foo) { } }
        class ClassWithClass : OptionalClass<ClassDependency> { public ClassWithClass(ClassDependency foo = default) : base(foo) { } }
        class ClassWithStruct : OptionalClass<StructDependency> { public ClassWithStruct(StructDependency foo = default) : base(foo) { } }
        class ClassWithEnum : OptionalClass<EnumDependency> { public ClassWithEnum(EnumDependency foo = EnumDependency.Foo) : base(foo) { } }
        class ClassWithEnum2 : OptionalClass<EnumDependency> { public ClassWithEnum2(EnumDependency foo = EnumDependency.FooBar) : base(foo) { } }
        class ClassWithNullable : OptionalClass<int?> { public ClassWithNullable(int? foo = default) : base(foo) { } }
        class ClassWithNullable2 : OptionalClass<int?> { public ClassWithNullable2(int? foo = 10) : base(foo) { } }
        class ClassWithNullableStruct : OptionalClass<StructDependency?> { public ClassWithNullableStruct(StructDependency? foo = default) : base(foo) { } }
        class ClassWithDateTime : OptionalClass<DateTime> { public ClassWithDateTime(DateTime foo = default) : base(foo) { } }
        class ClassWithNullableDateTime : OptionalClass<DateTime?> { public ClassWithNullableDateTime(DateTime? foo = default) : base(foo) { } }

        [Theory]
        [InlineData(typeof(ClassWithTrueBool))]
        [InlineData(typeof(ClassWithFalseBool))]
        [InlineData(typeof(ClassWithInt16))]
        [InlineData(typeof(ClassWithInt32))]
        [InlineData(typeof(ClassWithInt64))]
        [InlineData(typeof(ClassWithFloat))]
        [InlineData(typeof(ClassWithDouble))]
        [InlineData(typeof(ClassWithDoubleCastedFromFloat))]
        //[InlineData(typeof(ClassWithDecimal), Skip = "Not supported yet")]
        [InlineData(typeof(ClassWithString))]
        [InlineData(typeof(ClassWithClass))]
        [InlineData(typeof(ClassWithStruct))]
        [InlineData(typeof(ClassWithEnum))]
        [InlineData(typeof(ClassWithEnum2))]
        [InlineData(typeof(ClassWithNullable))]
        [InlineData(typeof(ClassWithNullable2))]
        [InlineData(typeof(ClassWithNullableStruct))]
        //[InlineData(typeof(ClassWithDateTime), Skip = "Not supported yet")]
        [InlineData(typeof(ClassWithNullableDateTime))]
        public void ResolveWithOptionalParameterWithoutRegister(Type type)
        {
            object Create(Type t) => Activator.CreateInstance(t, BindingFlags.CreateInstance | BindingFlags.Public | BindingFlags.Instance | BindingFlags.OptionalParamBinding, null, new object[] { Type.Missing }, CultureInfo.CurrentCulture);

            var expected = Create(type);

            var containerBuilder = new ContainerBuilder();
            containerBuilder.RegisterType(type);
            var container = containerBuilder.Build();

            var actual = container.Resolve(type);

            Assert.Equal(expected, actual);
        }

        class ClassWithRegisteredOptionalParam : OptionalClass<ClassWithInt32>
        {
            public ClassWithRegisteredOptionalParam(ClassWithInt32 t = null) : base(t) { }
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

        class ClassWithDispose : ILogger, IAsyncDisposable, IDisposable
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

    }
}
