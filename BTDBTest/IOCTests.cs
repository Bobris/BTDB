using System;
using BTDB.IOC;
using NUnit.Framework;

namespace BTDBTest
{
    using IOCDomain;

    [TestFixture]
    public class IOCTests
    {
        [Test]
        public void AlwaysNew()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>();
            var container = builder.Build();
            var log1 = container.Resolve<ILogger>();
            Assert.NotNull(log1);
            var log2 = container.Resolve<ILogger>();
            Assert.NotNull(log2);
            Assert.AreNotSame(log1, log2);
        }

        [Test]
        public void Singleton()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>().SingleInstance();
            var container = builder.Build();
            var log1 = container.Resolve<ILogger>();
            Assert.NotNull(log1);
            var log2 = container.Resolve<ILogger>();
            Assert.NotNull(log2);
            Assert.AreSame(log1, log2);
        }

        [Test]
        public void CreatesFuncFactory()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>();
            var container = builder.Build();
            var logFactory = container.Resolve<Func<ILogger>>();
            var log1 = logFactory();
            Assert.NotNull(log1);
            var log2 = logFactory();
            Assert.NotNull(log2);
            Assert.AreNotSame(log1, log2);
        }

        [Test]
        public void CreatesLazyFactory()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>();
            var container = builder.Build();
            var lazyLog = container.Resolve<Lazy<ILogger>>();
            var log = lazyLog.Value;
            Assert.NotNull(log);
        }

        [Test]
        public void InjectionToConstructorWithOneParameterAlwaysNew()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>();
            builder.Register<ErrorHandler>().As<IErrorHandler>();
            var container = builder.Build();
            var obj = container.Resolve<IErrorHandler>();
            Assert.NotNull(obj);
            Assert.NotNull(obj.Logger);
            var obj2 = container.Resolve<IErrorHandler>();
            Assert.NotNull(obj2);
            Assert.NotNull(obj2.Logger);
            Assert.AreNotSame(obj, obj2);
            Assert.AreNotSame(obj.Logger, obj2.Logger);
        }

        [Test]
        public void InjectionToConstructorWithOneParameterSingleton()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>().SingleInstance();
            builder.Register<ErrorHandler>().As<IErrorHandler>();
            var container = builder.Build();
            var obj = container.Resolve<IErrorHandler>();
            Assert.NotNull(obj);
            Assert.NotNull(obj.Logger);
            var obj2 = container.Resolve<IErrorHandler>();
            Assert.NotNull(obj2);
            Assert.NotNull(obj2.Logger);
            Assert.AreNotSame(obj, obj2);
            Assert.AreSame(obj.Logger, obj2.Logger);
        }

        [Test]
        public void ReusingSingletonMultipleTimesInOneResolve()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>().SingleInstance();
            builder.Register<ErrorHandler>().As<IErrorHandler>();
            builder.Register<Database>().As<IDatabase>();
            var container = builder.Build();
            var obj = container.Resolve<IDatabase>();
            Assert.NotNull(obj);
            Assert.NotNull(obj.ErrorHandler);
            Assert.NotNull(obj.Logger);
            Assert.AreSame(obj.Logger, obj.ErrorHandler.Logger);
        }

        [Test]
        public void ReusingSingletonMultipleTimesInOneResolveOnceInSingleton()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>().SingleInstance();
            builder.Register<ErrorHandler>().As<IErrorHandler>().SingleInstance();
            builder.Register<Database>().As<IDatabase>();
            var container = builder.Build();
            var obj = container.Resolve<IDatabase>();
            Assert.NotNull(obj);
            Assert.NotNull(obj.ErrorHandler);
            Assert.NotNull(obj.Logger);
            Assert.AreSame(obj.Logger, obj.ErrorHandler.Logger);
            var obj2 = container.Resolve<IDatabase>();
            Assert.NotNull(obj2);
            Assert.AreNotSame(obj, obj2);
            Assert.AreSame(obj.ErrorHandler, obj2.ErrorHandler);
            Assert.AreSame(obj.Logger, obj2.Logger);
        }

        [Test]
        public void CreatesFastFuncFactory()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>().SingleInstance();
            var container = builder.Build();
            var obj = container.Resolve<ILogger>();
            var fastFactory = container.Resolve<Func<ILogger>>();
            var obj2 = fastFactory();
            Assert.AreSame(obj, obj2);
        }

        [Test]
        public void InjectionToConstructorWithOneParameterSingletonWithOptimization()
        {
            var builder = new ContainerBuilder();
            builder.Register<Logger>().As<ILogger>().SingleInstance();
            builder.Register<ErrorHandler>().As<IErrorHandler>();
            var container = builder.Build();
            var obj = container.Resolve<ILogger>();
            Assert.NotNull(obj);
            var obj2 = container.Resolve<IErrorHandler>();
            Assert.NotNull(obj2);
            Assert.NotNull(obj2.Logger);
            Assert.AreNotSame(obj, obj2);
            Assert.AreSame(obj, obj2.Logger);
        }

        [Test]
        public void CanRegisterInstance()
        {
            var builder = new ContainerBuilder();
            var instance = new Logger();
            builder.RegisterInstance(instance).As<ILogger>();
            var container = builder.Build();
            var obj = container.Resolve<ILogger>();
            Assert.AreSame(instance, obj);
        }

        public interface ICycle1
        {
            ICycle2 Cycle2 { get; }
        }

        public interface ICycle2
        {
            ICycle1 Cycle1 { get; }
        }

        public class Cycle1 : ICycle1
        {
            readonly Lazy<ICycle2> _cycle2;

            public Cycle1(Lazy<ICycle2> cycle2)
            {
                _cycle2 = cycle2;
            }

            public ICycle2 Cycle2
            {
                get { return _cycle2.Value; }
            }
        }

        public class Cycle2 : ICycle2
        {
            readonly Lazy<ICycle1> _cycle1;

            public Cycle2(Lazy<ICycle1> cycle1)
            {
                _cycle1 = cycle1;
            }

            public ICycle1 Cycle1
            {
                get { return _cycle1.Value; }
            }
        }

        [Test]
        public void CanBuildLazyCycle()
        {
            var builder = new ContainerBuilder();
            builder.Register<Cycle1>().As<ICycle1>().SingleInstance();
            builder.Register<Cycle2>().As<ICycle2>();
            var container = builder.Build();
            var obj1 = container.Resolve<ICycle1>();
            var obj2 = obj1.Cycle2;
            Assert.AreSame(obj1, obj2.Cycle1);
        }

        public class InjectingContainer
        {
            readonly IContainer _container;

            public InjectingContainer(IContainer container)
            {
                _container = container;
            }

            public IContainer Container
            {
                get { return _container; }
            }
        }

        [Test]
        public void CanInjectContainer()
        {
            var builder = new ContainerBuilder();
            builder.Register<InjectingContainer>().As<InjectingContainer>();
            var container = builder.Build();
            var obj = container.Resolve<InjectingContainer>();
            Assert.AreSame(container, obj.Container);
        }

        [Test]
        public void RegisterFactory()
        {
            var builder = new ContainerBuilder();
            builder.RegisterFactory(c => new InjectingContainer(c)).As<InjectingContainer>();
            var container = builder.Build();
            var obj = container.Resolve<InjectingContainer>();
            Assert.AreSame(container, obj.Container);
            Assert.AreNotSame(obj, container.Resolve<InjectingContainer>());
        }

        [Test]
        public void RegisterFactoryAsSingleton()
        {
            var builder = new ContainerBuilder();
            builder.RegisterFactory(c => new InjectingContainer(c)).As<InjectingContainer>().SingleInstance();
            var container = builder.Build();
            var obj = container.Resolve<InjectingContainer>();
            Assert.AreSame(obj, container.Resolve<InjectingContainer>());
        }

    }

}