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

    }

}