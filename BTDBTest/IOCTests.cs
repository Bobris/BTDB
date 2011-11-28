using System;
using BTDB.IOC;
using NUnit.Framework;

namespace BTDBTest
{
    public interface ILogger
    {
    }

    public class Logger : ILogger
    {
    }

    [TestFixture]
    public class IOCTests
    {

        public object Main(Type type)
        {
            var handle = type;
            if (handle.Equals(typeof(ILogger))) return new Logger();
            return null;
        }

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

    }
}