using System;
using BTDB.IOC;
using BTDBTest.IOCDomain;
using Xunit;

namespace BTDBTest;

// Types without a generated factory are built by reflection. Before the fix a constructor parameter that could not be
// resolved got its DefaultValue, which is DBNull.Value without a default, and the constructor call failed on it.
public class IOCReflectionFallbackTest
{
    public class NeedsLogger
    {
        public NeedsLogger(ILogger logger) => Logger = logger;
        public ILogger Logger { get; }
    }

    public class MaybeLogger
    {
        public MaybeLogger(ILogger? logger, int? count) => (Logger, Count) = (logger, count);
        public ILogger? Logger { get; }
        public int? Count { get; }
    }

    public class DefaultedLogger
    {
        public DefaultedLogger(int count = 7) => Count = count;
        public int Count { get; }
    }

    static object Resolve(Type type)
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback(type).AsSelf();
        return builder.Build().Resolve(type)!;
    }

    [Fact]
    public void UnresolvableRequiredParameterIsReported()
    {
        var e = Assert.Throws<ArgumentException>(() => Resolve(typeof(NeedsLogger)));
        Assert.StartsWith("Cannot resolve BTDBTest.IOCDomain.ILogger logger parameter of", e.Message);
    }

    [Fact]
    public void UnresolvableNullableParameterIsNull()
    {
        var obj = Assert.IsType<MaybeLogger>(Resolve(typeof(MaybeLogger)));
        Assert.Null(obj.Logger);
        Assert.Null(obj.Count);
    }

    [Fact]
    public void UnresolvableParameterWithDefaultValueGetsIt()
    {
        Assert.Equal(7, Assert.IsType<DefaultedLogger>(Resolve(typeof(DefaultedLogger))).Count);
    }
}
