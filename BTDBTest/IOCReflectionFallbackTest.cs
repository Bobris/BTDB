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

    public class WithDependencies
    {
        [Dependency] public ILogger PrivateSetter { get; private set; } = null!;
        [Dependency] public ILogger Public { get; set; } = null!;
    }

    public class DerivedWithDependencies : WithDependencies
    {
    }

    // Before the fix a private setter declared in a base class was not found, so its dependency was not injected.
    [Fact]
    public void DependenciesWithPrivateSettersOfBaseClassAreInjected()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback(typeof(DerivedWithDependencies)).AsSelf();
        var logger = new Logger();
        builder.RegisterInstance<ILogger>(logger);
        var obj = (DerivedWithDependencies)builder.Build().Resolve(typeof(DerivedWithDependencies))!;
        Assert.Same(logger, obj.Public);
        Assert.Same(logger, obj.PrivateSetter);
    }

    public class HiddenDependenciesBase
    {
        [Dependency] ILogger PrivateProperty { get; set; } = null!;
        [Dependency] protected ILogger ProtectedField = null!;
        [Dependency] readonly ILogger _privateField = null!;

        public ILogger PrivatePropertyValue => PrivateProperty;
        public ILogger ProtectedFieldValue => ProtectedField;
        public ILogger PrivateFieldValue => _privateField;
    }

    public class HiddenDependencies : HiddenDependenciesBase
    {
        [Dependency] ILogger Own { get; set; } = null!;
        public ILogger OwnValue => Own;
    }

    // Before the fix only public [Dependency] properties were injected, generated factories inject all of them and
    // [Dependency] fields too.
    [Fact]
    public void NonPublicDependenciesAndFieldsAreInjected()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback(typeof(HiddenDependencies)).AsSelf();
        var logger = new Logger();
        builder.RegisterInstance<ILogger>(logger);
        var obj = (HiddenDependencies)builder.Build().Resolve(typeof(HiddenDependencies))!;
        Assert.Same(logger, obj.OwnValue);
        Assert.Same(logger, obj.PrivatePropertyValue);
        Assert.Same(logger, obj.ProtectedFieldValue);
        Assert.Same(logger, obj.PrivateFieldValue);
    }

    public class NamedParameters
    {
        public NamedParameters([Dependency("special")] ILogger a,
            [Microsoft.Extensions.DependencyInjection.FromKeyedServices("other")] ILogger b) => (A, B) = (a, b);

        public ILogger A { get; }
        public ILogger B { get; }
    }

    // Before the fix constructor parameters were resolved by their names, ignoring the names their attributes give.
    [Fact]
    public void ParametersAreResolvedByTheNamesTheirAttributesGive()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback(typeof(NamedParameters)).AsSelf();
        var special = new Logger();
        var other = new Logger();
        builder.RegisterInstance<ILogger>(special).Named<ILogger>("special");
        builder.RegisterInstance<ILogger>(other).Named<ILogger>("other");
        var obj = (NamedParameters)builder.Build().Resolve(typeof(NamedParameters))!;
        Assert.Same(special, obj.A);
        Assert.Same(other, obj.B);
    }

    public enum Mode
    {
        A = 1,
        B = 2
    }

    public class EnumDefaults
    {
        public EnumDefaults(Mode mode = Mode.B, Mode? maybe = Mode.A, Mode? none = null) =>
            (ModeValue, Maybe, None) = (mode, maybe, none);

        public Mode ModeValue { get; }
        public Mode? Maybe { get; }
        public Mode? None { get; }
    }

    // Before the fix the default of a nullable enum parameter, reported as its underlying number, was passed as it was
    // and the constructor call failed.
    [Fact]
    public void EnumDefaultsArePassed()
    {
        var obj = Assert.IsType<EnumDefaults>(Resolve(typeof(EnumDefaults)));
        Assert.Equal(Mode.B, obj.ModeValue);
        Assert.Equal(Mode.A, obj.Maybe);
        Assert.Null(obj.None);
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
